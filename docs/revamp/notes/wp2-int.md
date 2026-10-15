# WP2.INT — Wave 2 integration

Branch `revamp/wp2-int`, cut from `revamp/wave-2-base` (`ca7d8a9`).
`revamp/production-sync` was not touched and `revamp/wave-3-base` was not
created: the coordinator does both after review (§13 rule 6, rule 8 item 7).

## Summary

1. **Merges.** All eight package branches, each as a `--no-ff` merge commit,
   in the order of the coordinator's brief (2.1 → 2.8; see deviation 1),
   each followed by its touched tests.

   | # | Branch (head) | Merge | Conflicts / breakages | Touched tests after the merge |
   |---|---|---|---|---|
   | 1 | `revamp/wp2-1-orchestrator-wiring` (`8fee4a1`) | `41798b0` + `24ed304` | none textually. **Known breakage:** WP2.1's blocker, four `legacy_stabilizers` tests of the replaced Supervisor; CCR-WP2.1-2 applied right after the merge (`24ed304`) | lib 60+2i, `orchestrator` 15, `orchestrator_runtime` 13, `legacy_stabilizers` 3+4i, `w1_integration` 4+1i, `contracts` 39, `seam_stubs` 8, `harness_selftest` 18+2i |
   | 2 | `revamp/wp2-2-management-api` (`40eb537`) | `68f4a19` | none | `management_api` 27, `orchestrator_runtime` 13, `orchestrator` 15, bin 4, `seam_stubs`, `contracts` |
   | 3 | `revamp/wp2-3-worker-activation` (`9a3abe9`) | `03a5a06` | none (first daemon runs with activator workers: all green) | lib 65+2i, `worker_concurrency` 22, `worker_lifecycle` 25, `w1_integration`, `activation_inbox` 21, `availability` 13, `runtime_lanes` 3, `partial_content` 8, `mount_lifecycle` 11 (WinFsp), `core` 16, `acknowledgements` 3, `paired_sync` 9, `orchestrator`, `orchestrator_runtime`, `legacy_stabilizers`, `harness_selftest`, `replication_regressions` 9, `bulk_lanes` 3 |
   | 4 | `revamp/wp2-4-offlock-slowpath` (`842a4f2`) | `a810831` | none (the activator now gets `Prepared::Computed`: all green) | lib 68+2i, `offlock_commit` 20+1i, `activation_inbox`, `startup_snapshots` 8, `oracle_smoke` 4, `w1_integration`, `replication_regressions`, `derive_oracle` 11+2i, `worker_concurrency`, `scalability` 5, `core`, `contracts`, `harness_selftest`, `paired_sync`, `availability` |
   | 5 | `revamp/wp2-5-feed-worker` (`d014de1`) | `eaa8e64` | none (the stand-in activation is inert with WP2.3's activator) | `feed_v7` 37, `worker_lifecycle` 25, `seam_stubs`, `contracts`, `acknowledgements`, `paired_sync`, `replication_regressions`, `startup_snapshots`, `oracle_smoke`, `worker_concurrency`, `offlock_commit`, `activation_inbox`, `w1_integration`, `harness_selftest` |
   | 6 | `revamp/wp2-6-peer-supervisor` (`3545f14`) | `15cebb7` | none | `peer_supervisor` 25, `seam_stubs`, `status_hub` 82, `contracts` |
   | 7 | `revamp/wp2-7-meta-scheduler` (`8f0abbb`) | `869b08e` + `de69aa3` | **test breakage:** `peer_supervisor::catalog_changes_respawn_or_stop_supervisors` asserted the `PeerRemoved` count right after the server saw the sessions close. With WP2.7's real `run_control` runner a lane joins it (one watcher slice, ~0.25–0.5 s) after the session closed, so the catalog watcher records `PeerRemoved` a moment later. Fixed in the test: it waits for the event, then still asserts exactly one (deviation 3) | `meta_scheduler` 24+1i, `peer_supervisor` 25 (5 runs), `seam_stubs`, `status_hub`, `contracts` |
   | 8 | `revamp/wp2-8-inbound-v3` (`c6aa398`) | `b8fa679` | none | lib 69+2i, `inbound_v3` 19, `contracts`, `seam_stubs`, `session_handshake` 21, `session_core` 59, `status_hub` 82, `peer_supervisor`, `meta_scheduler` |

   `cargo fmt --check` and `cargo clippy --all-targets -- -D warnings`
   passed on the merged tree before any integration change.

2. **Integration tasks (§14 WP2.INT).**
   - **Task 1, delete `supervisor.rs` and `catalog_db.rs`** (`b28ebaf`): both
     files and their `mod` lines, with what only they used (WP2.1 notes,
     *For WP2.INT*): the legacy `api::manage(&Shared, …)` and its
     re-export, `lanes::link_key`, `peering::remote_list(&Shared, …)`, and
     the four `supervisor.rs` unit tests. `startup::Watchdog::expired`
     became test-only. Two test files used the legacy Supervisor:
     - `tests/catalog_actor.rs` used it as the oracle of its downgrade and
       mount-path tests (what a 6298d0b daemon reads and accepts). Those
       tests now use `legacy_oracle`, a port of exactly the parts they
       exercised (the lock, schema and installation of `open`, `projects`,
       the List/Inspect/Peers replies of a daemon with no worker running,
       `ConfigureMount` with its `mount_path` rule), with the same SQL and
       JSON shapes. Before deleting the Supervisor, I checked the oracle
       against it on the legacy fixture: identical List, Peers and
       `projects()`. All 19 tests pass unchanged otherwise.
     - `tests/seam_stubs.rs::legacy_manage_answers_v2_actions_not_implemented`
       pinned the legacy `manage`'s `NOT_IMPLEMENTED` answer to the v2
       actions; it is removed with its subject (the v2 actions are served by
       `manage_v2` through `manage::manage`: `orchestrator_runtime`,
       `management_api`, `w2_integration`).
   - **Task 2, `tests/w2_integration.rs`** (new; Tests added): Retry
     recovers a Failed folder; Attach after fixing a mount path (same worker
     pid, real WinFsp); pause and resume on the legacy lanes (both sides);
     `manage status`/`--json` and `watch --json` against the real daemon;
     Subscribe latency < 1 s. Plus one test per behaviour CCR, one per
     review fix, and an end-to-end run of the v3 peer stack (below).
   - **Task 3**: qualification, interoperability and legacy fixtures (VERIFY,
     Qualification, Interoperability).

3. **CCRs** (table below): applied CCR-WP1.11-1/-2, CCR-WP2.2-1,
   CCR-WP2.1-2, CCR-WP2.3-1, CCR-WP2.4-1 (ratified), CCR-WP2.4-6,
   CCR-WP2.5-1, CCR-WP2.5-5, CCR-WP2.7-1/CCR-WP2.8-1, CCR-WP2.8-3; ratified
   in the plan the spec CCRs (CCR-WP1.INT-5, CCR-WP2.1-4, CCR-WP2.1-6,
   CCR-WP2.6-1/CCR-WP2.7-2/CCR-WP2.8-2, CCR-WP2.7-5); the rest deferred to a
   named owner or declined, each with its reason.

4. **Review findings of the packages not approved** (WP2.1, WP2.3, WP2.5),
   fixed here under §13 rule 5 (no package fix round remains): WP2.1's
   blocker (CCR-WP2.1-2) and its two minors (the signal pipe, the hub
   latency test's precondition); WP2.3's major (seal forecast under the
   engine lock) and three minors (panic containment, revalidate wait, fault
   point scope); WP2.5's major (the `feed_journal` backfill). Their tests
   are in the table below (the majors' and the panic containment's fail
   without their fix). What remains open is in *Package status*.

5. **The v3 peer stack end to end.** WP2.6's notes asked WP2.INT to recheck
   the lanes once WP2.7's real runners merged.
   `w2_integration::the_v3_stack_syncs_two_daemons_end_to_end` runs WP2.5
   (worker feed ops), WP2.6 (PeerManager, probe pairing, catalog client),
   WP2.7 (MetaScheduler, interim bulk lane) and WP2.8 (inbound server)
   together over real v3 sessions between two in-process daemons with real
   workers: pair, list, hold B's repository, metadata and objects both ways
   (dialer-driven), and status on both hubs. It passes in about 3 s. No
   breakage between the four packages was found; the stack is still not
   wired into the daemon (WP3.1).

## CCRs

### Assigned to WP2.INT by WP1.INT

| CCR | Status | Change |
|---|---|---|
| CCR-WP1.11-1 (`PeerConfigured.display_name`) | **applied** (`31c6e86`) | `#[serde(default)] display_name: Option<String>`; the hub labels the peer with it (`None` keeps a label set through `set_peer_label`); PeerManager and the legacy bridge fill it from `peer_meta.display_name` |
| CCR-WP1.11-2 (`LaneState.protocol`) | **applied** (`31c6e86`) | `#[serde(default)] protocol: Option<String>`; the hub keeps it per lane while Ready, and `PeerStatus.protocol` is the Ready control lane's, else any Ready lane's. PeerManager reports the session's `tkfs-peer/3`; a Ready legacy lane reports `tkfs-peer/2` (the v2 pairing protocol, `"version": 2`) |
| CCR-WP1.INT-5 (§7.1 rule 4, NotShared at startup) | **ratified** in the plan (`583ee91`) | |
| CCR-WP1.6-1 (seal cadence) | applied in part by WP2.4 | the rest is CCR-WP2.4-3 (below) |
| CCR-WP1.6-3, CCR-WP1.6-4 | applied by WP2.3 | — |
| CCR-WP1.6-5 | applied by WP2.4 | — |
| CCR-WP1.INT-1 | applied by WP2.1 | the general case is CCR-WP2.1-3 |

### Filed in Wave 2

| CCR | Status | Change or reason |
|---|---|---|
| CCR-WP2.1-1 (`Ctx` extension slot) | **deferred to WP3.1** | the adapter (`ctx::extras`, a side table keyed by the `Ctx` allocation) works; a new `Ctx` field changes every `Ctx` literal (six test files and four source files), best done when WP3.1 rewires `ctx.rs` |
| CCR-WP2.1-2 (`legacy_stabilizers`, `qual_wp1_12_legacy`) | **applied** (`24ed304`, `c2e0379`) | the reviewed `#[ignore]` patch (each names its `orchestrator_runtime` replacement); and the two qualification scenarios behind the floors `legacy_hung_repo_healthy_progress` and `legacy_crash_backoff` now use WP2.1's mechanisms (scoped fault points: a hung `published-exchange`, a worker that exits at startup) with the same metrics, so the floors keep their scenarios (deviation 4) |
| CCR-WP2.1-3 (`PROC_THREAD_ATTRIBUTE_HANDLE_LIST`) | **deferred to WP4.1** (security hardening) | Windows FFI in WP1.3's launcher; the adapter (all inherited handles made private at daemon start) covers the daemon's own inheritance |
| CCR-WP2.1-4 (§7.3 status member) | **ratified** in the plan | |
| CCR-WP2.1-5 (Stop waits 15 s) | **declined** | `ControlClient` now applies each call's deadline to its socket, but other clients still read with `ws::connect`'s 15 s timeout (the test harness's `Control`, scripts, a 6298d0b-era client): the 13 s wait stays inside it |
| CCR-WP2.1-6 (frozen-worker acceptance) | **ratified** in the plan | |
| CCR-WP2.1-7 (hub mount from a stale heartbeat) | **deferred to WP3.4** (GUI) | needs the mount generation the view was built from in `FolderView` (a model change); display-only, for at most one heartbeat |
| CCR-WP2.2-1 (`orchestrator::` re-exports) | **applied** (`b28ebaf`) | `pub use api::{ControlClient, default_deadline, format_folder, format_status, is_read_only}` |
| CCR-WP2.2-2 (Revalidate reports `requeued`) | **declined** (optional) | the adapter (worker RPC, then an empty `FolderCmd::Revalidate` only for a Blocked gate) is correct |
| CCR-WP2.2-3 (information for WP2.1) | checked | `manage::manage` forwards the 11 v2 actions; persistent connections; `set_daemon`/`set_stopping` are called; intent before command (`w2_integration`) |
| CCR-WP2.2-4 (stable CLI text) | used | `w2_integration::manage_status_and_watch_show_lifecycle_and_mount` |
| CCR-WP2.3-1 (`Engine::control("status")` `caught_up`) | **applied** (`761ea7f`) | `incoming_active == 0` instead of `incoming_pending == 0`; test `w2_integration::in_process_caught_up_ignores_quarantined_rows` (fails without it) |
| CCR-WP2.3-2 (seal off the commit lock) | **deferred to Wave 3/4** (with CCR-WP2.4-3) | needs `snapshot.rs` changes and their crash tests |
| CCR-WP2.3-3 (`SliceOutcome` retried rows) | **deferred** (optional; WP3.2 or WP4.2) | telemetry only |
| CCR-WP2.3-4 (`paired_sync` page build) | **declined** | WP3.1 removes the legacy network path |
| CCR-WP2.3-5 (`Store::seal_due_in`) | **deferred with CCR-WP2.4-2/-3** | the mirrored interval only weakens seal isolation if it diverges |
| CCR-WP2.4-1 (`activation_inbox` assertion) | **ratified** | merged as WP2.4 wrote it (`Prepared::Computed` with a computation) |
| CCR-WP2.4-2 (`snapshot.rs` prepared seal) | **deferred to Wave 3/4** | refactor; the mirror is pinned byte for byte by a unit test and the differential fuzz |
| CCR-WP2.4-3 (seal interval scaling) | **deferred to Wave 3/4** | needs `try_resume_snapshot`'s bound and new crash tests |
| CCR-WP2.4-4, CCR-WP2.4-5 | information | WP2.3's activator keeps a pooled read handle and checkpoints off-lock |
| CCR-WP2.4-6 (`Store::view` after a caller's rollback) | **applied** (`761ea7f`) | option (a): `Store::view` calls `drop_rolled_back_view` first; that function now leaves a view an outer reader still holds for its next call instead of panicking on the `RefCell`. Test `w2_integration::a_reader_never_sees_a_commit_its_caller_rolled_back` (fails without it) |
| CCR-WP2.5-1 (`worker_lifecycle` assertion) | **applied** (`761ea7f`) | a route-less `feed-offer` is `INVALID_REQUEST`; `worker_lifecycle` and `feed_v7` assert it; the `UNSUPPORTED_OPERATION` adapter is gone |
| CCR-WP2.5-2 (`need` both ways) | **deferred to WP3.1** | a frozen wire change of feed format 7, which travels only on the v3 path WP3.1 switches on; the adapter (large events inline in request pages) works |
| CCR-WP2.5-3 (skip null `event`/`large`) | **deferred to WP3.1** | changes the serialized form pinned by contract fixtures; WP2.7's re-serialization makes it matter only on the v3 wire |
| CCR-WP2.5-4 (`FeedPage.remaining`) | **deferred to WP3.1** | `to_receive` is an upper bound until then |
| CCR-WP2.5-5 (wiring of the feed tip and epoch) | **applied** (`db73bb5`) | `published-summary.tip` = `{feed_epoch, feed_tip}` (shared events only); the status cell's and heartbeat's `tip` likewise (the MetaScheduler's trigger, CCR-WP2.7-3); `JournalCounters.epoch` = the feed epoch. Test `w2_integration::the_published_and_heartbeat_tips_count_shared_events_only` |
| CCR-WP2.5-6 (`stage_incoming` cost) | **deferred to Wave 3** (performance) | |
| CCR-WP2.6-1 (catalog.list framing) | **ratified** in the plan (with CCR-WP2.8-2) | WP2.6's parser reads the body when there is one: compatible with WP2.8's framing, exercised end to end |
| CCR-WP2.6-2 (interruptible dial) | **deferred to WP3.1** | the adapter (retire and detach at the deadline) holds |
| CCR-WP2.7-1 / CCR-WP2.8-1 (`max_body(objects.exchange)` 16 MiB) | **applied** (`3c35345`) | one arm in `model::protocol::max_body`; the acceptor's trim now acts above 16 MiB; the two tests that pinned the 1 MiB bound moved to the new bound with the same assertions (deviation 3) |
| CCR-WP2.7-2 (wire shapes) | **ratified** in the plan | |
| CCR-WP2.7-3 (generations and tip) | applied through CCR-WP2.5-5 | the heartbeat tip is the feed tip |
| CCR-WP2.7-4 (lane exit contract) | checked | WP2.6's 1 s runner join and WP2.7's ~0.55 s exit agree; the `peer_supervisor` breakage above is this join |
| CCR-WP2.7-5 (`GateView` semantics) | **ratified** in the plan | |
| CCR-WP2.8-2 (framing) | **ratified** in the plan | |
| CCR-WP2.8-3 (`ExchangeStarted.direction`) | **applied** (`31c6e86`) | `#[serde(default)] direction: Direction` (default Outbound); the hub books an inbound start on the inbound lane without making the pair outbound. The inbound server still sends none (its adapter); WP3.1 can switch it on |
| CCR-WP2.8-4 (inbound lanes by role) | **deferred to WP3.1** (optional) | probe sessions stay unreported |

Every applied model change is additive with serde defaults; the contracts
fixture `status.status_event.json` was re-blessed (its diff is the three new
members only) and `contracts::status_events_without_the_wave_2_members_parse`
pins backward parsing.

## Deviations from the spec, and why

1. **Merge order.** §14 WP2.INT lists 2.4, 2.3, 2.5, 2.1, 2.2, 2.6, 2.7, 2.8;
   the coordinator's brief asked for 2.1 → 2.8. I followed the brief. The
   packages own disjoint files (no file is changed by two branches), so the
   order changed only when breakages showed up, not what had to be fixed.
2. **Edits outside the merged packages' files** (integrator, §13 rule 5 and
   rule 8 item 6), each listed with its reason above or below: `src/model/
   status.rs`, `src/model/protocol.rs` (CCRs); `src/orchestrator/{hub,
   peer, scheduler, legacy_bridge, inbound_objects, objects, api, lanes,
   peering, shutdown, mod}.rs`; `src/core.rs`, `src/core/{commit,
   sharing}.rs`; `src/runtime/{control, server, sync_rpc, notify}.rs`;
   `src/worker/{activator, maintenance, status_cell}.rs`;
   `src/core/status.rs`; `src/startup.rs`; `docs/revamp/REVAMP_PLAN.md`
   (ratifications only).
3. **Existing test files changed**, each a merge, CCR or task consequence;
   no assertion was weakened:
   - `tests/legacy_stabilizers.rs`: CCR-WP2.1-2's four `#[ignore]` lines.
   - `tests/qual_wp1_12_legacy.rs`: `hung_remote_repo_charged` and
     `crash_backoff` use scoped fault points instead of a `runtime.json`
     redirect and a deleted store (deviation 4); same metrics and checks.
   - `tests/peer_supervisor.rs`: `catalog_changes_respawn_or_stop_supervisors`
     waits for `PeerRemoved` before counting it (merge breakage above);
     `all_lanes_reach_ready_with_their_roles_and_lanes` also checks that the
     hub shows `tkfs-peer/3` (CCR-WP1.11-2); the `PeerConfigured` pattern
     gained `..` (new member).
   - `tests/catalog_actor.rs`, `tests/seam_stubs.rs`: task 1 (above).
   - `tests/contracts.rs`, `tests/status_hub.rs`, `tests/meta_scheduler.rs`,
     `tests/management_api.rs`: the new `StatusEvent` members in every
     literal (`display_name: None`, `protocol: None`,
     `direction: Direction::Outbound`).
   - `tests/inbound_v3.rs`, `tests/meta_scheduler.rs`: CCR-WP2.8-1/-2.7-1.
     The trim case is now 3 × 3 MiB parts (18 MiB of hex) trimmed to two
     under 16 MiB (it was 3 × 400 KiB trimmed to one under 1 MiB); the
     oversize request page is 16 MiB + 128 KiB of hex (it was 1.2 MB), and
     the test waits for that key's exchange to end before judging it (a
     16 MiB page takes a while to build in debug).
   - `tests/worker_lifecycle.rs`, `tests/feed_v7.rs`: CCR-WP2.5-1
     (`INVALID_REQUEST` instead of the Wave 0 stub's
     `UNSUPPORTED_OPERATION`).
   - `tests/worker_concurrency.rs`: the scoped prepare fault point's name;
     one new test (below).
   - `tests/status_hub.rs`: `record_p99_stays_under_50us_while_a_large_hub_rebuilds`
     skips a run whose samples did not overlap 50 rebuilds (up to 5 runs,
     at least one must) instead of failing on it; the 50 µs bound is
     unchanged. WP2.1's full suite failed once on that precondition (32
     rebuilds) under load.
   - `tests/fixtures/contracts/status.status_event.json`: re-blessed.
   - `tests/qual_wp1_6_activation.rs`, `tests/qual_wp2_4_offlock.rs`:
     deviation 6.
4. **The legacy floors keep their scenarios** (CCR-WP2.1-2 asked to re-point
   `legacy_hung_repo_healthy_progress` and `legacy_crash_backoff` to
   `qual_wp2_1_lifecycle`). Those scenarios emit other metrics, so
   re-pointing would have dropped the two measurements. Rewriting the two
   scenarios with WP2.1's mechanisms keeps the metric names, the floors and
   their intent (a hung repository on a peer never stops the healthy one;
   a crashing worker restarts with backoff). `tests/qualification-floors.json`
   is unchanged.
5. **Review findings of approved packages** (WP2.2, WP2.4, WP2.6, WP2.7,
   WP2.8) are not fixed here, as the brief says; they are listed as open
   issues (*Package status*).
6. **Two qualification scenarios changed** because WP2.3 and WP2.4 now run
   together (`d2f0fa8`; found by the first wave 2 qualification run):
   - `qual_wp1_6_activation::quarantine_not_pending` read the worker status
     right after delivering an invalid event over the legacy op. Since
     WP2.3 that op only stages while the background activator runs, and
     the activator quarantines the row moments later, so the check read 0.
     It now waits up to 10 s for the row to be classified; the check is
     unchanged.
   - `qual_wp2_4_offlock::stale_convergence_worker` skipped until WP2.3's
     activator existed. Running now, its writer (a local commit every 50 ms
     for the whole run) makes every prepared ticket Stale, which §4.4 never
     commits inline, so the slow events starved until the 2,400 s budget
     (`QUAL_TIMEOUT`). It uses `stale_convergence`'s bursty writer (WP2.4
     deviation 7) and converges in 65 s.
7. **The `lock_hold_slow_event_2k` floor stays on the store-level
   scenario.** WP2.4 suggested moving it to the `_worker` variant once
   WP2.3 merged. That variant (max 56 ms, p99 20 ms) probes the engine
   while a legacy `published-exchange` builds its page under the engine
   lock (CCR-WP2.3-4, `fs_p99_during_legacy_catchup`), so it measures the
   legacy op, not the off-lock slow path. Both are recorded.

## Fixes of review findings (packages not approved)

| Package / finding | Fix | Test |
|---|---|---|
| WP2.1 **blocker**: four `legacy_stabilizers` tests fail (the full suite exits 101) | CCR-WP2.1-2 applied | the full suite (VERIFY) |
| WP2.1 minor: the signal self-pipe is not close-on-exec atomically | Linux/Android `pipe2(O_CLOEXEC)`; elsewhere `pipe` + `FD_CLOEXEC` as before | `shutdown::platform::tests::a_full_signal_pipe_never_blocks_the_handler` (WSL) checks the flags |
| WP2.1 minor: `watch_hang` acceptance | CCR-WP2.1-6 ratified | — |
| WP2.1 minor (WP1.11's file): the hub latency test's precondition fails under load | deviation 3 | — |
| WP2.3 **major**: `seal_forecast` parses up to 2 × 64 head payloads (peer events up to 8 MiB) under the engine lock before every slice | at most `FORECAST_ROWS` (8) head rows, and a payload is parsed only when `octet_length(payload) <= FORECAST_PAYLOAD` (64 KiB): `octet_length` reads no payload, and a larger event's branch counts as unknown (it caps nothing, so its seal may fall inside a normal slice, the documented cost of a branch not at the head). 64 × 1 MiB head rows: about 0.4 ms instead of about 90 ms (debug) | `w2_integration::the_seal_forecast_never_parses_large_payloads` (best of three < 25 ms; small rows are still read: a due branch at the head gives a one-event slice); `worker_concurrency` 22 unchanged and green |
| WP2.3 **major**: `fs_p99_during_catchup` floor not met on the shared machine | nothing to fix in code: the floor is **met** in the release qualification, 17.4 ms < 50 (*Qualification*) | the qualification scenario |
| WP2.3 minor: maintenance and notifier threads end on a panic | each tick/round in `catch_unwind`: poison policy, log, a growing stop-aware delay (`Backoff::inbox`); fault point `worker.maintenance.tick.<repo>` | `worker_concurrency::a_panicking_maintenance_tick_is_contained` (checked failing with the containment removed) |
| WP2.3 minor: `revalidate` waits 1 s for a pass that already ran | the wait also ends (polled every 10 ms) once no row is left to activate | existing `worker_concurrency` revalidate tests, `w1_integration` |
| WP2.3 minor: `worker.activator.prepare` is process-global | scoped `worker.activator.prepare.<repo>` | `a_failed_slow_preparation_requeues_only_its_event` arms the scoped name |
| WP2.5 **major**: the `feed_journal` build is one statement over the whole journal, without progress or a resume point (§9, I10, invariant 28) | table and triggers first; then `BACKFILL_CHUNK` (10,000) journal rows per transaction from the `feed_journal_v1_cursor` key, `startup_progress("indexing the feed")` per chunk, the `feed_journal_v1` marker at the end (the pattern of `objects::ensure_schema`). A table WP2.5's build made whole (no marker, no cursor) counts as complete; a new or dropped table starts from 0 | `w2_integration::the_feed_journal_backfill_resumes_from_its_cursor` (an interrupted build resumes from its cursor, rows built before are not rebuilt, private events stay out; a dropped table is rebuilt) |
| WP2.5 minor: `feed_150k_events` not confirmed on a quiet machine | completed in the release qualification, at 951 events/s (158 s): within the 2,400 s scenario budget, not within the 120 s default (*Qualification*) | the qualification scenario |
| WP2.5 minor: notes misdescribe `feed-offer` | not a code issue; left in the package notes (open issue) | — |

## Tests added

`tests/w2_integration.rs` (new, 11 tests and 1 ignored interop test):

| Test | What it checks |
|---|---|
| `retry_recovers_a_failed_folder` | a worker whose store is gone exits Fatal (`STATE_NOT_INITIALIZED`): the folder is `failed` at once, `needs_attention`, and stays failed; with the store back, `retry` runs it again, its old file is there and a new import works |
| `attach_after_fixing_the_mount_path_keeps_the_worker` | a folder whose mount path is occupied runs headless with `attach_failed`; after the user moves the directory aside, `attach` mounts it with the same worker pid; the mount serves a file imported while headless; the user's directory is untouched (real WinFsp/FUSE; skipped without mount support) |
| `pause_and_resume_on_the_legacy_lanes` | two daemons, received folder: pausing the dialer stops delivery (3 s), resuming delivers; the same with the acceptor paused (its inbound answers `FOLDER_SYNC_PAUSED`); replies show `intents.sync_enabled: false` and `sync.state: paused` |
| `manage_status_and_watch_show_lifecycle_and_mount` | the real `tkfs manage status` text (`LABEL [xxxxxxxx]`, lifecycle, mount rows), `--json` (schema 2, phase running), and `watch --json` (a snapshot at once, then the paused folder within 10 s) |
| `subscribe_sees_a_change_within_1s` | a `subscribe` long poll shows a pause within 1 s of its reply (following generations through heartbeats) |
| `the_published_and_heartbeat_tips_count_shared_events_only` | CCR-WP2.5-5: a 32-hex epoch; a private-branch commit moves the journal tip but neither `published-summary.tip`, the status cell's `tip` nor the heartbeat's |
| `a_reader_never_sees_a_commit_its_caller_rolled_back` | CCR-WP2.4-6: after an outer `ROLLBACK` + `BEGIN IMMEDIATE`, and after a nested `ROLLBACK TO`, `entry`, `lookup` and `projection` read the committed state (fails without the fix: `entry()` returned the rolled-back revision) |
| `in_process_caught_up_ignores_quarantined_rows` | CCR-WP2.3-1: a quarantined row keeps `caught_up`, a ready row clears it (fails without the fix) |
| `the_feed_journal_backfill_resumes_from_its_cursor` | WP2.5 major (fails on WP2.5's build) |
| `the_seal_forecast_never_parses_large_payloads` | WP2.3 major |
| `the_v3_stack_syncs_two_daemons_end_to_end` | Summary 5 |
| `interop_wave_2_with_a_6298d0b_peer` (ignored; `TKFS_BASELINE_BIN`) | *Interoperability* |

Elsewhere: `contracts::status_events_without_the_wave_2_members_parse`;
`status_hub::{the_catalogs_display_name_labels_the_peer,
a_ready_lanes_protocol_is_the_peers_protocol,
an_inbound_exchange_start_is_booked_on_the_inbound_lane}`;
`worker_concurrency::a_panicking_maintenance_tick_is_contained`.

Removed with their subjects (task 1): `seam_stubs::legacy_manage_answers_v2_actions_not_implemented`
and the four unit tests of `supervisor.rs`
(`ready_line_with_a_failed_mount_keeps_the_worker`,
`ready_line_mismatches_are_still_rejected`,
`failed_workers_restart_after_a_growing_delay`,
`reconcile_leaves_workers_alone_once_shutdown_began`); the controller that
replaced the Supervisor has its own (`folder_controller`, `orchestrator_runtime`).

## VERIFY

Run on the tree of `7659915` (the product code and tests of every change
above; later commits add the ignored interop test, the two qualification
scenario fixes and these notes). Windows `CARGO_TARGET_DIR=C:/tkfs-target/wp2-int`,
WSL `CARGO_TARGET_DIR=$HOME/tkfs-target/wp2-int`. No other cargo, test or
tkfs process ran on the machine during the suites.

| Check | Result |
|---|---|
| `cargo fmt --check` | pass (exit 0) |
| `cargo build --all-targets` | pass, no warnings |
| `cargo clippy --all-targets -- -D warnings` | pass (exit 0) |
| `CARGO_TARGET_DIR=C:/tkfs-target/wp2-int powershell -NoProfile -File C:/tkfs-build/suite-lock.ps1 cargo test --all-targets --no-fail-fast` | **pass, SUITE_EXIT=0: 810 passed, 0 failed, 94 ignored** across 61 test binaries (`--no-fail-fast` so that a failure could not hide later binaries; with exit 0 it is the plain command's result) |
| Final tree (`3cd5520`, the merged evidence and these notes aside it is this commit's tree): `cargo fmt --check`, `cargo build --all-targets`, `cargo clippy --all-targets -- -D warnings`, then the plain `CARGO_TARGET_DIR=C:/tkfs-target/wp2-int powershell -NoProfile -File C:/tkfs-build/suite-lock.ps1 cargo test --all-targets` | **pass**: fmt, build and clippy exit 0; **SUITE_EXIT=0: 810 passed, 0 failed, 95 ignored** across 61 binaries (the 95th ignored test is the new `interop_wave_2_with_a_6298d0b_peer`) |
| WSL `cargo check --all-targets` | pass (exit 0), on `7659915` and again on the final tree |
| WSL `cargo clippy --all-targets -- -D warnings` (extra: `cfg(unix)` code, the `pipe2` change) | pass (exit 0) |
| WSL `cargo test --all-targets --no-fail-fast` (the whole suite; it contains §13 rule 8.2's subset: `orchestrator` 15, `session_core` 59, `session_handshake` 21, `worker_lifecycle` 25, `mount_lifecycle` 17 with `/dev/fuse`) | **pass, exit 0: 815 passed, 0 failed, 95 ignored** across 61 binaries; also `orchestrator_runtime` 13 (with `sigterm_runs_the_shutdown_sequence`), `w2_integration` 11 (the attach test over FUSE), lib 71 + 2 ignored |
| Legacy fixtures (§13 rule 8.3) | `harness_selftest` 18 + 2 ignored (legacy worker and data-dir fixtures, compat goldens), `catalog_actor` 19 (the legacy data directory opens and upgrades; the downgrade view through `legacy_oracle`), `w1_integration::revalidate_requeues_quarantined_rows_and_reactivates_them` (the 6298d0b worker fixture, now with WP2.3's activator): pass on Windows and WSL |
| Interoperability (§13 rule 8.4) | see *Interoperability*: 4 of 4 and 4 of 4 runs pass |

**Per binary (Windows / WSL):** lib 65+2i / 71+2i, bin 4 / 0,
acknowledgements 3, activation_inbox 21, additional 5, availability 13 /
10, availability_index 21, basic_metadata 4, bulk_lanes 3, catalog_actor
19, contracts 40, control_transactions 4, core 16, derive_oracle 11+2i,
directory_progress 3, feed_v7 37, folder_controller 50, harness_selftest
18+2i, inbound_v3 19, legacy_stabilizers 3+4i, management_api 27,
meta_scheduler 24+1i, mount_lifecycle 11 / 17, offlock_commit 20+1i,
oracle_smoke 4, orchestrator 15, orchestrator_runtime 13, paired_sync 9,
partial_content 8, peer_supervisor 25, posix_metadata 3, recovery 6,
replication_regressions 9, runtime_lanes 3, scalability 5, seam_stubs 7,
session_core 59, session_handshake 21, startup_snapshots 8, status_hub 85,
w1_integration 4+1i, w2_integration 11 (+1 ignored interop since
`359355b`), worker_concurrency 23, worker_lifecycle 25, worker_process 22,
worker_seams 4, and 15 qual binaries with 81 ignored scenarios on Windows
(82 in WSL).

**Count reconciliation.** WP1.INT's final tree: 599 passed, 84 ignored, 52
binaries. Wave 2 adds 9 binaries: `orchestrator_runtime` 13,
`management_api` 27, `worker_concurrency` 23 (22 + 1 here),
`offlock_commit` 20, `feed_v7` 37, `peer_supervisor` 25, `meta_scheduler`
24, `inbound_v3` 19, `w2_integration` 11 = 199; lib 52 → 65 (+13: WP2.1
+8, WP2.3 +5, WP2.4 +3, WP2.8 +1, minus the 4 `supervisor.rs` tests);
`contracts` +1, `status_hub` +3, `seam_stubs` −1, `legacy_stabilizers` −4
(ignored). 599 + 199 + 13 + 1 + 3 − 1 − 4 = **810**. Ignored: 84 +
`legacy_stabilizers` 4 + `offlock_commit` 1 + `meta_scheduler` 1 + qual
scenarios 4 (`qual_wp2_1_lifecycle` +1, `qual_wp2_3_worker` +1,
`qual_wp2_4_offlock` +2) = **94**.

**Stability.** `w2_integration` ran 3 times in a row on Windows (11/11 each,
~7.6 s) and once in WSL; `peer_supervisor` 5 + 3 times after the merge fix
and the protocol assertion; `meta_scheduler` 3 times after the `max_body`
change; `worker_concurrency`'s new test was checked failing without its fix.

## Qualification (`test-evidence/qualification-wave-2.json`)

**Runs.** `CARGO_TARGET_DIR=C:/tkfs-target/wp2-int powershell -NoProfile
-File scripts/qualify.ps1 -Wave 2 -BudgetS 2400 -FileTimeoutMin 150`
(release; every wave 1 and wave 2 qual file; 5,303 s) on the clean tree
`359355b` (the product code of `7659915`; its evidence was committed as
`d1f7a31`). Three files were then
re-run with `-Merge` on `d1f7a31` (clean; `dirty: false`) after the two
scenario fixes of `d2f0fa8` (*Deviations* 6): `qual_wp1_6_activation`,
`qual_wp1_9_session`, `qual_wp2_4_offlock`. The evidence is the merged
file (`merged_from` names the first run). Hardware: AMD Ryzen 9 7950X3D
(16 cores, 32 threads), 127.7 GB, Windows 11 Pro 10.0.26300. No other
cargo, test or tkfs process ran; the machine was not idle, though (another
user process held about one core throughout; see the A/B below).

**Result: 51 scenarios: 49 pass, 1 skipped (`sigterm_matches_shutdown`,
Unix-only: it passes in WSL, below), 1 fail; 27 floors evaluated, 26 pass,
1 fails: `session_loopback_throughput` (a wave 1 floor; environmental, see
below). `ok: false`, `enforced_failures: [session_loopback_throughput]`.**

| Floor (wave 2) | Floor | Measured | Notes |
|---|---|---|---|
| `api_p99_under_churn` | List p99 < 100 ms | 2.33 | A crash-looping folder and a StopBlocked one |
| `subscribe_latency` | ≤ 1,000 ms | 62.5 | measured against the real `subscribe` (WP2.1's branch reported `UNSUPPORTED` without WP2.2) |
| `shutdown_busy_folder` | ≤ 30,000 ms | 100.3 | `left_running`, then adopted |
| `fs_p99_during_catchup` | < 50 ms | **17.4** | WP2.3's open floor: met here, with WP2.4 merged (the review measured 55–62 ms on WP2.3's branch alone, under load). Not waived |
| `status_latency_during_activation` | < 20 ms | 3.34 | |
| `startup_with_5k_staged` | < 3,000 ms | 234 | drain 8.6 s, heartbeat `activated` 5,000 |
| `lock_hold_slow_event_2k` | < 50 ms | 14.8 (first run 12.3) | `commit_prepared` 15.9 ms; the same event inline 107.8 ms |
| `feed_150k_events` | completed | completed | **951 events/s** (158 s). WP2.5's notes read the floor against the default 120 s budget, which needs about 1,250 events/s: at this run's rate it would not finish in 120 s. The A/B below shows the machine about 1.8× slower than at WP1.INT's run |
| `idle_exchange_50k_cpu` / `_bytes` | ≤ 10 ms/side, ≤ 4 KiB | 1.56 ms, 1,593 B | |
| `large_event_8mib` | completed | completed | |

Wave 1 floors (the evidence's values; the three re-run files from the
re-run): `remote_linear_create_2k` 0.94 ms/event (first run 0.80),
`remote_linear_create_10k` 2.01 (1.77), `local_create_after_resolved_conflict`
p90 2.53 (1.69), `two_way_backlog_2k` 62.4 ms/event (≤ 100), `two_way_backlog_6400`
226.1 (≤ 300), `concurrent_namespace_ops_5k` max 408.7 ms (≤ 500),
`restart_ready_20k_objects` 1,179 ms, `content_status_50k` 0.09 ms,
`tiny_pending_objects_12k` completed, `watch_latency` 1,012 ms,
`session_slow_does_not_block_fast` 0.24 ms, `session_control_under_queued_data`
43.7 ms, `legacy_hung_repo_healthy_progress` 697 ms (the rewritten
scenario; ≤ 45,000), `legacy_interval_20s_reconnects` 0,
`legacy_crash_backoff` 5 (≤ 5), and **`session_loopback_throughput` 273
MiB/s (fails ≥ 300; first run 269)**.

**The failing floor is the machine, not Wave 2.** Wave 2 does not change
`src/session` (only `max_body`'s `objects.exchange` arm, which the
streamed download does not use). A/B on the same machine, minutes apart,
release, alternating: the wave 2 base (`ca7d8a9`, exported with `git
archive`, its own target directory) measured 263 and 242 MiB/s (upload
239/241, WSS 453/456); this tree 241 and 250 (upload 255/224, WSS
453/444). WP1.INT measured 578/567 on the same base code. The derive floors
show the same environment shift: `two_way_backlog_2k` measured 77.8
ms/event on the base and 70.0 on this tree back to back (WP1.INT: 42.6),
so the integration is not slower than its base, and the machine is about
1.8× slower than during WP1.INT's run. The floor is **not waived**: it
should be re-measured on a quiet machine before the wave 2 release point.

Scenarios without a floor: `qual_wp1_4_worker` 5/5, `qual_wp1_5_mount` 3/3,
`qual_wp1_6_activation::{quarantine_not_pending, transient_retry}`,
`qual_wp1_8_objects::corrupt_mid_page_keeps_completed`,
`qual_wp1_9_session::{dead_peer_detection, duplex_32mib, idle_60s}`,
`qual_wp1_12_legacy::worker_busy_not_counted` (0 counted),
`qual_wp2_1_lifecycle::{crash_loop_reaches_failed (17.4 s),
adoption_after_daemon_kill (resumed 1.9 ms after Running),
hung_worker_restarted (11.2 s), watch_hang_alone_is_not_unresponsive,
mount_failure_end_to_end}`, `qual_wp2_3_worker::fs_p99_during_legacy_catchup`
(382 ms, no floor: the legacy op's O(history) page build under the engine,
CCR-WP2.3-4), `qual_wp2_4_offlock::{stale_convergence (41.4 s, 85 stale
tickets), lock_hold_slow_event_2k_worker (max 56 ms, p99 20 ms: it probes
during a legacy `published-exchange`, whose page build holds the engine;
the floor stays on the store-level scenario, which isolates the slow
path), stale_convergence_worker (65.4 s)}`, `qual_wp2_5_feed::{peer_restore,
staged_events_not_resent}`. WSL, debug:
`qual_wp2_1_lifecycle::sigterm_matches_shutdown` passes.

## Interoperability with 6298d0b (§13 rule 8.4)

Waves 1–2 stay wire-compatible: the daemon's only network path is still
WP2.1's legacy bridge (v2 lanes, `published-*`, bulk protocol 1, the v2
pairing exchange), and the v3 stack (WP2.5–2.8) is not wired.

The baseline binary was built from `git archive 6298d0b` (no worktree
added; the export is identical to WP1.INT's) in
`C:/tkfs-build/wp2-int-baseline-src`, with
`CARGO_TARGET_DIR=C:/tkfs-target/wp2-int-baseline` (debug).

| Run | Result |
|---|---|
| `TKFS_BASELINE_BIN=… cargo test --test w1_integration -- --include-ignored` (WP1.INT's `interop_with_a_6298d0b_peer`: this build dials the old daemon and receives its folder, files both ways; the old daemon dials this build and receives, files both ways; both list each other as peers) | **pass, 4 of 4 runs** (5 of 5 tests each, 12–13 s) |
| `TKFS_BASELINE_BIN=… cargo test --test w2_integration -- --ignored interop` (new, `interop_wave_2_with_a_6298d0b_peer`: what Wave 2 changed on the paths an old peer meets) | **pass, 4 of 4 runs** (12.3–12.5 s): the old dialer receives a folder this build serves; this build pauses it, so its inbound answers `FOLDER_SYNC_PAUSED` (a code 6298d0b does not know): nothing moves for 3 s, the old daemon's API answers meanwhile, and after resume the file arrives; the old side's writes come back. Then this build dials the old daemon, receives its folder, pauses it as the dialer (nothing moves), resumes (the file arrives). This build's CLI (`ControlClient`) lists the old daemon's folders; `manage status` is refused by it (non-zero exit) |

The old worker answers `hello` with an error in the WP1.INT test, which
proves the peer really runs the old build. Interop was run on Windows with
headless folders and small files, debug builds, one old and one new daemon.

## Package status

| Package | Review | Merged | Status in the merged tree | Open issues (owner) |
|---|---|---|---|---|
| WP2.1 orchestrator wiring | **not approved** (its review) | `41798b0` (+ `24ed304`, `c2e0379`, `669d80f`, `7054d7c`) | green: `orchestrator` 15, `orchestrator_runtime` 13, `legacy_stabilizers` 3 + 4 ignored, `w2_integration`'s daemon tests; the qualification below | **its blocker resolved** (CCR-WP2.1-2: the patch it prepared, plus the two qualification scenarios); its three minors fixed or ratified (signal pipe, hub test precondition, CCR-WP2.1-6). Remaining, from its notes: a blocked receive on a cancelled legacy lane is not woken on Windows (it ends at the 15 s socket timeout; v3 sessions are not affected); a dead catalog thread is not restarted (management writes answer `CATALOG_UNAVAILABLE`); the `folder-slots` safety net is a 2 s timer, not a `registry.rs` fix (WP1.2's file); an accept loop whose wake-up connection fails is left to the process exit. CCR-WP2.1-1 (WP3.1), -3 (WP4.1), -7 (WP3.4) deferred; -5 declined |
| WP2.2 management API, CLI | approved | `68f4a19` | green: `management_api` 27; the CLI against the real daemon (`w2_integration`) | review minors: `default_deadline(Revalidate)` (60 s) is below the server's worst case (~61 s); a mutating action's `REQUEST_TIMEOUT` lacks the "may or may not have run" wording; `watch` retries forever on `INVALID_REPLY`, and a retried `subscribe` re-sends its full `wait_ms`; opposite concurrent pause/resume can leave the controller's intent different from the catalog's; `diagnostics` is not refused while stopping. CCR-WP2.2-2 declined |
| WP2.3 worker activation | **not approved** (its review) | `03a5a06` (+ `2f64972`) | green: `worker_concurrency` 23, `worker_lifecycle` 25, `availability` 13, `w1_integration`, `w2_integration`; the qualification below | **its major (seal forecast) fixed**, three minors fixed. `fs_p99_during_catchup`: see *Qualification*. Remaining: CCR-WP2.3-2/-3/-5 deferred (seal off the commit lock, retried-row telemetry, seal cadence API), CCR-WP2.3-4 declined; the forecast's documented limitations (a seal of a branch not at the head, or of a large event, can fall inside a normal slice) |
| WP2.4 off-lock slow path | approved | `a810831` | green: `offlock_commit` 20 + 1 ignored, `activation_inbox` 21, `derive_oracle` 11 + 2 ignored, `startup_snapshots` 8, `scalability` 5; the activator now commits `Prepared::Computed` (worker variants of its qualification run, below) | review minor: no test reaches `commit_computed`'s `Outcome::Fast` branch (a branch moved between ticket and preparation). CCR-WP2.4-1 ratified, -6 applied; -2/-3 deferred. Known: preparing is O(history); periodic seals of fast commits still hash and serialize under the lock |
| WP2.5 feed (worker side) | **not approved** (its review) | `eaa8e64` (+ `894f8ea`) | green: `feed_v7` 37, the v3 end-to-end test; the qualification below | **its major (the `feed_journal` backfill) fixed**; `feed_150k_events`: see *Qualification*. Remaining: its notes say `feed-offer` refreshes `sync_obs`/the generation and pokes the activator, which it does not (an offer changes nothing; documentation only). CCR-WP2.5-2/-3/-4 deferred to WP3.1, -6 to Wave 3 |
| WP2.6 peer supervisor | approved | `15cebb7` (+ `de69aa3`) | green: `peer_supervisor` 25 (with real WP2.7 runners now), the v3 end-to-end test | review minors: `LaneExit::drop` removes a panicked lane's `Conn` without closing it (its runner keeps the session until the supervisor stops); `spawn` records `PeerConfigured` and starts lanes outside the retire mute (a race with `shutdown`: a late event, unjoined threads that exit at once); retiring reports lanes `Closed` without an error, so after a pool shrink stale `Closed` lanes and their lane errors stay in the hub until `PeerRemoved`; `peer_meta.incompatible` is not cleared when a block changes to a non-Incompatible one; `pair` returns `Err` if a `RemovePeer` fails after the upsert stored the pairing. CCR-WP2.6-2 deferred (WP3.1) |
| WP2.7 MetaScheduler | approved | `869b08e` | green: `meta_scheduler` 24 + 1 ignored, the v3 end-to-end test | review minors: a GOAWAY refusal ends the lane while the peer would still answer up to 3 in-flight exchanges (they end `CONNECTION_LOST`: uncertain, never replayed, recovered by the next cursor exchange); `Wake::Done` is not sent if a step after the inner `catch_unwind` panics (a status sink panic), which would freeze that key until the lane restarts |
| WP2.8 inbound server | approved | `b8fa679` | green: `inbound_v3` 19, the v3 end-to-end test | review minors: `accept_error_is_per_connection` treats Windows `WSAENETDOWN`-class errors as per-connection, so a failed network subsystem makes `net-accept` spin until it recovers; `meta_exchange` keeps the request body alive during the worker call (up to ~2 GiB across 128 hung exchanges). CCR-WP2.8-4 deferred (WP3.1). Not wired until WP3.1 |

## Known limitations

- **The v3 stack is still standalone.** The end-to-end test runs it in
  process with the daemons' legacy bridges shut down; WP3.1 wires it (and
  deletes the bridge, `lanes.rs`, `peering.rs`).
- WP2.1's, WP2.3's and WP2.5's fixes here were checked only by their own
  tests and this integration's runs, not by a package re-review.
- The seal forecast reads at most 8 head rows and no payload above 64 KiB:
  for a backlog of large events, or of several branches beyond the eighth
  head row, a seal can fall inside a normal slice (one slice's hold, never
  a run of short slices).
- The `feed_journal` backfill's progress line is emitted per 10,000 rows,
  as the objects backfill's is; no test runs a worker over a journal large
  enough to see it (the resume test drives the cursor directly).
- `Store::view` reads a one-row TEMP table once per call on a handle that
  has committed (CCR-WP2.4-6, option (a)).
- Deferred CCRs stay with their named owners (table above).
- Build artefacts outside the repository: the 6298d0b export
  `C:/tkfs-build/wp2-int-baseline-src` and its target
  `C:/tkfs-target/wp2-int-baseline`; they only served the interop runs and
  can be deleted.

## Commits

- merges: `41798b0` (WP2.1), `68f4a19` (WP2.2), `03a5a06` (WP2.3),
  `a810831` (WP2.4), `eaa8e64` (WP2.5), `15cebb7` (WP2.6), `869b08e`
  (WP2.7), `b8fa679` (WP2.8)
- `24ed304` CCR-WP2.1-2 (`legacy_stabilizers` ignores)
- `de69aa3` `peer_supervisor` waits for `PeerRemoved` (merge breakage)
- `b28ebaf` task 1 (Supervisor and catalog_db deleted), CCR-WP2.2-1
- `31c6e86` CCR-WP1.11-1, CCR-WP1.11-2, CCR-WP2.8-3
- `3c35345` CCR-WP2.7-1 / CCR-WP2.8-1
- `db73bb5` CCR-WP2.5-5
- `761ea7f`, `4e36642` task 2 (`w2_integration`), CCR-WP2.3-1,
  CCR-WP2.4-6, CCR-WP2.5-1
- `894f8ea` WP2.5 review major (backfill)
- `2f64972` WP2.3 review findings
- `669d80f` WP2.1 review minor (signal pipe)
- `7054d7c` `status_hub` latency precondition
- `c2e0379` CCR-WP2.1-2 (qualification scenarios)
- `583ee91` spec ratifications in REVAMP_PLAN
- `7659915` the v3 stack end to end
- `359355b` wave 2 interoperability test
- `d2f0fa8` two qualification scenarios (deviation 6)
- `d1f7a31` first qualification evidence and notes draft
- `3cd5520` merged qualification evidence and these notes; then notes-only commits (the final VERIFY)

## Post-integration re-review

An independent re-review of the integrated tree at `839eb22` covered six
areas: WP2.1, WP2.3 and WP2.5 (the packages whose review fixes the
integrator applied), and three cross-package areas (lifecycle isolation,
the feed protocol, off-lock determinism). All six came back *changes
required*, with 23 findings: 14 majors and 9 minors. Three issues were
reported twice, by a package area and by a cross-package area (the slow
commit under the engine lock, the inbound gate evidence, slow-path
starvation), so there are 20 distinct issues. All are fixed here except the
off-lock area's second major, whose storage change is deferred with its
measurement and a CCR (*Deviation* below). Each fix has a
regression test that fails on the code before it (the failure is quoted;
"before" means the file(s) of the fix reverted on the final tree, with a
stub for a new public name a test refers to, see *How "before" was run*).

Public signatures are unchanged (§13). Additions only:

- `catalog::APPLY_WAIT` (`CatalogApi::apply` is now bounded),
  `run::MAX_CONTROL_THREADS`;
- `Store::commit_prepared_unless_busy` (beside `commit_prepared`, §13
  rule 3), `Store::status_computations` (a test counter), the cache type
  `core::status::Cache` in `StoreCaches`;
- `sharing::PAGE_READ_BUDGET`, `meta_exchange::evidence_of`,
  `activator::STALE_ESCALATE`;
- crate-private: `Slots::force_acquire`, `legacy_bridge::LaneVoice`
  (`lanes::run_lane` takes the lane's voice), the controller's
  `Msg::Catalog`.

No `src/model/**` or `src/util/**` file changed. The spec changes are
ratified in REVAMP_PLAN as CCR-WP2.INT-1 to -4 (below).

### WP2.1 (`legacy_bridge.rs`, `lanes.rs`, `manage.rs`)

| # | Finding | Fix | Regression test: before → after |
|---|---|---|---|
| R1 | **Major.** The legacy lane manager cancelled retired lanes but never muted them. A lane parked in a local worker call (or, on Windows, a peer read) kept recording into the hub for up to 15 s after its retirement: after Unpair its late `Closed` recreated the removed peer (`offline`, for good); after a re-pair its late `Closed` overwrote the new lane's `Ready` (for good). | Each lane has a `LaneVoice` (a mutex-guarded mute flag): every status event and gate evidence of `run_lane` goes through it. When `reconcile` (or `retire_all`) retires lanes it cancels and mutes them, records each one `Closed` itself, and only then records `PeerRemoved` or spawns the successors (the order of WP2.6's PeerManager). A report in progress finishes before the mute returns. | `w2_integration::unpair_removes_the_peer_from_status_for_good` (two daemons; the peer must leave `status` and stay gone 5 s). Before: "timeout waiting for the peer gone from status". `w2_integration::a_re_pair_keeps_the_new_lane_ready_while_the_old_one_ends_late` (the old control lane's first local `published-offer` hangs 10 s, then a re-pair; lane 0 sampled every 250 ms for 18 s). Before: `ready`, then `closed` from 8.65 s to the end (37 samples). After: `ready` throughout. |
| R2 | Minor. The §7.3 projection ignored the mount intent: a folder the user detached (WP2.2's Detach) read `running:false, starting:true, startup_phase:"attaching filesystem"` while it ran. | `mount_requested` also needs the view's intent to be other than `Detach`; §7.3 records the rule (CCR-WP2.INT-3). | New case "running, user detached" in `orchestrator_runtime::projection_follows_the_section_7_3_rules`. Before: `[false, true, false, "attaching filesystem", null]`; after: `[true, false, false, null, null]`. |

### Lifecycle isolation (`folder.rs`, `catalog.rs`, `run.rs`)

| # | Finding | Fix | Regression test: before → after |
|---|---|---|---|
| L1 | **Major.** The FolderController called `CatalogApi::apply` on its own thread, and `apply` had no deadline: a slow catalog write (typically the `ClearGate` after a gated attach) froze that folder's lifecycle: a dead worker stayed `Running`, commands waited their whole deadline, Shutdown reported `left_running REQUEST_TIMEOUT` for a dead worker. | The controller's event-driven writes (`ClearGate`, `RetargetGate`) run on a helper thread and come back as `Msg::Catalog` (in-flight flags prevent duplicates; the result is read against the latest snapshot). `Catalog::apply` waits at most `APPLY_WAIT` (5 s): a command not yet started is withdrawn (`CATALOG_UNAVAILABLE`, never applied), a running one answers `REQUEST_TIMEOUT`, and while one command has run that long new ones are refused at once. The command-path writes (`SetDesired`, `OverrideGate`) stay synchronous (the intent must be durable before the stop, invariant 26) but are bounded, and `OverrideGate` is skipped when the management action already wrote it (the usual case; `SetDesired` already was). | `folder_controller::a_slow_catalog_write_never_freezes_the_folder_lifecycle` (a `FakeCatalog` that holds `ClearGate`; both workers die). Before: "timed out waiting for a: backoff while the write is held". After: `Backoff` within 2 s, a command answered within 1 s, Shutdown reports `not_running`. |
| L2 | **Major.** Management writes had no server-side deadline; parked write requests filled the 64 control connections and new List/Inspect/Status connections were refused. | `APPLY_WAIT` (L1) releases each write within 5 s; and the accept cap counts only connections outside a request (64), with at most `MAX_CONTROL_THREADS` (128) in all, so parked requests never take the place of a read. | `orchestrator_runtime::writes_behind_a_slow_catalog_never_lock_reads_out` (in-process daemon, `catalog.slow_write` hangs 12 s, 64 clients send `reset-pairing-code`). Before: the 65th connection was refused ("control connection refused: too many open connections") and the test process aborted. After: `list` and `status` answered (`list` well under 2 s), every write answered within 9 s with `CATALOG_UNAVAILABLE` or `REQUEST_TIMEOUT`. |
| L3 | **Major.** A folder with `desired_running=0` whose worker was still alive (a Stop left StopBlocked at a Shutdown, a daemon crash) was never adopted: `Stopped`, ForceStop a no-op, the worker unsupervised. | At controller start a live, verified previous worker is adopted whatever `desired` is; once it answers `hello` with `desired=0` the stop ladder runs (graceful, StopBlocked with retries, ForceStop). A version mismatch stops it without a respawn; an adoption that ends without a worker never spawns while not desired. | `folder_controller::a_live_worker_of_a_stopped_folder_is_adopted_and_stopped`. Before: "timed out waiting for a: stop blocked" (`Stopped`, empty log). After: adopted, `StopBlocked`, retried, ForceStop → `stop{force}` → `Stopped`, no kill, no launch, its slot released. |
| L4 | **Major.** A headless received folder never cleared its gate marker (deleted only after `Attached`): `initial_sync_pending` for good, and after a restart with the source unreachable the gate was `Awaiting` again (List: `starting`, "synchronizing remote metadata"). | With no mount requested (no path, or `mount_enabled` off) the marker is cleared once the gate is `Satisfied` or `Overridden` (the legacy rule; a mount configured later attaches at once). §3.3 and the invariant 26 row record it (CCR-WP2.INT-2). | `folder_controller::a_headless_received_folder_clears_its_gate_once_satisfied`. Before: "timed out waiting for a: gate cleared" (0 `clear_gate`). After: one `clear_gate`, no gate row, a later mount attaches. |
| L5 | **Major.** `begin_start` took a `maximum_running` slot before looking at `worker.json`: a folder whose previous worker was alive could lose the slot to a fresh spawn, show `WORKER_LIMIT`, and leave its worker unsupervised. | The previous worker is probed first; adopting a live one always takes a slot (`Slots::force_acquire`, beyond the maximum: it already runs; later starts wait until the count is back under it). | `folder_controller::adoption_of_a_live_worker_takes_a_slot_beyond_the_limit` (`maximum_running=1`, a running, b started with a live worker). Before: "timed out waiting for b: adopted" (b `Stopped`, `WORKER_LIMIT`, no `adopt`). After: b `Running{adopted}`, two slots, back to one after b stops. |
| L6 | Minor. A graceful Stop or Restart while a previous worker was being adopted (no `hello` yet) led, when the stop failed, to a `hello` probe and a kill without the `stop{force}` that persists dirty sessions. | A stop plan sent during adoption remembers it (`unverified_since`): a failed graceful stop is retried, and after the adoption patience the folder is `Failed{PREVIOUS_WORKER_UNRESPONSIVE}` (the adoption rule); only ForceStop kills, after `stop{force}`. | `folder_controller::a_failed_stop_of_a_worker_being_adopted_never_kills_it`. Before: the folder never reached `Failed` (the clock-advance loop timed out; the review's trace for the same case: a `hello` probe, then `kill:4242`, no `stop{force}`). After: `PREVIOUS_WORKER_UNRESPONSIVE`, stops retried, 0 kills; ForceStop → `stop{force}` → `Stopped`. |
| L7 | Minor. Each 10 s wait for `owner.lock` ended in a counted failure: a live orphan without `worker.json` opened the crash breaker (`CRASH_LOOP`) without any worker launched. | A lock wait is not a failure: past 10 s the folder shows `PREVIOUS_WORKER_STOP_TIMEOUT` (once) and keeps waiting in `Starting`; each poll re-reads `worker.json` and adopts a worker that published one (a record probed once without an adoption is not probed again). | `folder_controller::a_lock_wait_never_opens_the_crash_breaker_and_adopts_a_late_worker` (80 s of waits, then a `worker.json`). Before: failed at the "still waiting" assertion (the folder had left `Starting`). After: still `Starting` with the error, 0 restarts, 0 launches; then adopted, the error resolved. **Changed assertion:** `foreign_image_is_never_killed_and_a_held_owner_lock_blocks_the_spawn` expected `Backoff` after the 10 s wait; it now expects `Starting` with `PREVIOUS_WORKER_STOP_TIMEOUT` (this finding's behaviour); its other checks (no spawn while locked, a spawn once free, the foreign process never killed) are unchanged. |

### WP2.3 (`activator.rs`, `maintenance.rs`, `status_cell.rs`, `core/status.rs`) and off-lock determinism

| # | Finding | Fix | Regression test: before → after |
|---|---|---|---|
| W1 | **Major** (WP2.3 and off-lock #1). The slow ticket's `commit_prepared`, and the re-queues around it, waited for another connection's writer under the engine lock (the 5 s `poll_busy`; up to about 10 s with the re-queue). | `Store::commit_prepared_unless_busy`: a busy `BEGIN IMMEDIATE` hands the preparation back (nothing read or written). The activator's `commit` and `requeue` install `fail_busy` within `WRITER_PATIENCE` (2 s) and retry after `WRITER_RETRY` with the engine free, as a slice does; a busy attempt is neither `Stale` nor a failure, and a busy re-queue never fails the cycle. | `worker_concurrency::a_slow_commit_never_waits_for_another_writer_under_the_engine_lock` (a connection holds the writer 600 ms at a time, 20 ms apart; a probe takes the engine every 5 ms). Before: "the engine was held 606.7ms". After: under the 250 ms bound, the slow event committed. |
| W2 | **Major** (WP2.3; off-lock #3, minor). A `Stale` ticket was only re-queued: behind a local writer committing more often than a preparation takes, a slow event (and its descendants) never activated; `starved_since_ms` was cleared by any activation, so it was rarely reported. | **CCR-WP2.INT-1 (§4.4, ratified):** after `STALE_ESCALATE` (4) consecutive `Stale` outcomes the event is committed inline, alone (`Prepared::Inline`, through the same never-wait commit): one O(history) hold for that event instead of unbounded starvation. Starvation is tracked per event (first `Stale` time and count); `starved_since_ms` is the oldest still starving, cleared when that event commits, is quarantined or leaves the inbox (checked at most once a second on a pooled store), never by unrelated activations. | `worker_concurrency::a_starving_slow_event_is_committed_inline_and_reported_until_then` (a save through the engine every 3 ms, 200 events of history; side-branch events from C activate meanwhile). Before: "the slow event never committed behind 2510 local saves" in 60 s. After: committed after 4 `Stale` rounds (about 4 s), the report constant while it starved and through C's activations, cleared after. Release: `qual_wp2_4_offlock::stale_convergence_worker_steady_writer` (new; the 50 ms writer deviation 6 had replaced), *Qualification* below. |
| W3 | **Major.** An idle worker's maintenance recomputed `journal_counters` and `inventory_match` (uncached, O(history)) every refresh: about 26 % of a core at 50K events. | Both are cached per connection under its change key (`total_changes`, `PRAGMA data_version`, open transaction, active branch; `core::status::Cache`), as the content scan already was; and `inventory_match` is index-only once the feed index is built (`feed_journal`'s row count, each peer's `peer_known` range, per feed row a `(peer, event)` key lookup), never the wide `events` rows. | `worker_concurrency::status_totals_are_cached_until_the_database_changes` (a WAL checkpoint by another connection is no change; another connection's commit is). With the cache lookups disabled: "recomputed while idle", 12 computations instead of 2. |
| W4 | Minor. Since CCR-WP2.5-5 the cell's `tip` is the feed tip, but `inventory_tip` recorded the journal tip: once the newest event was private, every `status` re-ran the inventory match. | `inventory_tip` (and the new `inventory_epoch`) record the feed tip and epoch in both refreshes; `reply` compares like with like. | `worker_concurrency::a_private_journal_tip_does_not_recompute_the_inventory_match` (caught up, then a `branch` fork). Before: 20 of 20 calls recomputed it. After: 0. |
| W5 | Minor. Read-your-writes made the `status` after any client mutation run a full refresh (content, journal counters, inventory match) on the caller, and concurrent callers each repeated it. | That refresh is content and cheap members only (maintenance brings the totals); one at a time per worker store (a lock keyed by the store root; `WorkerCtx` is frozen), a waiting caller finds the content caught up. The first reply still computes everything once. | `worker_concurrency::status_after_a_mutation_refreshes_the_content_only`. Before: "the inventory match was recomputed by the read-your-writes refresh". After: the content moved, the inventory untouched. |

### WP2.5 and the feed protocol (`sharing.rs`, `inbound.rs`, `meta_exchange.rs`)

| # | Finding | Fix | Regression test: before → after |
|---|---|---|---|
| F1 | **Major.** Regression detection per channel missed an acceptor restored from a backup when the higher report came from the other channel (from when it used to dial): with only the dialer dialing, every page got `CursorMismatch` for good (I2 "never stuck"). | **CCR-WP2.INT-4 (§6.3.4, ratified):** a reply's report (`cursor`, or a `CursorMismatch.expected`) is compared with the request channel's report too, when that one was recorded before our request was built (`Offer.at_ms`; an unknown offer counts as before): lower, same epoch and still valid means the peer regressed, and it is forgotten as `observe_report` does. A request-borne report recorded after our offer may be newer than the reply (mutual dialing) and is not compared. | `feed_v7::a_restored_acceptor_is_found_through_the_other_channel`. Before: "A received 0 of D's 8 events", 20 × `CursorMismatch { seq: 0 }`. After: A has all 8, caught up, `peer_known` complete. `feed_v7::mutual_dialing_converges_without_false_regressions` stays green. |
| F2 | **Major** (feed protocol; WP2.5 minor). The inbound server forwarded the acceptor's raw `GateView`, whose `caught_up` is false whenever `blocked`: an acceptor-side gate never reached `Blocked`. | One helper for both directions, `meta_exchange::evidence_of` (`caught_up = gate.caught_up \|\| (gate.blocked && metadata caught_up)`); §6.3.6 says so. | `inbound_v3::a_caught_up_but_blocked_source_view_is_blocked_evidence`. Before: `caught_up: false, blocked: true` (the dialer's mapping gives `true, true`). After: equal to the dialer's. |
| F3 | **Major.** An event with a malformed parent id (empty or over 128 bytes) was staged `waiting`, and its parent went into every need list to every peer, which every acceptor refused whole (`PROTOCOL_VIOLATION`): no metadata moved again. | `precheck` rejects an event whose `dependencies` or change `parents` are not valid ids (`INVALID_EVENT` into `feed_rejected`); `MISSING_PARENTS_SQL` takes only parents of 1–128 bytes, and the need list drops any invalid id (a row staged by another path). | `feed_v7::a_malformed_parent_never_reaches_a_need_list`. Before: the event was staged (`staged: 1`). After: rejected; a legacy-staged row with such a parent waits but never reaches a request; A's and M's exchanges keep working. |
| F4 | Minor. Need serving read every requested payload, served or not: draining N large needs read about N²/2 payloads. | The stored length first (`octet_length`, no payload read); a payload is read only when it fits the budget. | `feed_v7::need_serving_reads_only_the_payloads_it_serves` (24 × 774 KB). Before: 24 payloads read for 16 served. After: reads equal served, two rounds. |
| F5 | Minor. A page read and canonicalized every listed payload (elided and `large` included) with no budget: up to 4,096 × 8 MiB, past the exchange deadline, and the next exchange rebuilt the same page. | `PAGE_READ_BUDGET` (8 MiB) of stored payloads per page: the page selects `octet_length` only, reads a listed payload by id, and ends before the budget is passed (always listing its first entry; `through` the last listed, `more` set). §6.3.3 says so. | `feed_v7::a_page_reads_its_entries_within_a_budget` (40 × 774 KB). Before: one page of 40 entries. After: pages of at most 11, contiguous, all 40 listed, reads equal entries. |

### Deviation: the lock-hold floor and conflict-producing slow events (off-lock #2)

The `lock_hold_slow_event_2k` floor (< 50 ms) is met by its scenario,
whose slow event (a concurrent directory creation) leaves the projection's
conflict list unchanged and so stays on the CCR-WP1.INT-4 delta path. A slow
event that adds or changes a conflict (a concurrent edit of the same file,
the canonical two-way event) takes the full `current_state` rewrite under
the lock (2.8 MB at 2K, plus the forced seal), and a page staged by another
connection makes the first slice reload the projection under the lock.

**Not fixed here; measured and recorded.** Keeping a conflict change on the
delta path needs the conflicts and the pending list stored outside
`current_state`. A `current_parts` row cannot carry them: 6298d0b's
`load_projection` reads every `current_parts` row as an `Entry` (a
downgrade would fail to load, or load a wrong projection), and a new side
table would leave a downgraded binary reading a stale conflict list from
`current_state`. That is a storage-format decision with downgrade
consequences, outside an integration fix. **CCR-WP2.INT-5 (deferred to
Wave 3/4, with CCR-WP2.4-2/-3):** store the projection's conflicts and
pending list so that a conflict change is a delta, with a downgrade story;
move the forced seal's O(projection) write and hash out of the critical
section. The new release scenario
`qual_wp2_4_offlock::lock_hold_slow_conflict_event_2k` (A edits the file B
edited; the page staged on a secondary connection) measures it; it has no
floor, and the existing floor keeps its scenario (*Qualification* below).

### CCRs filed by the post-integration re-review

| CCR | Status | Change |
|---|---|---|
| CCR-WP2.INT-1 (§4.4 starvation bound) | **ratified** in REVAMP_PLAN §4.4 | after 4 consecutive `Stale` outcomes an event is committed inline, alone; `starved_since_ms` per event |
| CCR-WP2.INT-2 (§3.3 adoption, lock wait, headless gate) | **ratified** in §3.3 and the invariant 26 row | adoption at every controller start whatever `desired` (then the stop ladder for `desired=0`), adoption before slot accounting with a forced slot, a lock wait never counted, the stop of an unverified adopted worker follows the adoption rule, the marker of a folder with no mount requested cleared once satisfied or overridden, the controller's event-driven catalog writes off its thread |
| CCR-WP2.INT-3 (§7.3 `running`) | **ratified** in §7.3 | a mount with intent `Detach` is not requested |
| CCR-WP2.INT-4 (§6.3.3–6.3.6) | **ratified** in §6.3 | a reply's report is compared across channels; page read budget; need payloads read only when served; malformed references rejected at staging, never in a need list; the gate evidence mapping on both sides |
| CCR-WP2.INT-5 (conflict changes as a projection delta) | **deferred to Wave 3/4** (with CCR-WP2.4-2/-3) | above |

`REVAMP_PLAN.md` also records the catalog's bounded `apply` (§3.2) and the
control connection cap (§3.5); neither changes a contract.

### How "before" was run

Each fix's files were reverted with `git stash` on the final tree and the
new tests run against them; afterwards the stash was restored. Tests that
name a new public item got a stub for it in the reverted file, removed
again afterwards: `sharing::PAGE_READ_BUDGET` (F4/F5 run), and
`activator::STALE_ESCALATE` (W1/W2 run). W3's test needs the new counter, so
its "before" is the final code with the two cache lookups disabled
(`&& false`). F2's "before" is `inbound.rs` reverted with the new helper kept
(the test compares with it). F3's second layer (the need-list filter) was
checked separately: with only the `precheck` fix (the SQL filter and the
`retain` reverted), the legacy-staged row's 200-byte parent was in D's
request and the test failed at the need assertion.

### Tests added (post-integration re-review)

19 new tests (and one new case in an existing test), each failing before its fix (tables above):
`folder_controller` 6 (`a_slow_catalog_write_never_freezes_the_folder_lifecycle`,
`a_live_worker_of_a_stopped_folder_is_adopted_and_stopped`,
`a_headless_received_folder_clears_its_gate_once_satisfied`,
`adoption_of_a_live_worker_takes_a_slot_beyond_the_limit`,
`a_failed_stop_of_a_worker_being_adopted_never_kills_it`,
`a_lock_wait_never_opens_the_crash_breaker_and_adopts_a_late_worker`);
`orchestrator_runtime` 1 (`writes_behind_a_slow_catalog_never_lock_reads_out`)
plus one case in `projection_follows_the_section_7_3_rules`;
`w2_integration` 2 (`unpair_removes_the_peer_from_status_for_good`,
`a_re_pair_keeps_the_new_lane_ready_while_the_old_one_ends_late`);
`worker_concurrency` 5 (`a_slow_commit_never_waits_for_another_writer_under_the_engine_lock`,
`a_starving_slow_event_is_committed_inline_and_reported_until_then`,
`status_totals_are_cached_until_the_database_changes`,
`a_private_journal_tip_does_not_recompute_the_inventory_match`,
`status_after_a_mutation_refreshes_the_content_only`);
`feed_v7` 4 (`a_restored_acceptor_is_found_through_the_other_channel`,
`a_malformed_parent_never_reaches_a_need_list`,
`need_serving_reads_only_the_payloads_it_serves`,
`a_page_reads_its_entries_within_a_budget`);
`inbound_v3` 1 (`a_caught_up_but_blocked_source_view_is_blocked_evidence`).
Release scenarios (ignored): `qual_wp2_4_offlock::{lock_hold_slow_conflict_event_2k,
stale_convergence_worker_steady_writer}`.

### Verification (post-integration re-review)

Run on the final code tree (`212fbc3`; the notes and evidence commits after
it change no code). Windows `CARGO_TARGET_DIR=C:/tkfs-target/wp2-int`, WSL
`$HOME/tkfs-target/wp2-int`. No other cargo, test or tkfs process of mine
ran during the suites. The machine had little free commit memory during
this work (another user process held about 34 GB): two builds with the
default job count failed in the linker or in rustc with out-of-memory
errors (`LNK1102`, `STATUS_STACK_BUFFER_OVERRUN`), so the test binaries
were built with `-j 2` before the suite, which then built nothing.

| Check | Result |
|---|---|
| `cargo fmt --check` | pass (exit 0) |
| `cargo build --all-targets` | pass (exit 0), no warnings |
| `cargo clippy --all-targets -- -D warnings` | pass (exit 0) |
| `CARGO_TARGET_DIR=C:/tkfs-target/wp2-int powershell -NoProfile -File C:/tkfs-build/suite-lock.ps1 cargo test --all-targets` | **pass, SUITE_EXIT=0: 829 passed, 0 failed, 97 ignored** across 61 binaries. That is 810 + 19 new tests; ignored 95 + the 2 new release scenarios. Changed binaries: `folder_controller` 50 → 56, `orchestrator_runtime` 13 → 14, `w2_integration` 11 → 13 (+1 ignored), `worker_concurrency` 23 → 28, `feed_v7` 37 → 41, `inbound_v3` 19 → 20, `qual_wp2_4_offlock` 4 → 6 ignored |
| WSL `cargo check --all-targets` | pass (exit 0) |
| WSL `cargo clippy --all-targets -- -D warnings` | pass (exit 0) |
| WSL `cargo test --test orchestrator --test orchestrator_runtime --test folder_controller --test session_core --test session_handshake --test worker_lifecycle --test mount_lifecycle --test worker_concurrency --test feed_v7 --test inbound_v3 --test activation_inbox --test offlock_commit --test catalog_actor --test w2_integration` | **pass, exit 0**: 15, 14, 56, 59, 21, 25, 17 (`/dev/fuse`), 28, 41, 20, 21, 20 + 1 ignored, 19, 13 + 1 ignored |
| Before the full suite, the touched binaries on Windows | `folder_controller` 56, `orchestrator_runtime` 14, `orchestrator` 15, `catalog_actor` 19, `management_api` 27, `legacy_stabilizers` 3 + 4 ignored, `feed_v7` 41, `inbound_v3` 20, `meta_scheduler` 24 + 1, `peer_supervisor` 25, `activation_inbox` 21, `availability` 13, `core` 16, `derive_oracle` 11 + 2, `offlock_commit` 20 + 1, `paired_sync` 9, `partial_content` 8, `replication_regressions` 9, `scalability` 5, `startup_snapshots` 8, `w1_integration` 4 + 1, `worker_concurrency` 28, `worker_lifecycle` 25: all pass |

### Qualification (release, `test-evidence/qualification-wave-2.json`)

`scripts/qualify.ps1 -Wave 2 -NoBuild -Merge -BudgetS 2400 -FileTimeoutMin
150` over the files whose code paths changed: `qual_wp1_6_activation`,
`qual_wp1_12_legacy`, `qual_wp2_1_lifecycle`, `qual_wp2_3_worker`,
`qual_wp2_4_offlock`, `qual_wp2_5_feed`, on the clean tree `212fbc3`
(`dirty: false`; the release binaries were built first with `-j 3`, see
above). The other files keep the first run's results (`merged_from`:
`d1f7a31`). **All six files pass; 53 scenarios: 51 pass, 1 skipped
(`sigterm_matches_shutdown`, Unix only), 1 fail (`qual_wp1_9_session`, not
re-run: the environmental `session_loopback_throughput` of the first run);
27 floors, 26 pass.**

| Floor | Floor | Before (first run) | After |
|---|---|---|---|
| `api_p99_under_churn` | < 100 ms | 2.33 | 2.72 |
| `subscribe_latency` | ≤ 1,000 ms | 62.5 | 63.0 |
| `shutdown_busy_folder` | ≤ 30,000 ms | 100.3 | 232.2 |
| `fs_p99_during_catchup` | < 50 ms | 17.4 | 27.8 |
| `status_latency_during_activation` | < 20 ms | 3.34 | 2.95 |
| `startup_with_5k_staged` | < 3,000 ms | 234 | 262 |
| `lock_hold_slow_event_2k` | < 50 ms | 14.8 | 15.2 |
| `feed_150k_events` | completed | 951 events/s | 1,123 events/s |
| `idle_exchange_50k_cpu` / `_bytes` | ≤ 10 ms, ≤ 4 KiB | 1.56, 1,593 | 3.91 (note), 1,593 |
| `large_event_8mib` | completed | completed | completed |
| `remote_linear_create_2k` / `_10k` | | 0.94 / 2.01 ms/event | 0.87 / 1.65 |
| `local_create_after_resolved_conflict` p90 | | 2.53 | 1.88 |
| `legacy_hung_repo_healthy_progress` | ≤ 45,000 ms | 697 | 869 |
| `legacy_interval_20s_reconnects` / `legacy_crash_backoff` | 0 / ≤ 5 | 0 / 5 | 0 / 5 |

Note: an idle exchange gained one point query on the dialer
(`check_reply_report`) and none on the acceptor; the rest of the CPU
difference is the machine (see *Verification*), and the floor holds.

New scenarios (no floor):

- `lock_hold_slow_conflict_event_2k`: **max engine wait 61.2 ms**
  (`commit_prepared` 63.1 ms, preparation 75.0 ms off-lock, staging 2.3 ms on
  the secondary connection, one conflict row, oracle empty). Against 15.2 ms
  for the floor's directory event in the same run: the full rewrite of the
  deviation above.
- `stale_convergence_worker_steady_writer`: **20 slow events converge in
  126.9 s** behind 668 local imports at 50 ms: 80 `Stale` outcomes (4 per
  event, then the inline commit), `starved_since_ms` null at the end.

Scenarios without a floor otherwise: `lock_hold_slow_event_2k_worker` 66.0 ms
(56 before; it probes during a legacy `published-exchange`, deviation 7),
`stale_convergence` 33.9 s (68 stale), `stale_convergence_worker` 62.5 s,
`fs_p99_during_legacy_catchup` 178.8 ms, `adoption_after_daemon_kill`
resumed 2.0 ms after Running, `crash_loop_reaches_failed` 14.8 s,
`hung_worker_restarted` 11.2 s, `peer_restore` 70 ms,
`staged_events_not_resent` 0.

### Known limitations (post-integration re-review)

- **CCR-WP2.INT-5**: a conflict-producing slow event holds the engine about
  60 ms at 2K (the deviation above).
- The first activation slice after another connection's commit still
  reloads the projection under the engine lock before it hands out a slow
  ticket (off-lock #2's second point); measured inside
  `lock_hold_slow_conflict_event_2k`, not changed.
- W5: the read-your-writes refresh still loads the projection on a pooled
  connection whose view the commit invalidated (cold, O(projection)); only
  the totals and the repetition are gone.
- F5: the receiver's own reads of held payloads for elided entries are not
  budgeted (bounded by the sender's page budget for an honest sender).
- L5: adopting live workers can hold more slots than `maximum_running` for
  as long as those workers run (new starts wait meanwhile).
- W2: an escalated event is committed inline: one O(history) engine hold
  (about 100 ms at 2K, `inline_slow_commit_ms` 118.5 ms in this run) after
  about 4 s of starvation.
- The WP2.3 review's three lower-value observations (the `failures` map
  evicts the smallest id; `published-bulk-*` never bumps
  `status_generation`; a non-busy slice error counts as a pass) are not
  addressed.

### Commits (post-integration re-review)

- `dc56483` WP2.INT: re-review fix - orchestrator: lifecycle isolation,
  retired legacy lanes muted, Detach projection (R1, R2, L1–L7)
- `6d7d046` WP2.INT: re-review fix - feed: cross-channel regression, shared
  gate evidence, bounded needs and page reads (F1–F5)
- `db0be5e` WP2.INT: re-review fix - worker: never-wait slow commits,
  bounded starvation, cached status totals (W1–W5)
- `212fbc3` WP2.INT: re-review fix - spec ratifications (CCR-WP2.INT-1 to
  -4) and off-lock qualification scenarios
- the commit that adds this section and the merged qualification evidence
