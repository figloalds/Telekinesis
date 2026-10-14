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
   point scope); WP2.5's major (the `feed_journal` backfill). Each fix has a
   test that fails without it. What remains open is in *Package status*.

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
   issues. Two are by-products of fixes above: none.

## Fixes of review findings (packages not approved)

| Package / finding | Fix | Test (fails without the fix) |
|---|---|---|
| WP2.1 **blocker**: four `legacy_stabilizers` tests fail (the full suite exits 101) | CCR-WP2.1-2 applied | the full suite (VERIFY) |
| WP2.1 minor: the signal self-pipe is not close-on-exec atomically | Linux/Android `pipe2(O_CLOEXEC)`; elsewhere `pipe` + `FD_CLOEXEC` as before | `shutdown::platform::tests::a_full_signal_pipe_never_blocks_the_handler` (WSL) checks the flags |
| WP2.1 minor: `watch_hang` acceptance | CCR-WP2.1-6 ratified | — |
| WP2.1 minor (WP1.11's file): the hub latency test's precondition fails under load | deviation 3 | — |
| WP2.3 **major**: `seal_forecast` parses up to 2 × 64 head payloads (peer events up to 8 MiB) under the engine lock before every slice | at most `FORECAST_ROWS` (8) head rows, and a payload is parsed only when `octet_length(payload) <= FORECAST_PAYLOAD` (64 KiB): `octet_length` reads no payload, and a larger event's branch counts as unknown (it caps nothing, so its seal may fall inside a normal slice, the documented cost of a branch not at the head). 64 × 1 MiB head rows: about 0.4 ms instead of about 90 ms (debug) | `w2_integration::the_seal_forecast_never_parses_large_payloads` (best of three < 25 ms; small rows are still read: a due branch at the head gives a one-event slice); `worker_concurrency` 22 unchanged and green |
| WP2.3 **major**: `fs_p99_during_catchup` floor | qualification run below | — |
| WP2.3 minor: maintenance and notifier threads end on a panic | each tick/round in `catch_unwind`: poison policy, log, a growing stop-aware delay (`Backoff::inbox`); fault point `worker.maintenance.tick.<repo>` | `worker_concurrency::a_panicking_maintenance_tick_is_contained` (checked failing with the containment removed) |
| WP2.3 minor: `revalidate` waits 1 s for a pass that already ran | the wait also ends (polled every 10 ms) once no row is left to activate | existing `worker_concurrency` revalidate tests, `w1_integration` |
| WP2.3 minor: `worker.activator.prepare` is process-global | scoped `worker.activator.prepare.<repo>` | `a_failed_slow_preparation_requeues_only_its_event` arms the scoped name |
| WP2.5 **major**: the `feed_journal` build is one statement over the whole journal, without progress or a resume point (§9, I10, invariant 28) | table and triggers first; then `BACKFILL_CHUNK` (10,000) journal rows per transaction from the `feed_journal_v1_cursor` key, `startup_progress("indexing the feed")` per chunk, the `feed_journal_v1` marker at the end (the pattern of `objects::ensure_schema`). A table WP2.5's build made whole (no marker, no cursor) counts as complete; a new or dropped table starts from 0 | `w2_integration::the_feed_journal_backfill_resumes_from_its_cursor` (an interrupted build resumes from its cursor, rows built before are not rebuilt, private events stay out; a dropped table is rebuilt) |
| WP2.5 minor: `feed_150k_events` not confirmed on a quiet machine | qualification run below | — |
| WP2.5 minor: notes misdescribe `feed-offer` | not a code issue; left in the package notes (open issue) | — |

## Tests added

`tests/w2_integration.rs` (new, 11 tests):

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

(filled in below)

## Qualification (`test-evidence/qualification-wave-2.json`)

(filled in below)

## Interoperability with 6298d0b (§13 rule 8.4)

(filled in below)

## Package status

(filled in below)

## Known limitations

(filled in below)
