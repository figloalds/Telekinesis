# WP4.INT — Wave 4 integration and production qualification

Branch `revamp/wp4-int`, cut from `revamp/wave-4-base` (`e8c2979`).
`revamp/production-sync` was not touched (no fast-forward); the coordinator
does that after review (§13 rule 6, rule 8 item 7).

## Summary

1. **Merges.** The five package branches, each as a `--no-ff` merge commit,
   in the order of §14.5a (4.2, 4.1, 4.4, 4.5, 4.3). No merge had a textual
   conflict.

   | # | Branch (head) | Merge |
   |---|---|---|
   | 1 | `revamp/wp4-2-operations` (`db76c59`) | `4a2f264` |
   | 2 | `revamp/wp4-1-security` (`bad2104`) | `783314f` |
   | 3 | `revamp/wp4-4-seal-path` (`7b3bf9a`) | `e362eee` |
   | 4 | `revamp/wp4-5-objects-cleanup` (`c9ba74e`) | `d8c392f` |
   | 5 | `revamp/wp4-3-docs` (`4b03e87`) | `ca8a887` |

2. **WP4.1's blocker** (the review did not approve WP4.1): a `peer.enroll`
   racing `ResetPairingCode` could leave an active per-peer credential after
   the rotation (invariant 21). The acceptor now stores an enrollment only
   while the verifier generation its session was authenticated at is still
   current, checked inside the catalog's IMMEDIATE transaction that stores it
   (`Catalog::enroll_at_generation`, `CredentialCmd::Enroll{generation}`);
   the catalog actor serializes it with `SetVerifier`, so an enrollment is
   either revoked by the rotation or refused `PAIRING_CODE_CHANGED`
   (`e52c3f7`). WP4.1's three minors are fixed too: the lanes of a peer being
   unpaired do not dial until the unpair ends (`59a224e`); `ws::accept` fails
   closed when the socket's local address cannot be read (`5992236`); the
   security grep test no longer exempts the Ready line copied to
   `worker.log` (CCR-WP4.1-4, `e905cdf`).

3. **CCRs applied** (table under *CCRs*): every Wave 4 CCR assigned to the
   integrator, the WP4.INT additions of §14.5a (CCR-WP3.1-3, CCR-WP3.4-1,
   WP4.5's CCRs against WP4.1's files), and the review minors of the
   approved packages.

4. **Idle worker CPU** (CCR-WP4.2-5, the floor WP4.2's `-All` run missed:
   18.85 % against ≤ 1 %). Profiled: every idle feed exchange moved
   `generations.sync`, which triggered a download round on each bulk lane,
   each an O(objects) want scan joining six tables; and every pooled
   connection rescanned the content each minute (a 60 s safety TTL, and a
   cold cache whenever the pool handed out another connection). Fixed by
   caching the want scan per connection while the database is unchanged,
   bumping no status generation for an exchange that moved nothing, spacing
   idle bulk rounds (`OBJECTS_IDLE_INTERVAL`, 5 s; triggers still run them at
   once), a 10 min content TTL and a connection of its own for the
   maintenance thread's status refresh (`19805ca`). Measured 20.4 % before,
   0.31–0.57 % after (4 runs).

5. **Defects found by the production qualification** and fixed:
   - The release feed fuzz (new, `tests/feed_fuzz.rs`) found that an entry a
     peer's feed listed while we lacked the event (elided, so not checkable)
     never became evidence once we held the event from another peer: its
     `feed_needs` row was deleted unread. The replica's known set of that
     peer stayed short for good and its feed kept sending that peer the
     payload. Now such rows become `peer_known` when the payload we hold
     matches their digest (`Store::evidence_from_needs`, `4f64579`). The
     derive-oracle stress run of WP4.4 had found a pre-existing refusal-parity
     gap (CCR-WP4.4-5): a concurrent content successor on a tombstoned
     entity could tip a cone over `MAX_NAMESPACE_CUTS` without counting its
     cuts; it now counts them and refuses as a full replay does (`b9debf9`).
   - A running folder was published with its repository id as `label` while
     a stopped one had its catalog label (CCR-WP4.3-2, `d73d0ae`).
   - A cleared `DATA/logs` stopped the installed log for the rest of the
     process; a replaced standard handle (the desktop daemon's inherited log
     file) was inheritable during the 2 s settle (WP4.2's minors, `92c93ce`,
     `7f0b33d`).

6. **Production qualification**: the full suite on Windows and the WSL
   subset, the 1,000-seed release oracles (`derive_oracle` and the feed
   fuzz) and the derive stress run, `qualify.ps1 -All` and `qualify.sh
   --all`, the two-hour soak, the upgrade from the 6298d0b fixtures and the
   6298d0b peer, `test-evidence/qualification-final.json` with the §15
   table, and the two-machine lab results (*Qualification* below).

7. **Docs reconciled** with the merged code (README, ARCHITECTURE,
   VALIDATION, CLI, REVAMP_PLAN; *Docs* below).

8. **WP4.5's review minors** (`c80b998`): more than 256 objects in their
   backoff for one source came back in every want (the exclusion list holds
   256): `ReleaseHeader` gains an additive `hold_ms`, with which the dialer
   releases them for their shortest remaining backoff and the worker hides
   them from that source meanwhile; the acceptor gives the last offer's
   unpushed leases up when its worker's want fails; a worker of an older
   build (no `objects.release`) is reported once; the saturated bulk
   session's own ping p99 is bounded (< 200 ms) next to the control lane's
   (< 100 ms); `SendBuffer::for_loopback` has a unit test. The push gate is
   `min(PEER_CALLS, REPOS_PER_LANE) − 1` (at least one) since `PEER_CALLS`
   is 4: `objects_v3_lanes::two_stalled_pushes_leave_a_third_repository_its_turn`
   caught two stalled pushes holding both repository turns (`d3eae4c`).

9. **Test races found while qualifying** (the property each test checks is
   unchanged): `qual_wp1_9_session::dead_peer_detection` let the server's
   own dead timer close the link at the moment the client's fired, so the
   client read the reset as `CONNECTION_LOST` (the server now declares the
   link dead three times later; `2e62858`);
   `objects_v3_lanes::an_object_of_unknown_size_pushed_in_part_is_handed_on`
   waited 10 s for four want calls, which the 5 s idle recheck can exceed
   (30 s; `860c6f7`).

10. **The soak's limit** (CCR-WP4.INT-3): the default two-hour soak passes,
    and so do conflict storms of 8 and 20 minutes (up to 1,300 unresolved
    conflicts); a two-hour storm (9,399 unresolved conflicts) and fast
    two-way editing (an operation every 50–400 ms per side) did not keep up,
    because a conflicting remote event's full replay grows with the history
    and the open conflicts.

**Results.** Full suite: 72 binaries, 1,062 passed, 0 failed, 123 ignored.
`qualify.ps1 -All`: 91 scenarios (90 passed, 1 skipped), **54 of 54
floors**. `qualify.sh --all`: 92 scenarios (91 passed, 1 skipped), every
enforced floor (52 of 54; the two misses are Windows-only floors). Release
oracles: `derive_oracle` 1,000 seeds and 40 stress seeds, the feed fuzz
1,000 seeds, 40 seal-path crash workloads: all pass. Soak: pass (default
workload). Upgrade from 6298d0b: pass. `qualification-final.json`: every
§15.1 row holds on Windows.

## CCRs

### Applied by WP4.INT

| CCR | What | Commit |
|---|---|---|
| WP4.1 blocker | `peer.enroll` checks the verifier generation in the storing transaction | `e52c3f7` |
| CCR-WP4.1-2 | `peer.enroll`/`peer.revoke` registered in `model::protocol` (ops, deadlines 10 s / 2 s, `op_deadline`), `PEER_HMAC_V1`; `session::auth` re-exports them | `fd921b8` |
| CCR-WP4.1-3 | the control server requires the daemon's token itself (`api::install_control_token` → `ControlToken`, `ws::accept_authorized` until the control server stops); the PeerManager adapter is removed | `5992236` |
| CCR-WP4.1-4 | `worker.log` owner-only (0600 on Unix); the grep test's Ready-line exemption removed (the launcher redacts the copy) | `e905cdf` |
| CCR-WP4.1-5 | `compat::worker_call` dials with the worker's instance token (`runtime::connect_worker`) | `fd921b8` |
| CCR-WP4.1-7 | ratified: WP4.1's change of `worker_lifecycle::ready_worker_record_and_hello_describe_the_same_process` (the Ready line carries a 64-hex token equal to `runtime.token` and `worker.json`'s; the record's keys are the golden's plus `token`) replaces the W0 placeholder assertions, as §14 WP4.1 asks | — |
| CCR-WP4.1-8 | `ReadyLine`, `WorkerRecord`, `WorkerEndpoint`: `Debug` prints `<redacted>` for the token | `fd921b8` |
| CCR-WP4.1-9 | §9: `peer_credentials(installation, side, …, PK(installation, side))` | `586296a` |
| CCR-WP4.1-10 | `peer_supervisor`: the interrupted-dial bound is 2.5 s (the slow proxy's 1.5 s chunk delay plus margin; an uninterrupted dial takes about 4.5 s) | `c80b998` |
| CCR-WP4.1-11 | `feed_v7::feed_ops_run_through_the_worker_rpc_seam` waits for the stand-in activation to converge before asserting the full set | `19805ca` |
| CCR-WP4.1-12 | already applied by WP4.5 (the test waits for the dialer's failed-push record) | — |
| CCR-WP4.1-13 | `activation_inbox::slice_budget_excludes_the_window_load`: a 4 s window-load hang with a 3 s slice budget (it was 1.5 s / 1 s) | `c80b998` |
| CCR-WP4.2-1 | every daemon installs `DATA/logs/orchestrator.log` once the catalog holds `orchestrator.lock`, and publishes its path (`log_path`) | `e905cdf` |
| CCR-WP4.2-2 | a slice reports the rows it moved to retry (`SliceOutcome::retried`); `activation.retried` counts them | `92c93ce` |
| CCR-WP4.2-5 | idle worker CPU (Summary 4) | `19805ca` |
| CCR-WP4.2-6 | `qual_wp3_1_transport::inbound_status` waits up to 5 s for the acceptor's status to list the inbound peer | `56f631f` |
| CCR-WP4.3-1 | the CLI names an unlisted peer once (`peer <id>`; it printed `peer peer <id>`) | `93be9fc` |
| CCR-WP4.3-2 | a running folder is published with its catalog label, and a rename while it runs reaches the entry; the worker's `published-summary` keeps its repository id there (the orchestrator ignores it) | `d73d0ae` |
| CCR-WP4.4-2 | ARCHITECTURE *Startup snapshots* describes format 2 | `586296a` |
| CCR-WP4.4-3 | the plan: invariant 5's row, the snapshot scope cut, §5.3's `projection_version` triggers (`current_meta`), §9's WP4.4 tables | `586296a` |
| CCR-WP4.4-4 | `qual_wp4_4_seal` registered (wave 4) in `qualify.ps1` and `qualify.sh`, with four floors | `56f631f` |
| CCR-WP4.4-5 | a concurrent content successor on a tombstoned entity counts its cuts (`check_content_successor`) | `b9debf9` |
| CCR-WP4.5-1 | (a)–(d): `inbound.rs`, `worker_client.rs` and `hub.rs` docs no longer describe the interim path; `max_body(objects.exchange)` is the 1 MiB default and `op_deadline` has no entry for it; (e): §6.4's lease handoff and §6.5 marked superseded | `bd509fa`, `586296a` |
| CCR-WP4.5-2 | 4 exchange slots per bulk session (`inbound::BULK_EXCHANGE_SLOTS`), `objects::PEER_CALLS` follows it | `d3eae4c` |
| CCR-WP3.1-3 | `Scope::Endpoint{endpoint}` (additive) for a pairing an endpoint refused (it was `Peer{installation: <endpoint>}`); hub, CLI and desktop show `endpoint <url>` | `93be9fc` |
| CCR-WP3.4-1 | the hub keeps the controller's mount over heartbeats older than the mount result | `9f22963` |

The approved packages' review minors: WP4.2 (`qualify.sh` recorded a file
with a failed scenario as passed, `56f631f`; `RotatingFile::reopen`,
`92c93ce`; the inherited-handle note, `7f0b33d`, and *Deviations* 4), WP4.3
(`docs_cli` value checks, `e1d06aa`/`9c6ad45`; the README state order, the
`revoked` row, `catalog.list` on probe sessions, `586296a`), WP4.4 (the
`journal_branch` setup in one savepoint, the staging cache restored on every
path, large-projection crash coverage, `59a224e`), WP4.5 (Summary 8).

### Not applied

| CCR | Why |
|---|---|
| CCR-WP2.8-4 (optional) | probe sessions stay unreported as inbound lanes (no functional effect; a hub keying change) |
| CCR-WP4.1-6 (optional) | the WP4.5 tests that dial a worker process without its token keep using `Discovery`'s token note (the adapter works) |
| CCR-WP4.4-1 | staging's `staged_objects` trigger parses each payload in SQL; moving it to Rust changes an existing trigger's definition (a schema migration) for a 17 vs 65 µs/event staging cost that meets every floor; left for a later package |
| CCR-WP4.5-3 (optional) | `SessionWorkerClient::{want_excluding, release}`: the adapters in `objects.rs` work |

### Filed by WP4.INT

- **CCR-WP4.INT-1** (`src/worker/status_cell.rs`, `src/runtime/engine.rs`):
  the worker `status` reply's top-level `caught_up` and `metadata_caught_up`
  are the legacy lanes' observations (`Engine::last_sync`, set only by
  `paired_sync`), so on the v3 path they stay `false` (the two-machine lab
  read them `false` throughout). Requested: derive them from the v3 feed
  observations (`feed_peers`, each with `caught_up` and `last_exchange_ms`)
  with a freshness rule that suits long sync intervals. Adapter: `docs/CLI.md`
  says so and points to `feed_peers` and `manage status`.
- **CCR-WP4.INT-2** (`src/mount.rs`, `src/fuse_linux.rs`): a read of content
  that is still downloading waits up to 15 s for the whole object and then
  fails (`STATUS_FILE_IS_OFFLINE`, which programs report as "invalid
  argument"; `EAGAIN` on Linux). The lab's mounted check hit it with a
  512 MiB file opened 3 s after the mount attached. Requested: serve reads
  from the durable prefix of the object's spool (ranges already received),
  or wait as long as the download progresses. Adapter: README *Initial-sync
  gate* documents it.

- **CCR-WP4.INT-3** (`src/core/commit.rs`, `src/core/derive.rs`,
  `src/worker/activator.rs`; performance, the release's main open limit):
  while a branch has unresolved conflicts, remote creations and namespace
  changes take the slow path, and a slow event whose append-mode
  precondition fails is a full replay (off-lock prepare, then commit), whose
  cost grows with the history and steeply with the unresolved conflicts. The
  soak measured it (*Qualification*, *Soak*): with every edit of both sides
  on one shared name pool, the stores were exact and converged at 3,408
  events / 236 conflicts (8 min) and 7,082 / 1,300 (20 min, oracle check
  about a minute), but after two hours (8,973 events, 9,399 conflicts) one
  event's prepare took minutes and went stale four times in 15 min, 1,549
  and 2,617 events were still waiting after the 30 min settle, and the
  oracle check of the stopped stores had not finished after 3 h. With 10 %
  of the edits shared but one every 50–400 ms per side, activation fell
  behind too (21.5K events and about 100 conflicts per side after 91 min,
  the backlog growing from 140 to 1.5K events), while continuous local
  writes made most prepared replays go stale (2,420 stale outcomes).
  Requested: keep the slow path's cost proportional to the touched cones
  when the append precondition fails for cones the event does not touch (or
  let remote namespace changes that touch no conflicted entity keep the fast
  path while unrelated conflicts are open), prepare one replay for several
  queued slow events, and do not discard a prepared replay when the branch
  moved only by commits of unrelated entities. A full rebuild (startup after
  a damaged snapshot, a downgrade, the oracle) has the same cost: a store
  with thousands of unresolved conflicts can take hours to open that way.
  Adapter: none; README (*Filesystem behaviour*) asks to keep unresolved
  conflicts few, VALIDATION (*Soak*) records the limit, and the soak's
  default workload edits one operation every 1–4 s per side with 10 %
  shared (deviation 2).

## Deviations from the spec, and why

1. **The feed fuzz is a file of its own** (`tests/feed_fuzz.rs`), not part
   of `tests/feed_v7.rs` (WP2.5's): §14 WP4.INT task 2 names "the `feed_v7`
   fuzz", which did not exist. It drives the same three store functions the
   workers call (`build_exchange_request`, `handle_exchange_request`,
   `apply_exchange_reply`). Re-applying a kept (stale) reply is off by default
   (`TKFS_FEED_STALE_REPLIES=1`): the session layer delivers each reply once,
   to its own request, and the orchestrator never replays `feed-apply`
   (invariant 16); the store-level binding check alone accepts a stale pair
   (`apply_exchange_reply` binds a reply to its request id, not to a
   one-time use), which a first draft of the fuzz flagged.
2. **The soak is a release test** (`tests/qual_wp4_int_soak.rs`, ignored),
   run on its own and not by `qualify.ps1 -All`: its 2-hour edit phase plus
   up to 30 min of settling exceeds `-All`'s per-file budget and would double
   the run. Its workload was changed twice after runs that did not converge
   (*Qualification*, *Soak*): the first edited only a shared name pool (a
   conflict storm), the second edited one operation every 50–400 ms per
   side; the default is now names of each side's own with 10 % shared, one
   operation every 1–4 s per side (`TKFS_SOAK_SHARED_PERCENT`,
   `TKFS_SOAK_PAUSE_MS_MIN` / `_MAX`; `TKFS_SOAK_MINUTES` and
   `TKFS_SOAK_SETTLE_MINUTES` shorten it). The plan's criteria ("equal feeds,
   empty oracle on both, no stalled or blocked state") are unchanged; what
   the faster workloads showed is CCR-WP4.INT-3, not hidden: it is in the
   README, VALIDATION and *Known limitations*.
3. **The upgrade checks** are the existing scenarios
   (`qual_wp3_int_e2e::legacy_fixture_upgrade_then_sync` from the 6298d0b
   data directory, `qual_wp3_1_transport::upgrade_against_a_6298d0b_peer_fails_closed_on_both_sides`
   against a real 6298d0b daemon, `catalog_actor`'s and `harness_selftest`'s
   fixture tests in the suite), run by `-All` with `TKFS_BASELINE_BIN` set to
   the lab's 6298d0b release build (`C:/tkfs-build/lab/target-baseline/release/tkfs.exe`,
   SHA-256 `69c6e8f8414f…`, the binary the lab ran as the baseline).
4. **WP4.2's notes say** a child spawned in the 2 s settle window "could
   inherit the old log file … the daemon spawns none then". That is
   inaccurate (the review's minor): Rust's `Command` inherits every
   inheritable handle, and workers start right after `ctx::build_with`. Not
   edited there (not this package's file); fixed in the code instead
   (`7f0b33d`: the replaced handle is made non-inheritable at once).
5. **CCR-WP4.3-2 changes what peers read**: a running folder's `label` in
   `catalog.list` / `remote-list` is now its catalog label, not its
   repository id (the previous release's value). Only this release reads
   it (a previous-release peer fails closed at the handshake); the desktop's
   *Browse folders* and `remote-list` show names instead of ids.
6. **Qualification ran with `-Priority Normal`** (as WP4.2's `-All` run did)
   on the shared machine; the evidence records the load at the start.

## Existing assertions changed

| Test | Change | Why |
|---|---|---|
| `peer_supervisor::a_lane_inside_a_dial_is_interrupted_and_reports_nothing_after_peer_removed` | the unpair bound 1.5 s → 2.5 s | CCR-WP4.1-10: the bound equalled the proxy's chunk delay; 2.5 s still tells an interrupted dial (about the remaining delay) from an uninterrupted one (about 4.5 s) |
| `activation_inbox::slice_budget_excludes_the_window_load` | window-load hang 1.5 s → 4 s, slice budget 1 s → 3 s, `elapsed ≥ 4 s` | CCR-WP4.1-13: the hang stays above the budget, so the test still tells a budget that excludes the load from one that includes it |
| `feed_v7::feed_ops_run_through_the_worker_rpc_seam` | waits for the stand-in activation to converge | CCR-WP4.1-11 (the stand-in budget is 100 ms under `try_lock`) |
| `qual_scale::idle_worker_cpu` | the measurement window starts 30 s after `caught_up` (was 5 s) | profiled: the first 20 s after `caught_up` are the end of the catch-up (activation, seals, scrub), not idle work |
| `contracts` `max_body(OBJECTS_EXCHANGE)` | 16 MiB → 1 MiB, and no deadline | CCR-WP4.5-1 (c) |
| `security` grep test | the Ready-line exemption removed | CCR-WP4.1-4 (stricter) |
| `logging::diagnostics_bundles_have_every_part_and_no_secret` | requires the installed log | CCR-WP4.2-1 (every daemon has one now) |
| `folder_controller` (2 tests) | the fake worker's summary probe moved from its `label` to its one branch; a running entry's label is the record's | CCR-WP4.3-2: the controller publishes the catalog label, so the label no longer names the summary the entry came from |
| `docs_cli` help-parser self-test | gains an optional-value option | the checker's new `Maybe` kind |
| `objects_v3_lanes::an_object_of_unknown_size_pushed_in_part_is_handed_on` | the wait for four want calls 10 s → 30 s | since CCR-WP4.2-5 an upload round whose wants reply is empty waits `OBJECTS_IDLE_INTERVAL` (5 s); the four calls still have to happen (Summary 9) |
| `qual_wp1_9_session::dead_peer_detection` | the server's `dead_after` is three times the client's (its assertions unchanged) | the server's own timer closed the link as the client's fired, so the client read `CONNECTION_LOST` (Summary 9) |

## Tests added

| Test | Covers |
|---|---|
| `security::an_enrollment_at_a_replaced_generation_stores_nothing` | an enrollment for a session authenticated at a replaced verifier generation is refused `PAIRING_CODE_CHANGED` and stores nothing |
| `security::an_enrollment_racing_a_rotation_never_outlives_it` | `peer.enroll` racing `ResetPairingCode` (a child process, repeated): no active credential survives the rotation (fails before `e52c3f7`) |
| `security::the_control_token_holds_until_the_control_server_stops` | the control endpoint refuses an upgrade without the token for as long as the control server runs, whatever the process registry holds |
| `security::debug_text_of_token_bearing_records_redacts_the_token` | `Debug` of `ReadyLine`, `WorkerRecord`, `WorkerEndpoint` never prints the token |
| `security::unpair_revokes_inbound_access_within_one_second` (extended) | no new session of the dialer is served during the unpair |
| `logging::every_daemon_installs_its_log_and_reports_it` | a console daemon writes `DATA/logs/orchestrator.log` and `manage status` reports its path |
| `logging::a_removed_log_directory_is_made_again_at_the_next_rotation` | `RotatingFile` recreates a removed directory |
| `logging::c_runtime_writes_follow_the_captured_streams` (extended) | the replaced log handles are not inheritable during the settle (fails before `7f0b33d`: `Some(true)`) |
| `logging::worker_log_rotates_at_spawn_and_never_keeps_the_ready_token` (extended) | `worker.log` is 0600 on Unix |
| `derive_oracle::a_content_successor_that_tips_a_cone_over_the_cut_limit_is_refused` | CCR-WP4.4-5 (fails before: the save was acknowledged and the history no longer rebuilt) |
| `seal_path::a_journal_index_without_its_keys_is_rebuilt` | a `journal_branch` table left without its marker and cursor is backfilled from the start |
| `seal_path::large_projections_resume_exactly_after_crashes` (release) | the crash scenarios above `FOLD_MAX_ENTRIES` (a startup with a conflict and a tail, a slow commit keeping its parts) |
| `activation_inbox::a_busy_writer_leaves_the_staging_cache_as_it_was` | a failed `BEGIN IMMEDIATE` restores the raised page cache |
| `activation_inbox::injected_busy_is_retried_and_committed_never_quarantined` (extended) | a slice that moves a row to retry reports it (`retried`) |
| `availability_index::the_cached_want_scan_follows_every_change` | the cached want scan is invalidated by every kind of change, on this and on another connection |
| `feed_v7::an_idle_feed_exchange_moves_no_generation` | an exchange that moved nothing bumps no status generation, a moving one does |
| `feed_fuzz::fuzz_seeds_debug`, `feed_fuzz::seeds_found_by_the_release_run`, `feed_fuzz::feed_fuzz_release` (release, ignored) | the format 7 feed fuzz (2–4 replicas, random topologies, lost requests and replies, partial activation, restarts, restores, private canaries; oracle after every step; convergence, acks and per-id evidence at the end); the two seeds the release run found |
| `status_hub::an_endpoint_scoped_refusal_is_a_daemon_error` | CCR-WP3.1-3 |
| `status_hub::a_heartbeat_older_than_the_mount_result_keeps_the_controllers_mount` | CCR-WP3.4-1 |
| `management_api::error_scopes_of_unlisted_peers_and_endpoints_read_once` | CCR-WP4.3-1 and the endpoint scope in the CLI text |
| `gui_model::error_lines_show_a_first_time_that_differs_and_unlisted_peers_once` (extended) | the desktop's endpoint scope line |
| `objects_v3_worker::a_release_with_a_hold_hides_the_objects_from_their_source` | `ReleaseHeader.hold_ms` |
| `objects_v3_lanes::qual_wp4_5_control_latency_under_a_saturated_lane` (extended, release) | the saturated bulk session's own ping p99 < 200 ms |
| `w3_integration::a_loopback_send_buffer_follows_its_own_horizon` | `SendBuffer::for_loopback` (10 ms horizon, whatever the round trip) |
| `folder_controller::a_folder_stopped_at_start_is_published_unavailable` (extended) | a rename while the folder runs reaches its entry |
| `docs_cli::the_checker_refuses_unknown_commands_options_and_incomplete_lines` (extended) | five lines with an option missing its value are refused |
| `contracts` (extended) | the peer ops' registration and the objects.exchange bounds |
| `qual_wp4_int_soak::soak_two_daemons_through_faulty_links` (release, ignored) | the two-hour soak (WP4.INT task 4) |

## VERIFY

Run in `C:/Users/felyp/Desktop/Projetos/Telekinesis-wt/wp4-int`,
`CARGO_TARGET_DIR=C:/tkfs-target/wp4-int` (Windows 11), on the final code.

| Command | Result |
|---|---|
| `cargo fmt --check` | clean (exit 0) |
| `cargo build --all-targets` | ok (exit 0) |
| `cargo clippy --all-targets -- -D warnings` | clean (exit 0) |
| `powershell -NoProfile -File C:/tkfs-build/suite-lock.ps1 cargo test --all-targets --no-fail-fast` (at `9dc5720`; later commits change docs, notes and evidence only) | **ok, exit 0: 72 test binaries, 1,062 passed, 0 failed, 123 ignored** (WP4.3's base: 972 passed, 104 ignored). An earlier run at `860c6f7`: the same counts, exit 0 |
| WSL: `cargo check --all-targets` (`CARGO_TARGET_DIR=$HOME/tkfs-target/wp4-int`) | ok (exit 0) |
| WSL: `cargo test --no-fail-fast` for `orchestrator`, `session_core`, `session_handshake`, `worker_lifecycle`, `mount_lifecycle`, `security`, `logging`, `feed_v7`, `feed_fuzz`, `seal_path`, `objects_v3_worker`, `objects_v3_lanes`, `derive_oracle`, `activation_inbox`, `status_hub`, `availability_index`, `docs_cli`, `inbound_v3`, `peer_supervisor` (at `9dc5720`) | 19 binaries: **534 passed, 1 failed, 23 ignored**; the failure is `objects_v3_lanes::a_saturating_push_does_not_delay_pulls_on_a_slow_uplink` (12.8 s against its 12 s bound with the whole binary running on a loaded host; 3 of 3 alone pass; *Known limitations*). The first subset run (at `4f64579`) failed `objects_v3_lanes::an_object_of_unknown_size_pushed_in_part_is_handed_on` once, a wait that the 5 s idle recheck could exceed: fixed (`860c6f7`), then 6 of 6 alone and the binary (48 passed) |

Per binary (Windows): `security` 21, `logging` 20, `feed_v7` 45,
`feed_fuzz` 2 + 1 ignored, `derive_oracle` 12 + 2 ignored, `seal_path` 18 + 2
ignored, `activation_inbox` 22, `availability_index` 22, `status_hub` 97,
`gui_model` 45, `management_api` 28, `folder_controller` 57, `docs_cli` 5,
`contracts` 40, `objects_v3_worker` 41, `objects_v3_lanes` 48 + 18 ignored,
`w3_integration` 10, `peer_supervisor` 25, `orchestrator` 18,
`qual_wp4_int_soak` 0 + 1 ignored.

## Qualification

All on the final code (no `src/` change after `4f64579`; later commits
change tests' timing, the soak's workload, docs and evidence).

### `qualify.ps1 -All` (Windows): `test-evidence/qualification-all.json`

`TMP`/`TEMP` on `F:` (SATA SSD, as WP4.2's run), `-Priority Normal`,
`TKFS_BASELINE_BIN` = the lab's 6298d0b release build, at `27802c3` with a
clean tree, 4,190 s. **`summary.ok`: true: 91 scenarios, 90 passed, 1
skipped (`sigterm_matches_shutdown`: no SIGTERM on Windows); 54 of 54
floors held**, every Wave 1–4 floor and the four `qual_wp4_4_seal` floors
included. The measurements are in `docs/VALIDATION.md` (*Floors*) and, as
the §15 before/after table, in `qualification-final.json`. Highlights
against WP4.2's `-All` (which missed the idle-CPU floor at 18.85 %): idle
worker CPU 0.52 %; tiny files 771 / 770 files/s and tiny objects 907/s
(floor 300); the 10K catch-up 10.8 s; the 150K catch-up's seals 72× smaller
than WP3.INT's with the longest seal 8.6 ms.

A preliminary `-All` run at `d73d0ae` (before the feed fix; its later files
ran on `4f64579`'s library, rebuilt in the same target meanwhile), made
while 25 release fuzz and crash processes ran beside it, missed five
fsync- and latency-bound floors (`fs_p99_during_catchup` 74 ms,
`objects_worker_256mib` 23 MiB/s, `objects_worker_tiny_10k` 28/s,
`e2e_tiny_files_10k` 264/s, `e2e_catch_up_10k` 72 s) and passed the other
49, the idle-CPU floor included (0.47 %); it is not the evidence.

### `qualify.sh --all` (WSL): `test-evidence/qualification-all-linux.json`

`TMPDIR` and the target on the WSL image's ext4 (disk-backed, 837 GB free),
at `2a7d96b` with a clean tree, 3,223 s; `qual_wp1_9_session` re-run at
`2e62858` with `--merge` after its race fix (the first run failed
`dead_peer_detection` once: the server's own dead timer closed the link at
the same moment, so the client read the reset as `CONNECTION_LOST`; 1 run in
4 alone). **`summary.ok`: true: 92 scenarios, 91 passed, 1 skipped (the
6298d0b upgrade: no Linux baseline binary); 52 of 54 floors held, every
enforced one**; the two misses are the Windows-only tiny-file floors (219
and 196 files/s on ext4), recorded and not enforced. WP4.2's Linux run
had failed the idle-CPU floor (18.3 %) and `inbound_status`; here 0.37 % and
pass.

### Release oracles and crash workloads (`CARGO_TARGET_DIR=C:/tkfs-target/wp4-int/rel2`, `4f64579`)

| Run | Seeds / workloads | Result |
|---|---|---|
| `derive_oracle::fuzz_300_seeds_release`, `TKFS_DERIVE_SEEDS=100` × 10 shards from 1000 | **1,000 seeds** (1000–1999), 100–180 steps | **ok**, every shard (37–52 min each, run in parallel): 76,214 append-mode commits, 39,280 verified (append and full compared), 44 fallbacks, 115,556 slow-path commits checked, 0 refusals |
| `derive_oracle::fuzz_stress_release`, 4 shards × 10 from 5000 | **40 seeds** (5000–5039), four rarely syncing replicas, 200–320 steps | **ok** (167–2,149 s per seed), 62 namespace-budget refusals, each one a full-mode refusal too; seed 5026, which failed WP4.4's run (CCR-WP4.4-5), passes |
| `feed_fuzz::feed_fuzz_release`, 4 shards × 250 from 10000 | **1,000 seeds** (10000–10999), 100–200 steps, 2–4 replicas | **ok** after the fixes (the first run failed seed 10023, the evidence defect of Summary 5, and seed 10631, the harness's convergence loop stopping after a round that only re-synchronized cursors): 41,769 events, 63,207 exchanges, 6,026 lost replies, 4,722 lost requests, 4,510 restarts, 2,097 restores, 1,201 private branches; convergence, acks and per-id evidence on every route |
| `seal_path::random_workloads_and_crashes_release`, 4 shards × 5 seeds | **40 workloads** (seeds 0–19 in both layouts) | **ok** |
| `seal_path::large_projections_resume_exactly_after_crashes` | 14 crashes above `FOLD_MAX_ENTRIES` | **ok** |
| `offlock_commit::prepared_commits_300_seeds_release` | 300 seeds | **ok** |
| `meta_scheduler::anti_entropy_at_30_s_keeps_one_session_for_90_s` | — | **ok** |

### Objects v3 lane scenarios (`objects_v3_lanes -- --ignored`, release, `9dc5720`)

WP3.3's and WP4.5's measurement scenarios with `PEER_CALLS` = 4 and 4
exchange slots per bulk session (CCR-WP4.5-2): **18 passed** (the two that
need a remote server skip themselves). Both directions at once through
50 ms / 20 MiB/s: download 0.967 and upload 0.773 of the link (WP4.5
measured 0.97 / 0.775 with its scratch copy of the change); a control-lane
ping p99 under a saturated bulk lane 52.6 ms (< 100 ms; idle 51.4 ms), the
saturated session's own p99 140 ms (< 200 ms); 1 GiB through 50 ms /
20 MiB/s at 0.974 of the link, through 50 ms / 100 Mbit/s at 0.986; tiny
files on loopback 605/s (disk spools), session throughput 2.8 GiB/s down,
2.6 GiB/s up.

### Soak (`tests/qual_wp4_int_soak.rs`)

Five runs, all on the final code (release; `rel2` / `rel3` targets of this
worktree). The criteria are the plan's: equal feeds (journals) and
projections, all content, nothing staged, every event acknowledged, both
oracles empty, neither folder stalled, blocked or needing attention.

| Run | Workload | Result |
|---|---|---|
| **default, 2 h** (`9dc5720`, the evidence) | own names, 10 % on the shared pool, an operation every 1–4 s per side | **pass** (7,257 s): 4,889 edits, 180 fault phases, 178 proxy resets, 662 connections; 159 of 240 per-minute samples `up_to_date`, the rest the fault in progress; settled in 0.01 s after the edits; 5,630 shared events on each side, equal journals and projections, oracles empty |
| storm, 8 min (`9dc5720`) | every operation on the shared pool, one every 50–400 ms per side, settle 1 min | **pass**: 3,408 events, 236 unresolved conflicts, settled in 1.6 s |
| storm, 20 min (`9dc5720`) | the same, settle 5 min | **pass**: 7,082 events, 1,300 unresolved conflicts, settled in 1.6 s, the oracle check about a minute |
| storm, 2 h (`4f64579`, the first run) | the same | **fail**: did not converge in the 30 min settle (1,549 and 2,617 events waiting; 8,973 / 7,905 events, 9,399 / 5,861 unresolved conflicts); the final oracle check had not finished after 3 h and the run was stopped |
| fast editing (`2a7d96b`, the second run) | own names, 10 % shared, an operation every 50–400 ms per side | stopped after 91 min: activation falling behind (21.5K events and 100–107 unresolved conflicts per side; 140 events waiting at 30 min, 485–615 at 60, 900–1,070 at 80, 1,500–1,600 at 91; 2,420 / 2,504 stale prepared replays) |

The two failures are CCR-WP4.INT-3 (a throughput limit of conflict
derivation, not a correctness one: every oracle that finished was empty,
and the stores of the passing storm runs converged exactly). The default
workload is the deviation-2 change; VALIDATION (*Soak*) and the README
state the limit.

### Upgrade from 6298d0b

In the `-All` run: `qual_wp3_int_e2e::legacy_fixture_upgrade_then_sync`
(the 6298d0b data directory fixture opens in place, its folder syncs to a
new host and back) **pass**;
`qual_wp3_1_transport::upgrade_against_a_6298d0b_peer_fails_closed_on_both_sides`
(two 6298d0b daemons paired both ways, one upgraded in place: both sides
fail closed with `PEER_NEEDS_UPGRADE`) **pass**; `v2_peer_fails_closed`
(both files) **pass**. In the suite: `catalog_actor`'s 6298d0b catalog
upgrade and downgrade tests and `harness_selftest`'s legacy fixture tests
**pass**.

### Two-machine lab

`test-evidence/lab/` (11 files), summarized in `docs/VALIDATION.md`
(*Two-machine lab*): the coordinator's run of 6298d0b against the Wave 4
base (`e8c2979`). Against the previous release: all paths and hashes
visible 2.0 s / 6.8 s after `receive` (161.6 s / not reached in 30 min),
all content at the tunnel's ceiling, two-way edits converged in 52.5 s /
120.8 s (1,031.6 s / not reached), no worker `status` call answered busy or
stale (63 of 95 / 454 of 892). Two findings: the resume after a 45 s
outage waited about 50 s after the lanes reconnected (the base's lease
lapse plus stall backoff; WP4.5's release of a lost round's leases removes
it, not re-measured), and the mounted read of a 512 MiB file still
downloading failed after 15 s (CCR-WP4.INT-2).

## Docs

Checked every passage WP4.3's notes list for WP4.INT against the merged
code (`586296a`, `d73d0ae`, `c26ac0d`, `4f64579` and this commit):

- **README**: the `revoked` row (this release never shows it: a refused
  credential is dropped and the dialer retries with its code); the folder
  state table's order (`Paused` wins over a worker that is not running, the
  review's minor); the endpoint error scope; what the initial-sync gate
  covers (metadata) and how a read of content still downloading behaves
  (CCR-WP4.INT-2).
- **ARCHITECTURE**: `peer.enroll` / `peer.revoke` in the op table;
  `catalog.list` on control and probe sessions (the review's minor); 4
  exchange slots per bulk session; snapshot format 2 (CCR-WP4.4-2: layouts,
  marks, the resumable journal hash, `current_meta`, the downgrade path);
  objects v3's exclusion list, `objects.release` and the 5 s idle recheck;
  socket buffers on loopback only.
- **CLI**: `remote-list`'s `label` is the folder's name on that host
  (CCR-WP4.3-2); the worker `status`'s top-level `caught_up` fields
  (CCR-WP4.INT-1).
- **VALIDATION**: the floors table with the final measurements (Wave 4 and
  `qual_wp4_4_seal` included), the soak and its result, the release oracles,
  the two-machine lab summary.
- **REVAMP_PLAN**: CCR-WP4.4-3, CCR-WP4.1-9, CCR-WP4.5-1 (e), `PEER_CALLS`
  4 and the push gate, the peer ops in the §6.2 table.
- `test-evidence/lab/`: the lab's eleven result files (`.gitignore` lets the
  directory in).

## Package status

| Package | Review | Merged | Blockers / majors | Minors and open issues |
|---|---|---|---|---|
| WP4.1 security | **not approved** | `783314f` | the `peer.enroll` / rotation race (invariant 21): **fixed** (`e52c3f7`) | unpair re-dial, `ws::accept` fail-open, the Ready-line exemption: fixed; CCR-WP4.1-6 (optional) not applied; per-peer credentials keyed by `(installation, side)` ratified in §9 |
| WP4.2 operations | approved | `4a2f264` | — | `-All` / `qualify.sh` green: now met (*Qualification*); `qualify.sh` pass-on-failed-scenario, `RotatingFile::reopen`, the inherited handle: fixed; the notes' settle statement: corrected here (deviation 4) |
| WP4.3 docs | approved | `ca8a887` | — | `docs_cli` value checks, README state order and `revoked` row, `catalog.list` on probe sessions: fixed; CCR-WP4.3-2: applied |
| WP4.4 seal path | approved | `e362eee` | — | `journal_branch` setup atomicity, staging cache on a failed `BEGIN`, large-projection crash coverage: fixed; CCR-WP4.4-5: fixed; CCR-WP4.4-1: open (performance) |
| WP4.5 objects cleanup | approved | `d8c392f` | — | more than 256 objects in backoff (`hold_ms`), the saturated-session latency check, the loopback send-buffer test, mixed-version adoption (warned once), the acceptor's leases on a failed want: fixed; the Wave 3 tiny-files floors: **hold** in the final `-All` (771 / 770 files/s, 907 objects/s, scenario data on `F:`) |

## Known limitations

- **The two-machine lab ran the Wave 4 base** (`e8c2979`), before the
  Wave 4 packages merged; it was not run again on this branch (it needs the
  two machines and the coordinator's harness). Its after-outage resume wait
  (about 50 s after the lanes reconnected) is consistent with the base's
  lease lapse plus stall backoff, which WP4.5's release of a lost round's
  leases removes; that gain is not measured on the lab
  (`docs/VALIDATION.md`, *Two-machine lab*).
- **Reads of content still downloading** wait up to 15 s and then fail
  (CCR-WP4.INT-2); the lab's mounted check hit it with a 512 MiB file.
- **The worker `status`'s top-level `caught_up` and `metadata_caught_up`**
  stay `false` on the v3 path (CCR-WP4.INT-1); documented.
- **Staging cost** keeps the SQL trigger that parses each staged payload
  (CCR-WP4.4-1, about 65 against 17 µs per event); every staging floor
  holds.
- **Probe sessions** are not reported as inbound lanes (CCR-WP2.8-4,
  optional).
- **A stale reply applied again at the store level** is accepted when it
  still binds to its request id (`apply_exchange_reply`); the session layer
  delivers each reply once and `feed-apply` is never replayed, so it is not
  reachable from the network (deviation 1). The fuzz can exercise it with
  `TKFS_FEED_STALE_REPLIES=1`.
- **The desktop manual checklist** was not run in this package: the desktop
  changes of Wave 4 are text (`endpoint <url>` scopes, folder labels in
  *Browse folders*), covered by `gui_model`; WP3.4 ran the checklist.
- **The Docker steps of the upgrade runbook** were not executed (as in
  WP4.3).
- **Timing floors on a shared machine**: the evidence records the load at
  the start of each run; the fsync-bound floors depend on the disk of the
  temporary directory (recorded too).
- **Concurrent two-way editing with unresolved conflicts** (CCR-WP4.INT-3):
  activation falls behind a peer that keeps writing fast (an operation
  every 50–400 ms per side) or once thousands of conflicts are unresolved;
  the soak passes at one operation every 1–4 s per side with 10 % shared
  names, and its storm runs pass up to about 1,300 unresolved conflicts. A
  full rebuild of a store with thousands of unresolved conflicts takes hours.
- **`objects_v3_lanes::a_saturating_push_does_not_delay_pulls_on_a_slow_uplink`
  in WSL** goes over its 12 s bound when its binary runs all its tests at
  once on a loaded host (12.8 s in the final subset run; 3 of 3 alone pass),
  as WP4.5 and WP3.INT recorded; it passes on Windows. The Linux socket path
  is unchanged in Wave 4.
- **The fsync-bound floors depend on the disk**: the final `-All` kept
  scenario data on the SATA SSD (`F:`), as WP4.2's did; on the NVMe system
  disk they measured 310–315/s at Wave 3 and below 300/s in WP4.5's runs
  while that disk was full.

## Commits

On `revamp/wp4-int` (first parent; every message starts `WP4.INT: `):

| Commit | Subject |
|---|---|
| `4a2f264` | merge WP4.2 (operations: logs, diagnostics, qualification floors) |
| `783314f` | merge WP4.1 (security: per-peer credentials, revocation, local tokens) |
| `e362eee` | merge WP4.4 (seal path and inbox staging cost) |
| `d8c392f` | merge WP4.5 (objects v3 cleanup and tuning) |
| `ca8a887` | merge WP4.3 (documentation) |
| `e52c3f7` | WP4.1 blocker - peer.enroll checks the verifier generation in the catalog transaction |
| `5992236` | CCR-WP4.1-3 - the control server requires the daemon's token itself |
| `e905cdf` | CCR-WP4.2-1 every daemon installs its log; CCR-WP4.1-4 owner-only worker.log |
| `b9debf9` | CCR-WP4.4-5 - a concurrent content successor on a tombstoned entity counts its cuts |
| `59a224e` | WP4.4 review minors and WP4.1 unpair minor |
| `d3eae4c` | CCR-WP4.5-2 - four exchange slots per bulk session, PEER_CALLS 4 |
| `bd509fa` | CCR-WP4.5-1 - interim objects.exchange docs and bounds removed |
| `56f631f` | qualification wiring - CCR-WP4.4-4, CCR-WP4.2-6, qualify.sh file status |
| `19805ca` | CCR-WP4.2-5 - an idle worker does no O(objects) work per sync interval |
| `93be9fc` | CCR-WP3.1-3 Scope::Endpoint, CCR-WP4.3-1, feed fuzz and soak scenarios |
| `fd921b8` | CCR-WP4.1-2, -5, -8 - peer ops registered, worker_call token, redacted Debug |
| `9f22963` | CCR-WP3.4-1 - the hub keeps the controller's mount over older heartbeats |
| `c80b998` | WP4.5 review minors; CCR-WP4.1-10 and -13 test margins |
| `92c93ce` | CCR-WP4.2-2 activation.retried counts slice retries; a cleared log directory is made again |
| `e1d06aa` | docs_cli refuses an option without its value (the WP4.3 review's minor) |
| `9c6ad45` | docs_cli help parser self-test reads an optional value |
| `d73d0ae` | CCR-WP4.3-2 - a running folder is published with its catalog label |
| `586296a` | docs and plan reconciled with the merged wave 4 |
| `7f0b33d` | a replaced standard handle is not inherited while its switch settles |
| `c26ac0d` | two-machine lab results in test-evidence/lab, summarized in VALIDATION |
| `4f64579` | an entry listed while we lacked its event becomes evidence once we hold it |
| `860c6f7` | objects_v3_lanes hand-on test waits for the idle recheck |
| `f9c22d8` | the soak's editors mostly edit names of their own |
| `27802c3` | notes (draft; results follow with the final runs) |
| `2a7d96b` | final Windows qualification (qualify.ps1 -All): 54 of 54 floors |
| `2e62858` | dead_peer_detection - the server declares the link dead later than the client |
| `5dabe00` | Linux qualification (qualify.sh --all, WSL): every enforced floor |
| `9dc5720` | the soak's edit rate is configurable, one operation every 1-4 s by default |
| (this commit) | notes, `qualification-final.json`, VALIDATION's soak results |
