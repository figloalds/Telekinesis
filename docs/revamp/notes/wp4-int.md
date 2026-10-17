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

TBD-RESULTS

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
  `src/worker/activator.rs`; performance): in a history with thousands of
  unresolved conflicts, each conflict-producing remote event falls back to a
  full replay (off-lock prepare, then commit), whose cost grows with the
  history and the conflicts: in the first soak run (*Qualification*, every
  edit of both sides on one shared name pool for two hours: 8,973 events and
  9,399 unresolved conflicts on one side) one event's prepare took minutes
  and went stale four times in 15 min before it was committed inline, a
  full rebuild of that store (a copy) had not finished after 20 min, and
  activation fell behind for good (1,549 and 2,617 events waiting after the
  30 min settle). Requested: keep the slow path's cost proportional to the
  touched cones when the append precondition fails for cones the event does
  not touch, or prepare one replay for several queued slow events, and do
  not throw a prepared replay away when the branch moved only by fast
  commits of unrelated entities (1,171 stale outcomes in that run). Adapter:
  none; the soak's default workload keeps conflicts occasional (deviation 2)
  and README / VALIDATION describe the limit.

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
   the run. It writes the same `QUAL` records (its evidence is in
   `qualification-final.json`). `TKFS_SOAK_MINUTES` (default 120) and
   `TKFS_SOAK_SETTLE_MINUTES` (default 30) shorten it for dry runs.
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

TBD-VERIFY

## Qualification

TBD-QUAL

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
| WP4.5 objects cleanup | approved | `d8c392f` | — | more than 256 objects in backoff (`hold_ms`), the saturated-session latency check, the loopback send-buffer test, mixed-version adoption (warned once), the acceptor's leases on a failed want: fixed; the Wave 3 tiny-files floors: TBD-TINY |

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
TBD-LIMITS2

## Commits

TBD-COMMITS
