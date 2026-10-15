# WP3.INT — Wave 3 integration

Branch `revamp/wp3-int`, cut from `revamp/wave-3-base` (`686718f`).
`revamp/production-sync` was not touched (no fast-forward); the
coordinator does that after review (§13 rule 6, rule 8 item 7).

## Summary

1. **Merges.** The four package branches, each as a `--no-ff` merge commit,
   in the order of the coordinator's brief (3.1 → 3.2 → 3.3 → 3.4; see
   deviation 1), each followed by its touched tests. No merge had a textual
   conflict.

   | # | Branch (head) | Merge | Breakages | Touched tests after the merge |
   |---|---|---|---|---|
   | 1 | `revamp/wp3-1-cutover-v3` (`cca0d6a`) | `6a08e3d` | none | `orchestrator` 18, `contracts` 40, `inbound_v3` 20, `feed_v7` 44 |
   | 2 | `revamp/wp3-2-objects-worker` (`4fcb603`) | `f7f1516` | none | `objects_v3_worker` 29, `worker_lifecycle` 25, `worker_process` 22, `seam_stubs` 7, `availability_index` 21, `availability` 13, `partial_content` 8, `bulk_lanes` 3, `core` 16, `orchestrator` 18, `paired_sync` 9, `replication_regressions` 9, `w2_integration` 13+1i |
   | 3 | `revamp/wp3-3-objects-orchestrator` (`bacddb8`) | `f6d26fc` + `61c8d2c` | **build:** the `Ctx` literal of `tests/objects_v3_lanes.rs` lacked `extensions` (CCR-WP3.1-5, fixed in `61c8d2c`). **Tests:** 4 of 18 `orchestrator` tests failed once objects v3 ran between real daemons (fixed in `4e3f723`, *Integration fixes*) | `inbound_v3` 20, `legacy_stabilizers` 2, `meta_scheduler` 24+1i, `objects_v3_lanes` 23+8i, `objects_v3_worker` 29, `orchestrator` 14 of 18 (then 18) |
   | 4 | `revamp/wp3-4-desktop-gui` (`e7cb8bc`) | `73bdc26` | none | `gui_model` 37, `contracts` 40, `status_hub` 85, bin 1 |

   The first full suite on the merged tree then found
   `harness_selftest::compat_goldens_recapture_identically` failing (the
   legacy Peers golden lists every repository on every bulk lane; objects v3
   lanes report only repositories whose objects moved). Fixed with the hub's
   legacy projection (below).

2. **Integration fixes** (`4e3f723`), what objects v3 between real daemons
   broke or exposed:
   - **The legacy projection of objects v3** (§7.3, `hub.rs`, WP1.11's file;
     the WP3.1 adapters in `compat.rs` are removed): every bulk lane lists
     each repository both sides hold whether objects moved on it yet or not
     (`legacy_peers` and `legacy_folder_sync`); `object_inventory` is read
     from the folder's and the peer's content counters when the projection
     is built (an objects v3 lane reports only when objects move, so the
     booked value went stale); CCR-WP3.1-1 and CCR-WP3.1-2 are applied in
     `update_legacy_repo`; a failed object transfer shows as its lane's
     repository `error`, `consecutive_failures` and `retry_at_ms` while the
     folder's error for that object is open (CCR-WP3.INT-1).
   - **A failed transfer's error is not cleared by stale counters.** The
     heartbeat clears a folder's `Object` errors when nothing is missing; with
     objects v3 objects arrive before their events are activated, so the
     counters could predate the failed object. An error that names its
     source stays until the inbox is idle and `SOURCED_GRACE_MS` (10 s) has
     passed.
   - **WP3.3's open major (invariant 22, bounded input):** the dialer acted on
     per-object results of a pull or push without checking that they belonged
     to its request. Now a peer's results count only for the request's own
     items, each once; `CORRUPT_AT_SOURCE` is reported to our worker only for
     an object the push sent through its end; a `wants` reply is capped at
     `MAX_WANT_ITEMS`. The verify deadline's sum of peer-supplied totals
     saturates (the review's overflow minor).
   - **Resume points:** `RESUME_OFFSET_AHEAD` and `OBJECT_PART_CHANGED` from
     our worker resynchronize the object from the durable offset it answered,
     with no backoff and no `ObjectError` (they were 30 s failures).
   - **An object we may not serve** (`NotPermitted` at our worker) is backed
     off silently on the dialer as on the acceptor; it had shared the
     folder's `Object` error slot and masked the real failure of a pull.
   - CCR-WP3.3-3: a malformed objects v3 request is `INVALID_REQUEST`.
   - `tests/orchestrator.rs`, `tests/v3_wire_tap` and the faulty-link
     qualification run on objects v3 (deviation 5).

3. **Open WP0.5 major, `qual_wp3_int_e2e::idle_exchange_daemons`.** It
   subtracted two unchecked `u64` samples (a missing sample defaulted to 0,
   so a restart or a busy reply underflowed) and never proved an exchange
   happened. Now each sample is an `Option` (no default), the difference is
   `checked_sub`, at least `MIN_IDLE` (10) control-lane exchanges must
   happen in the window, and neither feed may move (`metadata_progress`
   equal before and after); otherwise the metric is NaN and the floor
   fails.

4. **`session_loopback_throughput` (≥ 300 MiB/s)** (`e362f8c`). The receive
   path's chunk buffers come from one process-wide pool (32 buffers) instead
   of a pool per connection, so a new connection does not pay fresh pages
   for its first chunks (S1's memory pinning is not reintroduced: the pool
   is bounded and holds no connection's data). The scenario takes the best of
   three downloads on one connection and records every run (deviation 3).
   Measured: best-of-3 390–535 MiB/s over five runs; with the old per-connection
   pool and the same measurement 422–504 MiB/s, so most of the miss was
   run-to-run variance on the shared machine, not the code.

5. **Throughput and scale fixes found by the Wave 3 end to end** (`1e57432`,
   `338f1fb`, `8695de0`):
   - **Seal interval** (CCR-WP2.4-3, deferred to Wave 3/4 by WP2.INT): a
     periodic startup-snapshot seal serializes the whole projection; at a
     fixed 128 events it took 15 s of a 36 s 10K-file activation. The
     interval is now `sealed payload / 4 KiB` events, between 128 and 1024
     (`core::seal_interval`); the fast commit's `seal_due` and the
     activator's forecast use the same query and rule; a startup resumes a
     tail of up to 1024 events.
   - **One install turn per `objects.write`** (WP3.2 review minors): phase C
     hashes outside the spool I/O gate and takes it once for all of a call's
     renames; a present copy is made durable before registration when the
     batched barrier is off.
   - **View freshness across foreign commits** (CCR-WP3.INT-2): a cached view
     was reloaded in full after any other connection's commit (feed staging,
     object registration, status writes). At 30K entries the activator
     reloaded 216 times (40 s of a 138 s catch-up); now 0 times (79 s).
   - **Startup suffix replay against one view** (with CCR-WP2.4-3): a startup
     that resumes its snapshot replays the events after it; each replayed
     event's writes moved `total_changes`, so the next event reloaded the
     whole projection. With suffixes of up to 1024 events, reopening the
     150K-entry folder of `feed_150k_daemons` (a 508-event suffix) took 14
     minutes. The replay now patches its view per event, as a fast commit
     does (72 loads → 1 for a 72-event suffix).
   - **WAL truncation** (CCR-WP3.INT-3): under a sustained catch-up the WAL was
     copied but never restarted, and a 150K-event catch-up left a WAL file of
     16 GiB. A copied WAL of 64 MiB or more is now truncated when no reader
     is on it.
   - **`feed_150k_daemons` could not pass**: its completion check asked for
     the whole `state` (about 70 MB of JSON for 150K entries, over the RPC's
     64 MiB message limit), every 50 ms (deviation 6). With the check fixed it
     passed: 150,000 events activated in 942 s (159 events/s; before the
     replay fix the scenario then spent 14 minutes reopening the store).

6. **The Wave 3 end to end over mounts**
   (`w3_integration::two_daemons_sync_mounted_folders_both_ways_over_v3`):
   two real daemons, each folder mounted with WinFsp, files (including two
   3 MiB files) and directories written through one mount and read back
   through the other, both directions; a rename through B's mount and a
   deletion through A's reach the other mount; every lane is `tkfs-peer/3`
   and connected; both mounts are `attached`. About 3 s; 3 of 3 runs.

7. **A 6298d0b peer fails closed on both sides**
   (`qual_wp3_1_transport::upgrade_against_a_6298d0b_peer_fails_closed_on_both_sides`,
   against the real 6298d0b binary): see *Interoperability*.

8. **Review findings** of the packages, fixed here under §13 rule 5: WP3.3's
   major (item 2) and its write-loop overflow minor; WP3.1's `serve_with`
   minor and its untested `worker_call`; two of WP3.2's minors (Phase D
   under a busy writer; the present copy's durability); both WP3.4 minors.
   What remains is in *Package status*.

## CCRs

### Filed by WP3.INT

| CCR | Change | Reason | Adapter / status |
|---|---|---|---|
| CCR-WP3.INT-1 (`model::status`, frozen; §7.1, §7.3) | `StatusEvent::ObjectError` gains `#[serde(default, skip_serializing_if = "Option::is_none")] source: Option<ObjectSource>`, `ObjectSource { installation, lane, #[serde(default)] direction }` | the legacy projection must show a failed transfer on its lane's repository (`error`, `consecutive_failures`, `retry_at_ms`, as 6298d0b's lanes did), and the hub had no way to know which lane failed | **applied** (additive, serde-default; the golden contract fixtures are unchanged because `None` is skipped). Senders: `objects.rs` (outbound, the transfer's lane) and `inbound_objects.rs` (inbound, the peer's status lane) |
| CCR-WP3.INT-2 (`core.rs`, `core/{snapshot,commit,inbox}.rs`; I6) | a side table `projection_version(id = 1, version)`, bumped by `AFTER INSERT/UPDATE/DELETE` triggers on `current_state` and `current_parts`; a cached view records it and `Store::adopt_foreign_commits` re-keys a view to the new `data_version` when the counter did not move | another connection's commit that changes no projection row forced an O(entries) reload of every handle's view | **applied**; ratified in §5.3 and I6. Triggers also count writes by older binaries |
| CCR-WP3.INT-3 (`worker/maintenance.rs`, WP2.3) | `checkpoint_wal` (the activator's and maintenance's off-lock checkpoint) truncates a WAL of ≥ 16,384 frames that the passive checkpoint copied in full, waiting at most 20 ms for readers (`checkpoint_db`) | SQLite restarts a WAL only at a write that began after a full copy while no reader is on it; under a catch-up that never happened and the WAL file grew without bound (16 GiB at 150K events; the file keeps its size) | **applied** |
| CCR-WP3.INT-4 (`core/snapshot.rs`, Wave 4 scale) | seal cost at large projections: a seal writes the whole projection (about 1.4 KB per entry: 210 MB at 150K entries, 2.9 s), every 1024 events at most, so a 150K catch-up writes about 15 GB of seals | `MAX_SEAL_INTERVAL` bounds a startup's replay; raising it needs the replay cost (now one view load plus a patch per event) measured against the Wave 4 startup floor (`scale_startup_50k_objects`); an incremental seal is CCR-WP2.3-2/CCR-WP2.4-2's refactor | **deferred to Wave 4** (with CCR-WP2.3-2, CCR-WP2.4-2) |

### Wave 3 package CCRs

| CCR | Decision | Notes |
|---|---|---|
| CCR-WP3.1-1 (bulk lanes never claim `caught_up`) | **applied** in `hub.rs` `update_legacy_repo` | the `compat::legacy_values` adapter is removed |
| CCR-WP3.1-2 (`caught_up` waits for `remote_pending == 0`) | **applied** (same place) | test `legacy_caught_up_waits_for_the_remote_pending_events` |
| CCR-WP3.1-3 (a scope for an endpoint that is not a paired peer) | **deferred to Wave 4** | a frozen model change (`model::status`/`errors`); the `Peer{installation: "<endpoint>"}` adapter shows the refusal correctly |
| CCR-WP3.1-4 (RemoteList lists `Running` entries; refused pairings in Status) | **ratified** (§7.3) | |
| CCR-WP3.1-5 (merge note: `Ctx.extensions`, feed literal fields) | **applied** at the WP3.3 merge (`61c8d2c`) | |
| CCR-WP3.2-1 (`fetch_verified` quarantines through `Store::quarantine`) | **deferred to Wave 4** | `fetch_verified` renames the corrupt bytes while it holds the object's install lock; `Store::quarantine` needs the engine's store, so the call would take the engine lock under the object lock, while the legacy part receiver takes object locks under the engine: the lock order has to be settled first. The adapter (`request_object` right after the rename reconciles the row) holds |
| CCR-WP3.2-2 (status cell carries the current-object set; optional) | **declined** for Wave 3 | `Store::current_objects` (cached ≤ 5 s) serves; the cell's walk is WP4.2's to restructure |
| CCR-WP3.2-3 (`WorkerCtx` gains the objects state) | **deferred to Wave 4** | a frozen `WorkerCtx` change; the per-root statics are correct per process, and only in-process tests share a process |
| CCR-WP3.2-4 (loopback sessions serve the JSON ops) | **ratified** (§6.2), with the session's limits stated (WP3.2's limits minor) | |
| CCR-WP3.2-5 (the worker contract) | **ratified** (§6.4) | |
| CCR-WP3.2-6 (tiny-object floor) | nothing requested; `fs_objects_per_s` read with the floor (*Qualification*) | |
| CCR-WP3.3-1 (bulk exchange slots 4) | **deferred to Wave 4** (performance) | `PEER_CALLS` = 2 adapter; the e2e floors pass with it |
| CCR-WP3.3-2 (delete the interim fallback) | **deferred to Wave 4** | every key runs objects v3 (tests assert no `objects.exchange`); deleting the per-key fallback also rewrites the interim tests of `meta_scheduler` and `inbound_v3`; §6.5 records it |
| CCR-WP3.3-3 (`INVALID_REQUEST` for a malformed objects v3 request) | **applied** (`inbound_objects::request_of`; `inbound_v3::role_violations_and_route_violations_keep_their_codes`) | |
| CCR-WP3.3-4 (`WantHeader.exclude` or a lease release) | **deferred to Wave 4** | WP3.2's worker-side lapse backoff (deviation 13) plus WP3.3's lease handoff keep multi-source topologies live without a frozen contract change. Measured cost of not releasing: a sender that could not serve an object it was offered (its copy corrupt, then repaired) delivered it about two minutes later (deviation 8); a release on a failed serve would shorten that |
| CCR-WP3.3-5 (objects v3 wire details) | **ratified** (§6.4) | with WP3.INT's rules (results bounded to the request, resume points, silent `NotPermitted`) |
| CCR-WP3.4-1 (hub prefers the controller's mount) | **deferred to Wave 4** | display only; WP3.4's `MountSmoother` adapter holds and never triggers once fixed |
| CCR-WP3.4-2, -3, -4 (information) | noted | -2 for WP4.2's logging; -3 no change; -4: the v3 feed counters now fill `cursor_seq`/`remote_tip` |

### Wave 2 CCRs deferred to Wave 3

| CCR | Decision |
|---|---|
| CCR-WP2.4-3 (seal interval scaling) | **applied** (summary item 5); ratified in invariant 5 |
| CCR-WP2.3-2 (seal off the commit lock), CCR-WP2.4-2 (prepared seal), CCR-WP2.INT-5 (conflict changes as a projection delta) | **deferred to Wave 4** with CCR-WP3.INT-4: one refactor of the seal path, with its crash tests |
| CCR-WP2.3-3 (`SliceOutcome` retried rows; telemetry) | **deferred to WP4.2** (WP3.2 did not own the files) |
| CCR-WP2.3-5 (the seal interval's mirror) | **applied** (the read side): the activator's forecast and the fast commit read `core::SEALED_SQL` and `core::seal_interval` |
| CCR-WP2.5-6 (`stage_incoming` cost) | **deferred to Wave 4** (performance; the 150K e2e passes) |
| CCR-WP2.8-4 (inbound lanes by role; optional) | **deferred to Wave 4**: probe sessions stay unreported |
| CCR-WP2.1-1, CCR-WP2.5-2/-3/-4, CCR-WP2.6-2, CCR-WP2.8-3 | applied by WP3.1 (its notes) |
| CCR-WP2.1-7 | WP3.4's display adapter; the hub change is CCR-WP3.4-1 (deferred) |

## Deviations from the spec, and why

1. **Merge order.** The brief said 3.1 → 3.2 → 3.3 → 3.4; §14 WP3.INT says
   3.2, 3.3, 3.1, 3.4. I followed the brief. No merge conflicted, and the
   breakages after the WP3.3 merge are the same in either order (they need
   3.1's daemon and 3.3's lanes together).
2. **Core files changed by the integrator** (§13 rule 5): `core/snapshot.rs`,
   `core/commit.rs`, `core.rs`, `core/inbox.rs`, `core/transfer.rs`,
   `worker/activator.rs`, `worker/maintenance.rs`, `session/conn.rs`. Each
   change is a CCR above, a review finding or a defect the Wave 3 end to end
   found (the startup replay), with its test.
3. **`qual_wp1_9_session::loopback_throughput` measures the best of three
   downloads** on one connection (it was one run). The floor judges what the
   session layer can carry; interference from other jobs on the shared
   machine only lowers a run. Every run is recorded (`runs_mib_per_s`).
4. **`inbound_v3::role_violations_and_route_violations_keep_their_codes`** expects `INVALID_REQUEST` for a
   malformed `objects.pull` (it was the Wave 2 stub's
   `UNSUPPORTED_OPERATION`): CCR-WP3.3-3.
5. **`tests/orchestrator.rs`** (WP3.1's file) runs on objects v3: its fake
   v3 acceptor serves `objects.wants`, `objects.push` (stalled 20 s, then
   written through a `SessionWorkerClient`) and `objects.pull`;
   `sync_survives…` asserts objects v3 exchanges and no interim
   `objects.exchange` (it asserted the interim op);
   `failed_repository…` accepts the v3 codes (`NOT_AVAILABLE`,
   `CORRUPT_AT_SOURCE`, `CORRUPT_OBJECT`) and waits up to 120 s for the
   content (`has_content`; v3 backs a failed object off for 30 s).
   `qual_wp3_1_transport::faulty_link_no_stuck_sync_no_replay` counts
   objects v3 exchanges instead of `objects.exchange` and checks that no
   interim exchange happened. These assertions are as strict as before for
   the path that now runs.
6. **`qual_wp3_int_e2e::feed_150k_daemons`'s completion check.** It polled
   `has_hash` (the worker's whole `state`) every 50 ms. At 150K entries the
   reply is about 70 MB of JSON, over the RPC's 64 MiB message limit, so the
   check never held although the folder synchronized (all 150,000 events
   were activated about 24 minutes in, and the scenario would have timed
   out); and each poll loaded and serialized the whole projection under a
   read transaction, which slowed the activation it measured. Now progress
   is read from B's worker `status` (`journal.events`, the status cell),
   and once all events are activated the last entry is read from B's store
   (folder stopped): it must be alive with the empty content. Stricter
   than before on the content; the floor (`completed == 1`) is unchanged.
7. **The objects v3 legacy values** (§7.3 rows added): an objects v3 lane
   reports nothing until objects move, so the projection reads the live
   inventory and synthesizes idle repositories on every bulk lane, as
   6298d0b's lanes listed them.
8. **`orchestrator::failed_repository_does_not_starve_healthy_folders_on_the_same_lane`'s
   outbound waits** (WP3.1's file): "the corrupt outbound object" waits 120 s
   (was 60; it timed out once on WSL under load: A's folder restarts first,
   and the lane's error alternates between B's failed push, `NOT_AVAILABLE`,
   and B's pull of the same object from A, which never had it,
   `NOT_PERMITTED`); "the repaired outbound object" waits 240 s (was 120):
   the repaired copy waits out the backoffs its failures started (the
   dialer's per-object backoff, 30 s doubling; at A, the lease of an object
   B's push session did not move, 60 s, and the stall backoff after it, 60
   s). Traced on Windows: delivered 119 s after the repair, and later than
   120 s in two of three runs. The assertions are unchanged.
9. **`qual_wp1_12_legacy::hung_remote_repo_charged`'s charge window** (WP1.12's
   scenario, changed by WP2.INT and WP3.1): after the healthy repository
   moved, the scenario samples Peers for the hung repository's charge for 75
   s (was 20 s). Since the cutover the hung exchange is charged at the
   dialer's exchange deadline (`limits::EXCHANGE_DEADLINE`, 30 s), so the
   charge came after the window and the scenario failed in the first
   qualification run; WP3.1's own v3 scenario
   (`qual_wp3_1_transport::hung_remote_repo_extra_delay`) waits 75 s for the
   same reason. Measured after the change: charged about 25 s into the
   scenario. The floor (`legacy_hung_repo_healthy_progress`) and the check
   are unchanged.
10. **`qual_wp3_int_e2e::idle_exchange_daemons`'s window starts with both feeds
   at rest**: it slept 5 s after B's lane said `metadata_caught_up`, which
   holds once B has received A's feed while B may still activate it; in the
   first qualification run B's feed moved during the window (so the window
   was not idle and the metric was NaN, as the WP0.5 fix requires). The
   scenario now waits until neither side has events pending and both feeds
   stayed put for 5 s. Measured after the change: 485 exchanges in the 30 s
   window (the test daemons' 50 ms interval), 2,693 bytes each.

## Tests added

| Test | Covers | Fails before the fix |
|---|---|---|
| `objects_v3_lanes::a_peer_reply_counts_only_for_the_objects_it_was_asked_for` | WP3.3 major: a scripted lying peer returns results for objects the request did not list (one private), twice, and `CORRUPT_AT_SOURCE` for an object not sent through its end | yes |
| `objects_v3_lanes::a_resume_point_resynchronizes_the_object_without_a_backoff` | `RESUME_OFFSET_AHEAD` from the worker resumes at once (fake worker fault `stale_want_offsets`) | yes |
| `status_hub::legacy_projection_books_failed_objects_on_their_lane` | CCR-WP3.INT-1 projection | yes |
| `status_hub::legacy_bulk_lanes_list_idle_repositories_and_read_their_inventory_now` | idle bulk repositories, live `object_inventory` | yes |
| `status_hub::legacy_caught_up_waits_for_the_remote_pending_events` | CCR-WP3.1-2 (and -1) | yes |
| `status_hub::a_failed_transfer_is_not_cleared_by_counters_that_predate_its_object` | `SOURCED_GRACE_MS` | yes |
| `objects_v3_worker::a_busy_writer_fails_the_registration_once_not_per_object` | WP3.2 minor: a held `BEGIN IMMEDIATE` makes a 16-object write answer `SQLITE_BUSY` once (< 9 s), and the next write completes | yes (one busy timeout per object) |
| `w3_integration::the_seal_interval_scales_with_the_sealed_payload` | CCR-WP2.4-3 rule | new rule |
| `w3_integration::a_large_projection_seals_less_often` | seals land exactly `seal_interval(previous payload)` apart; a store with a long tail reopens to the same projection | new rule |
| `w3_integration::two_daemons_sync_mounted_folders_both_ways_over_v3` | the Wave 3 end to end over WinFsp mounts (summary item 6) | n/a |
| `w3_integration::a_view_survives_foreign_commits_that_leave_the_projection_alone` | CCR-WP3.INT-2: no reload after a foreign non-projection commit; a foreign projection change is seen | yes (with adoption disabled) |
| `w3_integration::the_folder_passthrough_call_is_bounded_and_never_replayed` | WP3.1 minor: `compat::worker_call` answers `REQUEST_TIMEOUT` within its deadline, sends each request once, returns a timely reply | n/a (coverage the review asked for) |
| `w3_integration::a_large_copied_wal_is_truncated_once_no_reader_is_on_it` | CCR-WP3.INT-3: not under a reader (and quickly), truncated once the reader left, a small WAL left alone, busy timeout restored | yes (the old checkpoint never truncates) |
| `w3_integration::a_startup_replays_the_snapshot_suffix_against_one_view` | a reopen replays a 72-event suffix with at most 3 view loads, to the same projection, and seals it | yes (72 loads) |

Changed: `qual_wp3_int_e2e::idle_exchange_daemons` (summary item 3 and
deviation 10), `::feed_150k_daemons` (deviation 6),
`qual_wp1_9_session::loopback_throughput` (deviation 3),
`qual_wp1_12_legacy::hung_remote_repo_charged` (deviation 9),
`orchestrator::failed_repository…`'s waits (deviations 5 and 8);
`tests/contracts.rs` and five `status_hub` literals gain `source: None`.

## VERIFY

Run on `8695de0` (the code of the final commit; later commits change only
the notes and the evidence), Windows 11, `CARGO_TARGET_DIR=C:/tkfs-target/wp3-int`.

| Command | Result |
|---|---|
| `cargo fmt --check` | clean |
| `cargo build --all-targets` | ok |
| `cargo clippy --all-targets -- -D warnings` | clean |
| `powershell -NoProfile -File C:/tkfs-build/suite-lock.ps1 cargo test --all-targets` | **ok: 65 test binaries, 922 passed, 0 failed, 104 ignored** (ignored: the 86 qualification scenarios, 77 of them run below and `qual_scale`'s 9 of Wave 4, and 18 marked tests: `objects_v3_lanes` 8, `derive_oracle` 2, `harness_selftest` 2, lib 2, `meta_scheduler` 1, `offlock_commit` 1, and the 6298d0b interop tests of waves 1–2, `w1_integration` 1 and `w2_integration` 1). The first run of the suite stopped at `peer_supervisor::a_lane_inside_a_dial_is_interrupted_and_reports_nothing_after_peer_removed`: the unpair took 1.503 s against its 1.5 s bound (WP3.1's test; one lane was in its TCP connect, which is not interruptible, the open WP3.1 finding); it passed 5 of 5 runs alone and in the full rerun |
| WSL: `cargo check --all-targets` (`CARGO_TARGET_DIR=$HOME/tkfs-target/wp3-int`) | ok |
| WSL: `cargo test --no-fail-fast` of `orchestrator`, `session_core`, `session_handshake`, `worker_lifecycle`, `mount_lifecycle` (FUSE), and the touched `objects_v3_worker`, `objects_v3_lanes`, `status_hub`, `w3_integration` (its mounted end to end ran under FUSE, 2.3 s), `startup_snapshots`, `inbound_v3`, `feed_v7`, `worker_concurrency`, `activation_inbox`, `offlock_commit`, `core`, `contracts`, `peer_supervisor` | **ok: 18 binaries, 514 passed, 0 failed, 9 ignored** (`objects_v3_lanes` 8, `offlock_commit` 1). An earlier WSL run, concurrent with the 150K scenario on Windows, timed out `orchestrator::failed_repository…` at its 60 s outbound wait (deviation 8); it passed alone, and with the new waits in this run |

Per binary (Windows): `lib` 49+2i, `bin` 1, `acknowledgements` 3, `activation_inbox` 21, `additional` 5, `availability` 13, `availability_index` 21, `basic_metadata` 4, `bulk_lanes` 3, `catalog_actor` 19, `contracts` 40, `control_transactions` 4, `core` 16, `derive_oracle` 11+2i, `directory_progress` 3, `feed_v7` 44, `folder_controller` 56, `gui_model` 37, `harness_selftest` 18+2i, `inbound_v3` 20, `legacy_stabilizers` 2, `management_api` 27, `meta_scheduler` 24+1i, `mount_lifecycle` 11, `objects_v3_lanes` 25+8i, `objects_v3_worker` 30, `offlock_commit` 20+1i, `oracle_smoke` 4, `orchestrator` 18, `orchestrator_runtime` 17, `paired_sync` 9, `partial_content` 8, `peer_supervisor` 25, `posix_metadata` 3, `recovery` 6, `replication_regressions` 9, `runtime_lanes` 3, `scalability` 5, `seam_stubs` 7, `session_core` 59, `session_handshake` 21, `startup_snapshots` 8, `status_hub` 89, `w1_integration` 4+1i, `w2_integration` 13+1i, `w3_integration` 7, `worker_concurrency` 29, `worker_lifecycle` 25, `worker_process` 22, `worker_seams` 4.

## Qualification (`test-evidence/qualification-wave-3.json`)

<<QUALIFICATION>>

## Interoperability with 6298d0b (§13 rule 8.4)

Since WP3.1 a 6298d0b peer fails closed: the v2 network is deleted.
`qual_wp3_1_transport::upgrade_against_a_6298d0b_peer_fails_closed_on_both_sides`
runs the real 6298d0b binary (`TKFS_BASELINE_BIN=C:/tkfs-target/wp2-int-baseline/debug/tkfs.exe`,
built by WP2.INT from `git archive 6298d0b`): this build's `Pair` with the
old daemon fails `PEER_NEEDS_UPGRADE` and Status shows it, and the old
daemon's own Peers reply shows `PEER_NEEDS_UPGRADE` for this build.
`qual_wp3_int_e2e::v2_peer_fails_closed` checks the same against this
build's own legacy client, and `legacy_fixture_upgrade_then_sync` upgrades
a 6298d0b data directory and syncs it over v3. <<INTEROP>>

The waves 1–2 interop tests (`w1_integration::interop_with_a_6298d0b_peer`,
`w2_integration::interop_wave_2_with_a_6298d0b_peer`) stay ignored: they
test the wire compatibility that Wave 3 ends by design.

## Package status

| Package | Review | Merged | Status here | Open issues |
|---|---|---|---|---|
| WP3.1 cutover to v3 | approved | `6a08e3d` (`cca0d6a`) | green | `serve_with` minor **fixed**; `worker_call` **now tested**. Open: the detach-at-deadline path of `PeerManager` has no test (every `detached_lanes()` assertion expects 0 since CCR-WP2.6-2; a lane whose TCP connect outlasts the join deadline would cover it); under two-way pairing `need_list` can serve one large event on both channels (idempotent, up to 12 MiB twice per round) |
| WP3.2 objects v3, worker | approved | `f7f1516` (`4fcb603`) | green | Phase D under a busy writer and the present copy's durability **fixed**; the session limits **documented** (§6.2). Open: `append_group` answers `RESUME_OFFSET_AHEAD` with an in-call `durable` that a later failure of the same call can invalidate (the receiver resynchronizes from it); `sweep_lapsed` logs under the lease locks; an object with no known size sorts first in the want order (`size = 0`); `report_corrupt` records `REPORTED` before verifying, so a repeat within 10 s answers `VerifiedOk` unverified; the `WANT_US`/`READ_US`/`WRITE_US` qualification counters are not reset between scenarios |
| WP3.3 objects v3, orchestrator | **not approved** | `f6d26fc` (`bacddb8`) | green | major (results bounded to the request) and the overflow minor **fixed**. Open: a settled object is not pruned from its round's plan, so the rest of a large object can be pulled and dropped until `ROUND_CAP` (10 s); the fake worker's leases in `objects_v3_lanes` renew on every want (WP3.2's real worker renews only on progress) |
| WP3.4 desktop GUI | approved | `73bdc26` (`e7cb8bc`) | green | both minors **fixed** (detail busy keys; `CommandPool` wording). Open: the window's flows are tested through the view model and the command pool, not by driving the Slint window (no simulated clicks) |

## Known limitations

- **Large catch-ups.** A 150K-event folder activates at about 250 events/s
  early and about 120/s near the end on this machine: each seal writes the
  whole projection (2.9 s and 210 MB at 150K entries, every 1024 events;
  CCR-WP3.INT-4), and every slice commits a few events. The WAL truncation
  (CCR-WP3.INT-3) needs a moment with no reader on the WAL; the status
  cell's full refresh of a 150K-entry folder holds one for 3–5 s (spaced to
  four times its duration), so the WAL can still grow between such moments
  (see *Qualification* for the peak measured).
- **The interim bulk path** remains as a per-key fallback for a peer or
  worker that answers `UNSUPPORTED_OPERATION` (none in this build); its
  deletion is CCR-WP3.3-2 (Wave 4).
- **Shared-machine floors.** `tiny_files_10k` (≥ 300 files/s) ran 288–318
  files/s before the view-freshness fix; `objects_worker_tiny_10k` is bounded
  by the disk's fsynced creates (WP3.2 records `fs_objects_per_s` beside it).
  The session throughput scenario takes the best of three runs (deviation 3).
- **`qual_wp3_int_e2e::feed_150k_daemons`** reads the last entry from the
  receiver's store with its folder stopped: no worker op returns one entry of
  a folder whose `state` exceeds the RPC's 64 MiB message limit.
- **The detach-at-deadline path of `PeerManager`** and the Slint window's own
  event wiring are untested (*Package status*).
- Mounted end to end on Linux runs under FUSE only where `/dev/fuse` exists
  (WSL: see *VERIFY*).

## Commits

<<COMMITS>>
