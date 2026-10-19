# WP5.INT — Wave 5 integration and qualification

Closure addendum (2026-10-19): the user authorized final integration into
`master` with long tests skipped. This report preserves the original package
history; [the handoff](wave-5-handoff.md) records subsequent integration,
recovered evidence, focused checks and pending continuation decisions.

Branch `revamp/wp5-int`, cut from `revamp/wave-5-base` (`c6948c9`),
worktree `C:/Users/felyp/Desktop/Projetos/Telekinesis-wt/wp5-int`. Spec:
REVAMP_PLAN §14.5b (*WP5.INT*) and the §13 rule 8 checklist.
`revamp/production-sync` was not touched (no fast-forward, no
`revamp/wave-6-base`): the coordinator does that after review.

## Summary

1. **Merges**, each a `--no-ff` merge commit, in the order of §14.5b (5.4,
   5.3, 5.2, 5.1). No merge had a textual conflict; the touched tests ran
   after each (*VERIFY*, per merge).

   | # | Branch (head) | Review | Merge |
   |---|---|---|---|
   | 1 | `revamp/wp5-4-leftovers` (`6e67ca0`) | not approved (blocker: fix round 2 uncommitted) | `a1526c2` |
   | 2 | `revamp/wp5-3-net-recovery` (`b7df80c`) | approved | `8b2dddb` |
   | 3 | `revamp/wp5-2-local-fs` (`77d1621`) | not approved (one major) | `d536964` |
   | 4 | `revamp/wp5-1-concurrent-activation` (`a5c1e9b`) | approved | `987ab92` |

2. **The two packages not approved.**
   - **WP5.4**'s blocker: its fix round 2 (a one-line doc comment in
     `src/orchestrator/ctx.rs`, `install_log`'s `\\?\`) was never committed
     in its worktree. Applied here (`05796d8`), with the review's minor (the
     flaky test's comment now states the measured rate, 12 of 1,000 runs).
   - **WP5.2**'s major (a deferred create restored late took the name of a
     newer one, which then never committed and kept the engine dirty) and
     its minors (renewed demand reloading the projection, duplicate names in
     listings) are fixed (`9e84933`, `cf5c106`); its flake notes are below.
3. **Every CCR the wave assigned to the integrator is applied** except the
   ones under *CCRs, not applied* (CCR-WP5.1-2 snapshot format 3,
   CCR-WP5.2-3, the optional CCR-WP5.4-5), with reasons.
4. **The approved packages' review minors**: WP5.1's two (a later `CORRUPT`
   verdict wins over the verified set; full mode and declined appends keep
   the view) are fixed; WP5.3's first (kept leases of a key a round still
   holds) is fixed, the other two are documented.
5. **Qualification** on the merged tree: the full suite (Windows and the WSL
   subset), the release oracles, the default two-hour soak and both heavy
   WP5.1 workloads, and `qualify.ps1 -All` with a floor for every WP5
   acceptance number; `test-evidence/qualification-wave-5.json`.
6. **Docs** follow the merged behaviour (README with a new *Known
   limitations* section and runbooks, ARCHITECTURE, VALIDATION,
   CONFIGURATION, the plan).

The preserved `qualification-all.json` records 102 passing scenarios and one
Windows-inapplicable SIGTERM scenario skipped, with 76 passing floors. The
separate completed conflict-storm and fast-two-way runs supply the remaining
three heavy floors: **79 distinct floors passed, zero failed** across those
records. These were run on `90ad052`, before this closure; they are not new
runs. See `test-evidence/qualification-wave-5.json` for the combined evidence
and `wave-5-handoff.md` for the user's abbreviated closure policy, current
focused checks, remaining limitations and continuation decisions.

## CCRs

### Applied by WP5.INT

| CCR | What | Commit |
|---|---|---|
| CCR-WP5.1-1 | `Store::put_object`, `put_stream` and their dedupe (`verify_existing`) record what they hash in `commit::verified` (`note_verified`); an installed object's stamped mtime is never racy | `cf5c106` |
| CCR-WP5.2-1 | the engine's phase 2 (`store_snapshot`) records the bytes it hashed in the verified set, so a save's commit no longer hashes the new object again under the engine lock; the savers' `SETTLE` (10 ms before each background commit, the adapter) is removed | `cf5c106` |
| CCR-WP5.2-2 | availability rows written on a handle's own connection (`put_object`, `put_stream`, the dedupe) keep that handle's view fresh (`Store::keeping_view`, over the inbox's `keep_view` / `rekey_view`, now `pub(super)`) | `cf5c106` |
| CCR-WP5.2-4 | `engine::lock_after_saves`: a graceful stop over RPC and every mutating control request wait (at most `QUIET_SAVES_WAIT`, 5 s) while the only unsaved files are closed ones being saved, instead of answering `BUSY_VIEW` | `1bab106` |
| CCR-WP5.2-5 | docs: §12's off-lock-save cut marked done, ARCHITECTURE's lock table and filesystem path, README's limitations (write-behind, deferred creates, content waits, `TKFS_DEFERRED_CREATE`) | `492e00b` |
| CCR-WP5.2-6 | `qual_wp5_2_local` in `harness_selftest` (`LATER_QUAL_FILES`) and its floors | `30ea6f4` |
| CCR-WP5.2-7 | `Store::note_corrupt_copy(object, len, mtime_ns)`: phase 2's corrupt copy is recorded without hashing it again, while the file is still that copy (`CorruptCopy` carries the length and mtime it hashed) | `cf5c106` |
| CCR-WP5.3-1 | `limits::LANE_RESET_AFTER` 30 s → 5 s, `limits::NETWORK_SPACING_RATIO` (30); `peer.rs`'s constants name them; §3.7's lane table | `cfdf608`, `492e00b` |
| CCR-WP5.3-2 | `inbound_objects::session_closed`: the inbound server reports a bulk session's end, and the leases of its offers end at once (on a thread of their own); the reaper keeps covering a session that stays open and silent | `cfdf608` |
| CCR-WP5.3-3 | `objects.demand` and `DemandHeader` / `DemandItem` / `DemandReply` / `MAX_DEMAND_ITEMS` in `model::protocol::objects`; `runtime::objects_rpc` re-exports them; §6.4's op table | `cfdf608`, `492e00b` |
| CCR-WP5.3-4 | README's backing-off row, ARCHITECTURE's connection row, lane states, leases and demand, §6.4; `qual_wp5_3_recovery` in `harness_selftest` and its floors; `tiny_objects_10k` re-run on the qualification disk (`-All`, `TMP` on `F:`) | `30ea6f4`, `492e00b` |
| CCR-WP5.4-1 | `inbound.rs` and `peer.rs` reach per-peer credentials through `CatalogApi::credentials` and the `*Credential` commands (`peer.rs`'s `CredentialCatalog` adapter over the contract); `Catalog::of`, `CATALOGS` and `catalog_key` removed; `tests/security.rs` and `tests/catalog_actor.rs` follow | `cfdf608` |
| CCR-WP5.4-2 | the plan's §3.2 and WP0.4 listing gain the credential contract | `492e00b` |
| CCR-WP5.4-4 | the worker route answers `folder-details` with the `status` op's `caught_up`, `metadata_caught_up` and `upload_state` (no engine lock held while it reads them) | `1bab106` |
| CCR-WP5.1-3 | ARCHITECTURE describes the slow path as built (appended projection, branch index, interned derivation, skipped rows, the seal's anchor, the verified set) | `492e00b` |
| CCR-WP5.1-4 | floors: (c) on `event_cost_5k_1k` ≤ 26.9 ms (10× WP4.INT's 269 ms); the storm-shaped (c') tracked on its own at ≤ 21.8 ms (5×, chosen here: the acceptance is (c), and (c') measured 8.2–9.9× with little margin to 10× on a shared machine) | `30ea6f4` |

The review findings fixed here (not CCRs):

| Package | Finding (severity) | Fix | Commit | Regression test |
|---|---|---|---|---|
| WP5.4 | fix round 2 not committed (blocker) | the `install_log` doc comment | `05796d8` | — (comment) |
| WP5.4 | the flaky test's comment gives a rate the notes do not measure (minor) | "12 of 1,000 runs" | `05796d8` | — |
| WP5.2 | a deferred create restored late overwrote a live new file's name-index entry (major) | `restore_pending` refuses a deferred create whose name another new file holds (`NameHeld`, `RECOVERY_REQUIRED`); the record stays unrecovered and is due again as soon as a deferred create commits; its commit then makes the name conflict | `9e84933` | `local_fs_throughput::a_late_restore_of_a_new_file_never_takes_the_name_of_a_newer_one` (before: "B is still a new file" failed, the overwrite) |
| WP5.2 | renewed demand on the engine's connection reloaded the projection (minor) | `request_object` keeps the view fresh (`keeping_view`) | `cf5c106` | `availability_index::object_bookkeeping_keeps_the_handles_view` (before: "an install, a dedupe or a demand reloaded the view") |
| WP5.2 | `children_visible` listed a name twice (minor) | a deferred create whose name a committed child holds is left out, as lookups do | `9e84933` | `local_fs_throughput::a_listing_never_shows_a_name_twice_for_a_new_file` (before: two ids listed) |
| WP5.1 | the verified set was read before the availability row, so a later `CORRUPT` verdict was ignored (minor) | the row first; the set only when the row is absent or `AVAILABLE`; `forget_served` also forgets the set's entry | `cf5c106` | `core::commit::tests::a_later_corrupt_verdict_wins_over_the_verified_set` (before: `unwrap_err` on `Ok`) |
| WP5.1 | `compute_slow` took the view unconditionally: full mode and declined appends loaded the projection again for their delta (minor) | full mode leaves the view in place; `derive::project_append_or_base` hands a declined base back, and `compute_slow` restores it (`restore_view`) | `cf5c106` | `derive_oracle::slow_commits_that_do_not_append_keep_the_view` (before: "full mode loaded the view 11 times for 11 slow commits, append mode 0") |
| WP5.3 | `release_kept` dropped the kept leases of a repository whose key a still-running round held (minor) | handed back to the lane (merged), so that round's settle gives them up | `cfdf608` | — (needs a round that outlives the runner's exit wait; covered by the existing lease tests) |

### Not applied

| CCR | Why |
|---|---|
| CCR-WP5.1-2 (snapshot format 3) | the stored projection's header and the rows anchor keep the whole conflict list (O(open conflicts) per conflict-changing commit). A format bump touches `tests/startup_snapshots.rs`'s pin, every seal and the downgrade path: a redesign at the end of the wave, not an integration step. Recorded under *Known limitations* (README, ARCHITECTURE); the storm converges with it |
| CCR-WP5.2-3 (re-key the view on a commit that overlapped another connection's) | performance only, at most one reload per second since the availability rows are batched once a second (WP5.2's adapter); changing the commit path's freshness rule (invariant I6) needs its own review and measurements |
| CCR-WP5.4-5 (optional; forget feed peers that stop exchanging) | needs a route signal from the orchestrator to the worker (a new op or a field); README *Known limitations* and `docs/CLI.md` state the limit |

### Filed by WP5.INT

- **CCR-WP5.INT-1** (`src/orchestrator/objects.rs`; performance, the WP5.3
  review's third minor): demand moves to the front only inside its
  repository's own busy round, or makes an idle key due; a lane runs at most
  2 repositories' rounds and turns (`REPOS_PER_LANE`) with no priority for
  demand, so with two other repositories busy on the owning lane a demanded
  object waits for one of their rounds to end (up to `ROUND_CAP`, 10 s).
  Requested: a busy round yields its slot and turn (keeping its leases) when
  another key of the lane is due because of demand, or `RepoGate` and the
  dispatch rank demand-triggered keys first. Adapter: none; README *Known
  limitations* states it.
- **CCR-WP5.INT-2** (`src/orchestrator/objects.rs`; the WP5.3 review's
  second minor): pull lease ids are per lane, so `release_kept` of a runner
  that ended can give up leases that the lane's *next* session's round kept
  under the same id, if that round ran and ended before the release reached
  the repository. Requested: a per-lane run generation (or a per-(peer,
  lane, repo) epoch bumped by each round) so a release skips a repository a
  newer runner entered. Impact is low: the worker still arbitrates, and the
  next want takes the lease again while it is free. Adapter: none.
- **Carried forward, open:** CCR-WP5.1-2 (snapshot format 3; header writes
  O(open conflicts)), CCR-WP5.2-3 (the view on overlapping commits),
  CCR-WP5.4-5 (forget feed peers that stop exchanging), CCR-WP4.4-1
  (staging's payload-parsing trigger, unchanged since WP4.INT).

## Deviations from the spec, and why

1. **The heavy WP5.1 workloads run apart from `-All`**, through
   `qualify.ps1 -Heavy -Filter <scenario>` (one evidence file each,
   `qualification-wave-5-conflict-storm.json` and
   `qualification-wave-5-fast-two-way.json`), as the soak runs apart: each
   takes about 2.5 h, and the three ran at the same time. Their floors carry
   `heavy: true`, which `qualify.ps1`/`qualify.sh` enforce only when the
   scenario ran (a plain `-All` records them `not_run`); `-Heavy` also gives a
   file 200 minutes (`90ad052`: the default 90-minute file timeout would
   have killed them; the first attempts, started without it, were stopped
   after two minutes and started again).
2. **The (c') floor is 5× (≤ 21.8 ms), not 10×.** CCR-WP5.1-4 left it to the
   coordinator: the acceptance's (c) is floored at 10× on its own scenario
   (`event_cost_5k_1k`, ≤ 26.9 ms); (c') measured 8.2–9.9× in WP5.1 and 13.3
   ms (8.2×) here, so a 10× floor would fail on any busy run.
3. **`test-evidence/qualification-wave-5.json` is a summary** (like WP4.INT's
   `qualification-final.json`): the `-All` run's own evidence is
   `qualification-all.json` (replacing WP4.INT's), and the heavy runs have
   their own files. It is written by a script from those files, the soak's
   `QUAL` records, the oracles' logs and the suite logs.
4. **The release oracles ran beside the soak and the heavy workloads**, at
   below-normal priority with their stores on the SATA SSD (`F:`), so the
   two-hour runs' fsyncs on the system disk were not shared with them.
   `qualify.ps1 -All` ran after them on the quiet machine (`TMP` on `F:`, as
   WP4.INT's, `-Priority Normal`).
5. **WP5.4's fix round 2 is applied by this package** (`05796d8`): its
   worktree is dirty and belongs to WP5.4; it was read, not modified.
6. **`SETTLE` is removed** (CCR-WP5.2-1 asked for it): with the verified
   set the commit no longer reads the new object, so nothing waits for the
   scanner under the lock. The copy floor held without it (*Qualification*).
7. **CCR-WP5.2-3 is not applied** (*CCRs, not applied*) and **CCR-WP5.1-2
   is carried forward**: both change commit or snapshot rules that need
   their own review; the qualification passes without them.
8. **No Linux qualification run** (`qualify.sh --all`): the package section
   names `qualify.ps1 -All`; the WSL check, clippy and test subset ran, and
   `qualify.sh` knows wave 5 (`--heavy` included).

## Existing assertions changed

| Test | Change | Why |
|---|---|---|
| `objects_v3_lanes::an_offer_whose_session_went_away_gives_up_its_leases` | the release after the close: `elapsed >= OFFER_GRACE` → `< OFFER_GRACE`; a second phase checks the reaper on a session that stays open and silent (`>= OFFER_GRACE`) | CCR-WP5.3-2: the inbound server now reports the session's end, so the leases end at once; the old assertion measured the reaper's delay, which the CCR exists to remove; the reaper itself keeps its own check |
| `catalog_actor::a_catalog_without_credentials_answers_none` | `assert!(Catalog::of(&fake).is_none())` removed (the `fake.credentials().is_none()` assertion stays) | CCR-WP5.4-1 removes `Catalog::of` |
| `security::the_catalog_keeps_credentials_per_installation_and_side` | `Arc::ptr_eq(&Catalog::of(&api).unwrap(), &catalog)` → `api.credentials().is_some()` | CCR-WP5.4-1; the property (the `dyn CatalogApi` reaches the catalog's credentials) is kept |
| `harness_selftest::floors_reference_existing_scenarios` | waves `1..=4` → `1..=5`; `heavy` must be a boolean | wave 5 floors |
| `harness_selftest::qualify_sh_runs_with_lf_and_crlf` | the message for `--wave 9`: `use --wave 1..4` → `use --wave 1..5` | `qualify.sh` knows wave 5 |
| `harness_selftest::qual_files_exist_and_ignores_name_their_owner` | also checks `LATER_QUAL_FILES` (the WP5 files) | CCR-WP5.2-6, CCR-WP5.3-4 (stricter) |

## Tests added

| Test | Covers |
|---|---|
| `core::commit::tests::a_later_corrupt_verdict_wins_over_the_verified_set` (unit) | WP5.1 minor 1 and CCR-WP5.1-1: an install recorded by `put_object`, a same-size rewrite that keeps the stamped mtime, this process's `CORRUPT` verdict and another process's `CORRUPT` row both win |
| `derive_oracle::slow_commits_that_do_not_append_keep_the_view` | WP5.1 minor 2: full mode loads the view no more often than append mode across 12 slow deliveries |
| `availability_index::object_bookkeeping_keeps_the_handles_view` | CCR-WP5.2-2 and the WP5.2 demand minor: installs, a dedupe, a streamed install and two demands leave the view fresh; a real change is still seen |
| `local_fs_throughput::a_saves_commit_does_not_hash_its_bytes_again` | CCR-WP5.2-1: in memory, from a staging file and a verified dedupe, no commit hashes the object again (before: 2 extra hashes) |
| `local_fs_throughput::a_late_restore_of_a_new_file_never_takes_the_name_of_a_newer_one` | the WP5.2 major, end to end: both files committed, a name conflict, the oracle empty |
| `local_fs_throughput::a_listing_never_shows_a_name_twice_for_a_new_file` | the WP5.2 listing minor |
| `local_fs_throughput::a_quiet_engine_is_awaited_while_only_closed_files_are_being_saved` | CCR-WP5.2-4: waits for slowed background saves, and not at all with a file open |
| `worker_feed_status::folder_details_tells_the_status_claims_of_the_feed` | CCR-WP5.4-4, over a real worker RPC server (before: `caught_up` false, `peer-not-configured`) |

Each fix's test was run against the code without the fix (the fix
reverted or disabled for that run, then restored) and failed as quoted.

## VERIFY

Run in `C:/Users/felyp/Desktop/Projetos/Telekinesis-wt/wp5-int`,
`CARGO_TARGET_DIR=C:/tkfs-target/wp5-int` (Windows 11), on the final code
(`30ea6f4`; every later commit changes docs, notes, evidence or the
qualify scripts only: `492e00b` docs, `90ad052` `qualify -Heavy`'s file
timeout).

| Command | Result |
|---|---|
| `cargo fmt --check` | clean (exit 0) |
| `cargo build --all-targets` | ok (exit 0) |
| `cargo clippy --all-targets -- -D warnings` | clean (exit 0) |
| `powershell -NoProfile -File C:/tkfs-build/suite-lock.ps1 cargo test --all-targets --no-fail-fast` | **ok, exit 0, at the first run: 79 test binaries, 1,157 passed, 0 failed, 137 ignored** |
| WSL: `cargo check --all-targets` (`CARGO_TARGET_DIR=$HOME/tkfs-target/wp5-int`) | ok (exit 0) |
| WSL: `cargo clippy --all-targets -- -D warnings` | clean (exit 0) |
| WSL: `cargo test --no-fail-fast` for the §13 rule 8 subset (`orchestrator`, `session_core`, `session_handshake`, `worker_lifecycle`, `mount_lifecycle`) and every file wave 5 or this package touched (`--lib`, `local_fs_throughput`, `pending_saves`, `partial_content`, `worker_concurrency`, `peer_supervisor`, `objects_v3_lanes`, `objects_v3_worker`, `inbound_v3`, `derive_oracle`, `activation_inbox`, `offlock_commit`, `seal_path`, `startup_snapshots`, `scoped_conflict_gate`, `catalog_actor`, `security`, `worker_feed_status`, `meta_scheduler`, `contracts`, `harness_selftest`, `availability_index`, `logging`, `folder_controller`), `--test-threads=4`, beside the heavy runs | **29 binaries: 773 passed, 1 failed, 28 ignored**. The failure: `objects_v3_lanes::a_saturating_push_does_not_delay_pulls_on_a_slow_uplink` ("a 64 KiB pull took 12.8 s under a push", bound 12 s), the load-bound test WP4.INT and WP4.5 recorded in WSL; re-run alone 3 of 3 passed (18.9–21.9 s per run), and its whole binary passed (53 passed, 18 ignored) |

The count against the packages: WP4.INT's suite was 74 binaries, 1,084
passed, 124 ignored. The packages add 65 tests and 13 ignored scenarios
(WP5.1 11 + 5, WP5.2 35 + 4, WP5.3 12 + 4, WP5.4 7) and five binaries
(`worker_feed_status`, `local_fs_throughput`, `qual_wp5_1_concurrent`,
`qual_wp5_2_local`, `qual_wp5_3_recovery`); this package adds 8 tests: 1,157
and 137. Per binary (Windows): `local_fs_throughput` 39, `derive_oracle` 15
+ 2 ignored, `availability_index` 23, `worker_feed_status` 4,
`objects_v3_lanes` 53 + 18 ignored, `objects_v3_worker` 42, `security` 28,
`catalog_actor` 21, `harness_selftest` 18 + 2 ignored, `peer_supervisor` 30,
the library 79 + 2 ignored.

**After each merge** (debug, the touched binaries):

| After | Ran | Result |
|---|---|---|
| WP5.4 | `--lib`, `catalog_actor`, `worker_feed_status`, `meta_scheduler`, `logging`, `security` | all passed |
| WP5.3 | `--lib`, `peer_supervisor`, `objects_v3_lanes`, `objects_v3_worker`, `meta_scheduler`, `inbound_v3`, `contracts`, `harness_selftest`, `catalog_actor`, `security` | all passed but one timing assertion, `objects_v3_worker::a_busy_writer_fails_the_registration_once_not_per_object` (its 9 s bound, with nine binaries running before it); 3 of 3 alone and its binary (42) passed |
| WP5.2 | `--lib`, `local_fs_throughput`, `mount_lifecycle`, `pending_saves`, `partial_content`, `worker_concurrency`, `worker_lifecycle`, `objects_v3_lanes`, `harness_selftest` | all passed |
| WP5.1 | `--lib`, `derive_oracle`, `offlock_commit`, `activation_inbox`, `seal_path`, `scoped_conflict_gate`, `startup_snapshots`, `local_fs_throughput`, `pending_saves`, `worker_concurrency`, `oracle_smoke`, `w2_integration`, `w3_integration`, `feed_v7` | all passed but `local_fs_throughput::many_concurrent_writers_never_stall_the_mount` once (a write over its 5 s bound, the binary taking 74 s against 48 s); 3 of 3 alone and 2 of 2 binary runs passed |

Neither timing failure came back in the full suite.

## Qualification

Preserved evidence (generated 2026-10-18, Windows):

| Record | Outcome |
|---|---|
| `test-evidence/qualification-all.json` | 102 pass, 1 skipped; 76 floors pass, 3 heavy floors not run in this invocation |
| `test-evidence/qualification-wave-5-conflict-storm.json` | 2-hour storm passes; settle 198.304 s; 1 additional floor passes |
| `test-evidence/qualification-wave-5-fast-two-way.json` | 2-hour two-way workload passes; settle 12.588 s, backlog trend 0.412 events/min; 2 additional floors pass |

The union covers all 79 floor IDs in the all-run record, without a failing
floor. The notes' earlier full-suite, oracle and default-soak outcomes are
retained as reported historical runs; the three files above are the preserved
machine-readable qualification records. Full raw logs remain in the original
integration worktree's `test-runs` directories. No soak, large fuzz campaign,
heavy qualification workload or full suite is rerun for the 2026-10-19 closure,
per the user's instruction to skip long tests and integrate. The final focused
checks and explicit omissions are recorded in the handoff and summary JSON.

## Package status

| Package | Review | Merged | Blockers / majors | Minors and open issues |
|---|---|---|---|---|
| WP5.1 concurrent-edit activation | approved | `987ab92` | — | verified set read before the row: **fixed** (`cf5c106`); `take_view` in full mode and on declined appends: **fixed** (`cf5c106`); CCR-WP5.1-1, -3, -4 applied; CCR-WP5.1-2 (format 3) open; acceptance met: oracles 1,000 + 40 seeds, (a) and (b) at 2 h, (c) and (d) floors |
| WP5.2 local filesystem path | **not approved** | `d536964` | late restore of a deferred create taking a newer one's name: **fixed** (`9e84933`) | renewed demand reloading the view: **fixed** (`cf5c106`); duplicate names in listings: **fixed** (`9e84933`); flakes noted by the review: `orchestrator_runtime::writes_behind_a_slow_catalog_never_lock_reads_out` and `worker_concurrency::a_slow_commit_never_waits_for_another_writer_under_the_engine_lock` did not fail in this package's runs (open, timing-bound); CCR-WP5.2-1, -2, -4, -5, -6, -7 applied, CCR-WP5.2-3 not applied |
| WP5.3 network recovery and demand | approved | `8b2dddb` | — | kept leases of a held key: **fixed** (`cfdf608`); release vs the next session's kept leases: CCR-WP5.INT-2; demand across repositories: CCR-WP5.INT-1; CCR-WP5.3-1 to -4 applied; `tiny_objects_10k` met on the qualification disk |
| WP5.4 leftovers | **not approved** | `a1526c2` | fix round 2 uncommitted: **applied** (`05796d8`) | the flaky-test comment's rate: **fixed** (`05796d8`); CCR-WP5.4-1, -2, -4 applied; CCR-WP5.4-5 (optional) open |

## Known limitations

- **Header writes grow with open conflicts** (CCR-WP5.1-2): each
  conflict-changing commit writes the conflict list into the stored
  projection's header and the startup snapshot's anchor, about 50 + 90 ms
  at 13,000 open conflicts (WP5.1's probe). The two-hour storm (24,269 open
  conflicts) converges, its backlog peaking at a few hundred events during
  fault phases.
- **A close during a burst of closes is not a durability point** (WP5.2,
  deviation 1 there): a hard crash can lose up to 72 closed files (never
  tear one), and a new file created through the WinFsp mount enters the
  journal at its first save. FUSE has no syncfs at protocol 7.31.
- **Reads of content that cannot arrive** fail after 60 s without progress
  or 240 s in all (`STATUS_DEVICE_NOT_READY` / `EIO`).
- **Demand across repositories** (CCR-WP5.INT-1) and **kept leases of the
  next session** (CCR-WP5.INT-2).
- **A long outage is found later** (the shared redial spacing reaches 60 s
  after 30 min); a silent link is detected by the session's liveness (20 s).
- **The worker's `caught_up` keeps a peer that stops exchanging for good**
  until a restart (CCR-WP5.4-5).
- **Timing-bound tests under load**: two assertions failed once each in the
  per-merge runs here (`objects_v3_worker::a_busy_writer_fails_the_registration_once_not_per_object`,
  9 s; `local_fs_throughput::many_concurrent_writers_never_stall_the_mount`,
  5 s) and one in WSL beside the heavy runs (`objects_v3_lanes::a_saturating_push_does_not_delay_pulls_on_a_slow_uplink`,
  12 s); each passed alone and in the full suite. The WP5.2 review's two
  (`orchestrator_runtime`, `worker_concurrency`) did not fail here. None
  is changed: they measure real bounds, which a loaded machine stretches.
- **Not run here**: the two-machine lab (the coordinator's, after this
  package), `qualify.sh --all` (deviation 8), the Docker steps of the
  upgrade runbook and the desktop manual checklist (no desktop change in
  Wave 5 beyond what `gui_model` covers).

## Commits

On `revamp/wp5-int` (first parent; every message starts `WP5.INT: `):

| Commit | Subject |
|---|---|
| `a1526c2` | merge WP5.4 |
| `05796d8` | WP5.4 fix round 2 - install_log's comment; meta_scheduler's comment |
| `8b2dddb` | merge WP5.3 |
| `d536964` | merge WP5.2 |
| `987ab92` | merge WP5.1 |
| `cf5c106` | CCR-WP5.1-1, CCR-WP5.2-1/-2/-7 and the WP5.1 review's minors |
| `9e84933` | WP5.2 review major and minor - deferred creates restored late; listings |
| `1bab106` | CCR-WP5.2-4 and CCR-WP5.4-4 |
| `cfdf608` | CCR-WP5.3-1/-2/-3, CCR-WP5.4-1 and a WP5.3 review minor |
| `30ea6f4` | wave 5 qualification wiring and floors |
| `492e00b` | docs and plan follow wave 5 |
| `90ad052` | qualify -Heavy gives a file 200 minutes |
| (this commit) | notes, evidence, VALIDATION's results |
