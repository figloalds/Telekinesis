# WP5.4 Leftovers: notes

Branch `revamp/wp5-4-leftovers`, cut from `revamp/wave-5-base` (`c6948c9`),
worktree `C:/Users/felyp/Desktop/Projetos/Telekinesis-wt/wp5-4`. Spec:
REVAMP_PLAN §14.5b, "WP5.4 Leftovers".

Files changed: the package's own `tests/meta_scheduler.rs`,
`src/model/orchestrator.rs`, `src/orchestrator/catalog.rs`,
`tests/catalog_actor.rs`, `src/worker/status_cell.rs`,
`src/orchestrator/ctx.rs`; one new test file, `tests/worker_feed_status.rs`
(§13 rule 4; deviation 1); and, under task 5 (WP4.INT review minors "in
files that no other Wave 5 package owns", fix round 1), `docs/CLI.md` and
`tests/logging.rs`, which no Wave 5 package owns; these notes. Fix round 2
corrects one doc comment in `src/orchestrator/ctx.rs`. No `mod` line was
needed.

## Summary

### 1. The flaky test (`meta_scheduler::a_request_id_is_never_sent_twice`)

**Cause.** The test waited for the `REQUEST_ID_REUSED` `ExchangeFinished`
event, then read the key's failure counter at once. The exchange's slot
thread records that event (`ctx.status.record`) *before* it hands the
result to the lane's loop (`Done`), and the loop counts the failure
(`scheduler::Engine::finish`, `key.failures += 1`) and publishes the
`SchedulerView` after that. Under load the read came first and found 0.

**Fix.** The test waits, up to 5 s, for the view's key to show
`failures >= 1` (it also tolerates the key not being in the view yet; the
old `lane.key` panicked then). The property it checks is unchanged: the
repeated id is never sent again (`meta.seen` stays 1, its id is the fixed
one), and the refusal is a counted failure. The scheduler is not changed:
the event-before-count order is not a defect (only the lane and the tests
read the `SchedulerView`; the hub counts failure streaks from the events
themselves, `hub_rules`).

The other tests of the file that read a counter after waiting for events
were checked: they assert one failure fewer than the events they waited
for (a key's next exchange starts only after its previous result was
counted), or a zero that cannot race upward, so only this one could race.

**Proof** (the spec asks for 200 runs under load with 0 failures). The test
binary built before the fix (`meta_scheduler_before.exe`, the base commit)
and after it were each run in a loop of single-test processes
(`--exact --test-threads=1`), from the worktree, while this package's full
suite ran (VERIFY below) beside the other packages' builds and release
runs on the shared machine:

| Binary | Load | Runs | Failed |
|---|---|---|---|
| after the fix | the full suite | 200 | **0** |
| before the fix | the full suite | 200 | 2 (runs 87 and 171) |
| after the fix | the full suite + 4 loops at once | 4 × 200 = 800 | **0** (0 in each loop) |
| before the fix | the full suite + 4 loops at once | 4 × 200 = 800 | 10 (3, 2, 4 and 1 per loop) |

So **1,000 runs after the fix, 0 failures**; 1,000 runs before it, 12
failures (about 1 in 83), every one `assertion failed:
lane.key("repo-a").failures >= 1` at `tests\meta_scheduler.rs:1257`, the
line the fix replaces. Each loop of 200 took about 1.5 to 2.5 minutes
(14:44 to 14:50, Windows 11, the suite at its first 20 to 25 binaries).

### 2. CCR-WP4.INT-1: the worker's `caught_up` follows the feed peers

`worker::status_cell` composes the worker `status` reply. Its top-level
`caught_up` and `metadata_caught_up` (and `upload_state`) counted only the
legacy lanes' observations (`Engine::last_sync`, `peer_sync`,
`sync_peers`), which only `paired_sync` sets: on the v3 path they stayed
`false` and `upload_state` said `peer-not-configured`, while the reply's own
`feed_peers` said `caught_up: true` (the two-machine lab; reproduced by the
new test against the old code: "not caught up after 10 rounds", both sides'
`feed_peers[0].caught_up` true, `upload_state: peer-not-configured`).

Now the peers are the legacy lanes' **and** the feed's: every replica with
feed counters in `ctx.sync_obs` (both ends of a feed exchange record them,
the dialer at `feed-apply`, the acceptor at `feed-exchange`). `caught_up`
holds when:

- the local conditions hold, as before: nothing unacknowledged
  (`outgoing_unacknowledged`), nothing to activate (`incoming_active`;
  quarantined and blocked rows do not count, CCR-WP1.6-3), no pending
  causal event, no unflushed file, a healthy store;
- there is at least one peer;
- each legacy peer passes the legacy rule (unchanged; a worker with no
  legacy peer skips it);
- each feed peer's last exchange said its metadata was caught up (§6.3.6
  `metadata.caught_up`: our cursor at its tip, nothing it listed missing,
  no row from it still to activate) **and is fresh**;
- every peer acknowledged every shared committed event (the per-peer
  inventory match, `peer_known`, as before; the feed's acks write the same
  rows). Its recomputation is triggered by a feed exchange too.

`metadata_caught_up` carries the same value, as it did. `upload_state`
counts the feed peers, so a v3 folder reads `queued` or
`peer-acknowledged`.

**The freshness rule.** The legacy rule's fixed 15 s suits the default
500 ms sync interval but reads a folder synced every 60 s (the longest
`sync_interval_ms` the daemon accepts) as behind for three quarters of
every interval, and the worker does not know the daemon's interval. So a
feed peer's last exchange counts for **three of that peer's exchange
intervals, at least 15 s and at most 180 s**, and the interval is measured
from the exchange stamps (`PeerFeedCounters::last_exchange_ms`) by
`FeedCadence` (kept per replica in the cell's bookkeeping, `CellMeta.feed`):

- the stamps are sampled by every cell refresh (maintenance about once a
  second, every activator slice and mutation) and every `status` call; a
  sampled gap is never shorter than the real interval;
- the cadence is the longest gap that ended in the current 90 s window or
  in the one before (`FEED_CADENCE_MAX`: the longest sync interval plus an
  exchange's 30 s deadline), so a burst of quick exchanges (a moved tip, a
  mutual pairing's two schedules) does not shorten it, and a shorter
  interval takes over within two windows;
- a gap above 90 s is an outage and is left out, so an outage never
  stretches the window;
- while no gap is known (the first exchange after the worker started), the
  15 s floor holds.

### 3. CCR-WP4.1-1: per-peer credentials in the catalog contract

`model::orchestrator` gains:

- the credential types, moved from `orchestrator::catalog` (which
  re-exports them, so `catalog::{CredentialSide, Credentials,
  PeerCredential}` still resolve): `CredentialSide` (`parse` is public
  now), `PeerCredential` (the key stays private; `PeerCredential::new`,
  `active_key`), `Credentials` (`get`, `active`, `iter`, `len`, `is_empty`,
  plus `insert` and `FromIterator` to build one); `Debug` never shows a
  key;
- `CatalogCmd::EnrollCredential { installation, side, key, paired_only,
  generation }`, `RevokeCredential { installation, side }` and
  `ForgetCredential { installation, side }`, with WP4.1's and WP4.INT's
  semantics (`CANNOT_PAIR_WITH_SELF`; `paired_only` refused
  `PEER_NOT_PAIRED` unless a peer record names the installation;
  `generation` refused `PAIRING_CODE_CHANGED` unless the verifier is still
  at it; both checked in the command's transaction). `Debug` redacts the
  key; `name()` is `enroll_credential`, `revoke_credential`,
  `forget_credential` (the names the actor already logged);
- `CatalogApi::credentials(&self) -> Option<Arc<Credentials>>`, a default
  method answering `None`: a catalog that keeps no credentials (every test
  fake, unchanged) means code only, as `Catalog::of` returning `None` did.

The catalog actor's private `Work` and `CredentialCmd` are gone: credential
commands are `CatalogCmd`s run by `apply_command` in the same transaction,
retry, `APPLY_WAIT` and panic rules as every command. A credential-only
change still publishes no snapshot (no generation, no `wait_newer` wake-up);
`SetVerifier` and `RemovePeer` still change both in one transaction.
`Catalog::{enroll, enroll_paired, enroll_at_generation, revoke, forget,
credentials}` stay as shorthands over the contract.

**The adapter** (`Catalog::of`, the registry of open catalogs by handle
address) is **not removed**: its callers are in `src/orchestrator/inbound.rs`
(owned by no Wave 5 package), `src/orchestrator/peer.rs` (WP5.3's) and
`tests/security.rs` (owned by no Wave 5 package), which this package may not
edit (§13 rule 1). It is now a thin, documented leftover; CCR-WP5.4-1 moves
those callers to the contract and deletes it.

### 4. `ctx.rs`: `plain_path` strips the real verbatim prefix

`plain_path` looked for `\?\` (one backslash), which no path starts with,
so a daemon whose data directory came through `Config::load` (a relative
`data_directory` is joined to the canonical, `\\?\`-prefixed configuration
directory) installed and published its log path as `\\?\C:\…`. It now
works on path components: `\\?\C:\…` becomes `C:\…`, `\\?\UNC\host\share\…`
becomes `\\host\share\…`; every other path is kept as it is, including the
verbatim forms that have no plain spelling (`\\?\Volume{…}\…`, a bare
`\\?\C:`, which would become drive-relative) and device paths (`\\.\…`). A
name that is not valid Unicode survives (the old version went through
`to_string_lossy`). Outside Windows a backslash is an ordinary character
and the path is returned unchanged.

### 5. Other WP4.INT review minors

The WP4.INT notes' open items were checked against the files no other Wave
5 package owns (the Owned lists of WP5.1, WP5.2, WP5.3 and WP5.INT). Fixed
here:

- `plain_path` (item 4).
- `tests/logging.rs` (`every_daemon_installs_its_log_and_reports_it`, fix
  round 1): the check that the published log path has no verbatim prefix
  repeated the one-backslash prefix (`!reported.starts_with(r"\?\")`,
  always true). It now names `r"\\?\"`. The harness's node names its data
  directory relatively (`data_directory='data'`, `tests/support/node.rs`),
  so the daemon's data directory is joined to the canonical configuration
  directory and the check covers the `plain_path` fix end to end: with the
  base commit's `ctx.rs` restored for one run, it fails ("a path as the
  user knows it: `\\?\C:\Users\…\data\logs\orchestrator.log`"); with this
  package's, it passes.
- `docs/CLI.md` (the worker `status` row, fix round 1): it said the
  top-level `caught_up` and `metadata_caught_up` "stay `false`" on the v3
  path (WP4.INT's adapter for CCR-WP4.INT-1). It now states the rule of
  item 2: the local conditions, every peer exchanged with since the worker
  started caught up and acknowledging every shared event, a feed peer
  counting while its last exchange said so and is younger than three of its
  exchange intervals (15 s to 180 s); a peer that stops exchanging for good
  keeps them `false` until a restart; the four `upload_state` values.

The first round read "files that no other Wave 5 package owns" as this
package's Owned list and filed the last two as CCRs (CCR-WP5.4-3 and the
`CLI.md` part of CCR-WP5.4-2). The review pointed out that no Wave 5
package owns either file, which the Owned lists confirm, so task 5 covers
them; both are applied here and those CCRs are withdrawn (below).

Not moved under the same reading: the `Catalog::of` callers in
`src/orchestrator/inbound.rs` and `tests/security.rs` (no Wave 5 package
owns them either). The adapter is task 4's (CCR-WP4.1-1), not a WP4.INT
review minor, and its third caller, `src/orchestrator/peer.rs`, is WP5.3's,
so moving two of the three would leave the adapter in place and split one
change between this package and the integrator. CCR-WP5.4-1 keeps it in one
place, applied after WP5.3 merges.

The rest of the WP4.INT notes' open items are design limits or belong to
other packages: the `backfill_journal_branch` resume bug is WP5.1's (task
6); the off-lock save is WP5.2's (CCR-WP4.INT-5); the dialer-side
`peer.revoke` fallback and the enrollment retry without tests of their own,
and the enrollment retries sharing the peer-credentials thread, are in
`peer.rs` (WP5.3); a daemon that cannot remove a stale `orchestrator.json`
fails closed (`api.rs`), the in-place `worker.log` rotation can lose a line
(`maintenance.rs`), credential rows grow with the installation ids a code
holder mints, and an active attacker can force the code scheme: WP4.INT
recorded these as *Known limitations* (observations of its reviews, not
findings with a fix), so none is an open review minor.

## Deviations from the spec, and why

1. **A new test file**, `tests/worker_feed_status.rs`, not listed in the
   package's owned files. The status cell has no test file of its own among
   this package's files (its tests live in `worker_concurrency`, WP5.2's),
   and §13 rule 4 has each package add its own test files. No other file
   is touched.
2. **`Catalog::of` stays** (task 4 says to remove the adapter): its callers
   are in files this package does not own (above). The contract change is
   complete, the actor runs the credential commands through it, and
   CCR-WP5.4-1 gives the integrator the exact change.
3. **`upload_state` counts the feed peers too** (CCR-WP4.INT-1 names
   `caught_up` and `metadata_caught_up`): it had the same defect
   (`peer-not-configured` on every v3 folder) from the same legacy-only peer
   set, in the same function.
4. **The freshness rule measures the cadence** rather than taking the sync
   interval as a parameter: the interval is the orchestrator's
   configuration, and nothing in this package's files carries it to the
   worker (the launch spec, the IPC and `WorkerCtx` are other packages' or
   frozen). The measured rule needs no contract change and follows a daemon
   whose interval changes.

## CCRs filed

- **CCR-WP5.4-1** (`src/orchestrator/inbound.rs`, `src/orchestrator/peer.rs`,
  `tests/security.rs`, then `src/orchestrator/catalog.rs`; for WP5.INT, after
  merging WP5.3): move the last callers of `Catalog::of` to the catalog
  contract and remove the adapter.
  - `inbound.rs` (`CatalogAcceptPolicy`, `Inner`): hold no
    `Option<Arc<Catalog>>`; `peer_key` reads
    `ctx.catalog.credentials()` (`None`: refuse `PEER_REVOKED` "keeps no
    per-peer credentials", as today); `peer.enroll` and `peer.revoke` check
    `ctx.catalog.credentials().is_some()` (else `unknown_op`, as today) and
    `apply` `CatalogCmd::EnrollCredential { …, paired_only: false,
    generation: Some(peer.verifier_generation) }` /
    `CatalogCmd::RevokeCredential { installation, side: Inbound }`.
  - `peer.rs` (`PeerManager`, `CredentialWriter`, `run_credential_job`):
    the same mapping: `enroll` → `EnrollCredential { paired_only: false,
    generation: None }`, `enroll_paired` → `paired_only: true`, `forget` →
    `ForgetCredential`, `credentials().active(…, Outbound)` →
    `ctx.catalog.credentials().and_then(|c| c.active(…, Outbound))`.
  - `tests/security.rs:1076`: `Catalog::of(&api)` → `api.credentials().is_some()`.
  - Then delete `Catalog::of`, `CATALOGS` and `catalog_key` from
    `catalog.rs` (the registration in `Catalog::open` with them).
  - Reason: CCR-WP4.1-1 (task 4). Adapter until then: `Catalog::of` itself,
    unchanged in behaviour.
- **CCR-WP5.4-2** (docs; for WP5.INT): REVAMP_PLAN §14 WP0.4's
  `CatalogApi`/`CatalogCmd` listing and §3.2 gain the credential commands
  and `credentials()` (ratify CCR-WP4.1-1). Reason: the plan is not this
  package's. Adapter: none needed (docs). The first round's `docs/CLI.md`
  part (the worker `status` row) is **withdrawn**: applied by this package
  in fix round 1 (item 5; no Wave 5 package owns `CLI.md`).
- **CCR-WP5.4-3** (`tests/logging.rs`): **withdrawn**, applied by this
  package in fix round 1 (item 5; no Wave 5 package owns the file).
- **CCR-WP5.4-4** (`src/runtime/server.rs` or `src/runtime/control.rs`; no
  Wave 5 package owns them; for WP5.INT or the next wave; found by the fix
  round 1 review): the worker op `folder-details` embeds
  `Engine::control`'s in-process `status`, which still uses the legacy-only
  rule (`last_sync`, `sync_peers`, `peer_sync`): on a v3 folder its
  `status.caught_up` and `status.metadata_caught_up` stay `false` and its
  `status.upload_state` says `peer-not-configured`, while the worker
  `status` op (`status_cell::reply`) says `true` / `peer-acknowledged`.
  Requested: in `RpcServer::route`, a `"folder-details"` arm that takes
  `Engine::control`'s reply (the engine guard released at the end of that
  statement, so no lock is held across the next call), then sets its
  `status.caught_up`, `status.metadata_caught_up` and `status.upload_state`
  from `self.status()` (`status_cell::reply`, which takes no engine lock;
  invariant 30); or drops those three members from `folder-details`.
  `Engine::control`'s own `status` op keeps the legacy rule: the engine has
  no feed observations (`ctx.sync_obs` is on `WorkerCtx`), and in-process
  callers without a `WorkerCtx` use it
  (`w2_integration::in_process_caught_up_ignores_quarantined_rows`).
  Reason: CCR-WP4.INT-1 named the `status` reply; this is its second copy,
  and the route is not in this package's files (nor a WP4.INT review minor,
  so task 5 does not cover it). Adapter: none on this side (the route never
  reaches the status cell); the GUI reads only the conflict members of
  `folder-details`, and scripts read `status` (*Known limitations*).
- **CCR-WP5.4-5** (optional; the orchestrator's unpair and route changes,
  `src/runtime/sync_rpc.rs`; for a later wave): a feed peer's counters stay
  in `ctx.sync_obs` for the worker's life (`sync_rpc::observe` only
  inserts), so a peer that stops exchanging for good (unpaired while other
  peers remain, or its folder recreated under a new replica id) keeps
  `caught_up` `false` until the worker restarts. Requested: when the
  orchestrator stops routing a replica to a folder, tell the worker (a feed
  op, or the route set with the next exchange) and remove that replica's
  counters, so `status_cell` (which already rebuilds its peer set from
  `ctx.sync_obs` at every reply) stops counting it. Dropping peers by age
  alone would be wrong: a peer that is offline is a peer this folder is not
  caught up with. Reason: the route information is not in this package's
  files. Adapter: none; the legacy rule had the same limit (`sync_peers` is
  insert-only, `src/paired_sync.rs`), and *Known limitations* and
  `docs/CLI.md` say so.

## Existing assertions changed

| Test | Change | Why |
|---|---|---|
| `meta_scheduler::a_request_id_is_never_sent_twice` | `assert!(lane.key("repo-a").failures >= 1)` read at once → waits up to 5 s for the view to show `failures >= 1` | task 1: the counter is published after the event the test waited for (item 1); the claim is unchanged |
| `logging::every_daemon_installs_its_log_and_reports_it` (fix round 1) | `!reported.starts_with(r"\?\")` → `!reported.starts_with(r"\\?\")` | task 5 (a WP4.INT review minor; no Wave 5 package owns the file): the old check named a prefix no path has and always held. Stronger, not weaker: it fails with the base commit's `plain_path` (item 5) and passes with this package's |

## Tests added

| Test | Covers |
|---|---|
| `worker_feed_status::feed_peers_make_both_workers_caught_up` | two workers synced only through the feed (`feed-offer` → `feed-exchange` → `feed-apply`): both the dialer and the acceptor reach `caught_up` and `metadata_caught_up` with `upload_state: peer-acknowledged`; before any exchange `peer-not-configured`; a new local event is `queued` and not caught up until acknowledged. Fails on the old cell ("not caught up after 10 rounds", `upload_state: peer-not-configured`) |
| `worker_feed_status::a_feed_observation_counts_for_three_exchange_intervals` | the reply's freshness rule: an unknown cadence holds 15 s; exchanges every 60 s count for 180 s (160 s old caught up, 220 s not); a burst does not shorten it; a peer whose last exchange said it was behind is not caught up; every feed peer counts; a 200 s gap is an outage |
| `worker_feed_status::the_cadence_is_the_longest_recent_gap_short_of_an_outage` | `FeedCadence` and `feed_peers_caught_up` as pure functions: floor and ceiling, 500 ms / 20 s / 60 s cadences, older and repeated stamps, bursts, a mutual pairing's interleaved schedules, outages, a gap of exactly `FEED_CADENCE_MAX`, a shorter interval taking over, the window boundaries |
| `catalog_actor::credentials_are_read_and_changed_through_the_catalog_contract` | every credential operation through `dyn CatalogApi`: enroll both sides (no generation, no wake-up, the same view as `Catalog::credentials`), the three refusals storing nothing, both conditions holding, revoke and forget, a rotation revoking inbound only, an unpair forgetting outbound, a reopen keeping them; `Debug` never prints a key |
| `catalog_actor::a_catalog_without_credentials_answers_none` | the trait's default: a fake answers `None`, `Catalog::of` finds nothing |
| `orchestrator::ctx::tests::plain_path_strips_the_verbatim_prefix_of_drive_and_unc_paths` (unit, Windows) | drive and UNC verbatim paths, paths without a prefix, `\\?\Volume{…}`, a bare `\\?\C:`, `\\.\pipe`; a canonical temporary directory joined like `Config::load` does. Fails on the old function |
| `orchestrator::ctx::tests::plain_path_keeps_names_that_are_not_unicode` (unit, Windows) | an unpaired surrogate survives. Fails on the old function |
| `orchestrator::ctx::tests::plain_path_is_the_path_outside_windows` (unit, not Windows) | identity |

## Fix round 1

The review approved the package with three minors; each is addressed.

| Finding | Response |
|---|---|
| 1. Task 5's "files that no other Wave 5 package owns" covers `docs/CLI.md` and `tests/logging.rs`, filed as CCRs instead of fixed | Accepted: the Owned lists of WP5.1, WP5.2, WP5.3 and WP5.INT name neither file. Both fixed here (item 5); CCR-WP5.4-3 and the `CLI.md` part of CCR-WP5.4-2 are withdrawn. The logging check was shown to fail with the base `plain_path` and pass with this package's. The `Catalog::of` callers in `inbound.rs` and `tests/security.rs` stay as they are (item 5: task 4's adapter, not a WP4.INT minor, and `peer.rs` still blocks removing it), as the review noted deviation 2 still holds |
| 2. `folder-details` embeds `Engine::control`'s `status`, a second copy of the claim that still uses the legacy-only rule | Accepted and confirmed by reading (`control.rs` `"folder-details"` calls its own `"status"`; `server.rs` routes it to `Engine::control`, `ops::STATUS` to `status_cell::reply`). Filed as CCR-WP5.4-4 and recorded under *Known limitations*: the route is in `server.rs`/`control.rs`, which are not this package's, and this is not a WP4.INT review minor, so task 5 does not cover it; no adapter on this side can reach that reply |
| 3. Feed peers are never removed from `ctx.sync_obs`, so a peer that stops exchanging for good keeps `caught_up` false until a restart | Accepted (`sync_rpc::observe` only inserts; the legacy `sync_peers` is insert-only too, `paired_sync.rs:274`). Recorded under *Known limitations* and in `docs/CLI.md`; the route-based fix is filed as the optional CCR-WP5.4-5. Ignoring old peers by age alone was rejected: a peer that is offline is one the folder is not caught up with |

This round changes `docs/CLI.md`, `tests/logging.rs` and these notes; no
file under `src/` changed.

## Fix round 2

The second review approved the package with one minor; it is addressed.

| Finding | Response |
|---|---|
| 1. `src/orchestrator/ctx.rs` `install_log`'s doc comment still spells the verbatim prefix with one backslash (`` `\?\` ``), the misspelling this package fixes in `plain_path` | Accepted: the comment now reads `` `\\?\` ``. A fixed-string search for `` `\?\` `` in `src/` and `tests/` now finds only the two intentional historical notes (`ctx.rs` on `plain_path`'s WP4.INT version, and `tests/logging.rs` on the old check) |

This round changes one doc comment in `src/orchestrator/ctx.rs` (no code)
and these notes.

## VERIFY

### Fix round 2

Run in `C:/Users/felyp/Desktop/Projetos/Telekinesis-wt/wp5-4`,
`CARGO_TARGET_DIR=C:/tkfs-target/wp5-4` (Windows 11), on the fix round's
tree (the doc-comment change plus these notes).

| Command | Result |
|---|---|
| `cargo fmt --check` | clean (exit 0) |
| `cargo build --all-targets` | ok (exit 0) |
| `cargo clippy --all-targets -- -D warnings` | clean (exit 0) |
| `powershell -NoProfile -File C:/tkfs-build/suite-lock.ps1 cargo test --all-targets --no-fail-fast` | SUITE_RESULT |
| WSL: `cargo check --all-targets` (`CARGO_TARGET_DIR=$HOME/tkfs-target/wp5-4`) | ok (exit 0) |
| WSL: `cargo test --no-fail-fast --lib --test logging` | **2 binaries: 90 passed, 0 failed, 2 ignored** (exit 0): lib 72 + 2 ignored, `logging` 18 |

### Fix round 1

Run in `C:/Users/felyp/Desktop/Projetos/Telekinesis-wt/wp5-4`,
`CARGO_TARGET_DIR=C:/tkfs-target/wp5-4` (Windows 11), on the fix round's
tree (`fe279a7`; the notes commit after it changes this file only).

| Command | Result |
|---|---|
| `cargo fmt --check` | clean (exit 0) |
| `cargo build --all-targets` | ok (exit 0) |
| `cargo clippy --all-targets -- -D warnings` | clean (exit 0) |
| `powershell -NoProfile -File C:/tkfs-build/suite-lock.ps1 cargo test --all-targets --no-fail-fast` | **ok, exit 0: 75 test binaries, 1,091 passed, 0 failed, 124 ignored** (the same count as the first round: the round changes an assertion, adds no test); `logging` 21 passed, `every_daemon_installs_its_log_and_reports_it` among them |
| WSL: `cargo check --all-targets` (`CARGO_TARGET_DIR=$HOME/tkfs-target/wp5-4`) | ok (exit 0) |
| WSL: `cargo test --no-fail-fast --lib --test logging --test catalog_actor --test worker_feed_status --test meta_scheduler` | **5 binaries: 135 passed, 0 failed, 3 ignored** (exit 0): lib 72 + 2 ignored, `logging` 18, `catalog_actor` 21, `meta_scheduler` 21 + 1 ignored, `worker_feed_status` 3 |
| `cargo test --test logging every_daemon_installs_its_log_and_reports_it`, with the base commit's `src/orchestrator/ctx.rs` put back for that run only | **fails** as expected ("a path as the user knows it: `\\?\C:\Users\…\data\logs\orchestrator.log`"); with this package's `ctx.rs`, passes |

### First round

Run in `C:/Users/felyp/Desktop/Projetos/Telekinesis-wt/wp5-4`,
`CARGO_TARGET_DIR=C:/tkfs-target/wp5-4` (Windows 11), at `4ca96f3`, the last
source commit (the notes commit after it changes this file only).

| Command | Result |
|---|---|
| `cargo fmt --check` | clean (exit 0) |
| `cargo build --all-targets` | ok (exit 0) |
| `cargo clippy --all-targets -- -D warnings` | clean (exit 0) |
| `powershell -NoProfile -File C:/tkfs-build/suite-lock.ps1 cargo test --all-targets --no-fail-fast` | **ok, exit 0: 75 test binaries, 1,091 passed, 0 failed, 124 ignored** (run 3, below) |
| WSL: `cargo check --all-targets` (`CARGO_TARGET_DIR=$HOME/tkfs-target/wp5-4`) | ok (exit 0) |
| WSL: `cargo test --no-fail-fast --lib --test catalog_actor --test worker_feed_status --test meta_scheduler --test security --test worker_concurrency --test feed_v7 --test orchestrator --test inbound_v3 --test folder_controller --test management_api --test contracts --test worker_lifecycle --test peer_supervisor` | **14 binaries: 435 passed, 0 failed, 3 ignored** (exit 0) |

The suite ran three times on the same code, on the shared machine beside
other packages' release qualification runs and builds:

- **Run 3** (the result above): every binary passed.
- **Run 1** (during the flaky-test loops above, with other packages' release
  runs on the machine): 75 binaries, 1,090 passed, **1 failed**, 124
  ignored. The failure, `worker_concurrency::a_slow_commit_never_waits_for_another_writer_under_the_engine_lock`
  ("the engine was held 9.2463634s while another connection held the
  writer"; its binary took 29 s against 16 s alone), is WP5.2's file and
  code this package does not touch; alone it passed 5 of 5, and its binary
  3 of 3 (29 passed each).
- **Run 2**: 75 binaries, 1,090 passed, **1 failed**, 124 ignored.
  `objects_v3_lanes::an_object_of_unknown_size_pushed_in_part_is_handed_on`
  ("C's lane did not end with its session", a 20 s wait; its binary took
  31 s against 20 s alone), WP5.3's file, the wait WP4.INT already
  lengthened once; alone it passed 3 of 3, and its binary 1 of 1 (48
  passed). `worker_concurrency` passed in this run.

The count matches the base: WP4.INT's last suite was 74 binaries, 1,084
passed, 124 ignored; this package adds 7 tests (2 in `catalog_actor`, 3 in
the new `worker_feed_status`, 2 Windows unit tests in `orchestrator::ctx`)
and one binary. Per binary (Windows): `catalog_actor` 21, `meta_scheduler`
21 + 1 ignored, `worker_feed_status` 3, `security` 28, `worker_concurrency`
29, the library 68 + 2 ignored.

## Known limitations

- **`Catalog::of` remains** until CCR-WP5.4-1 is applied; the inbound server
  and the peer stack still reach the credentials through it (same
  behaviour as before).
- **The cadence is sampled.** A worker's stamps are sampled by its cell
  refreshes (maintenance about once a second) and `status` calls, so a gap
  is the real interval or longer. A worker without a maintenance thread
  (in-process tests) samples only at `status` calls; with calls further
  apart than 90 s, the gaps read as outages and the 15 s floor holds.
- **The window after a change of interval:** after a switch from a long
  sync interval to a short one, the longer window holds for up to two
  cadence windows (180 s); after the first exchange of a new worker, or a
  switch from a short interval to a long one, the 15 s floor holds until a
  gap of the new length has been seen (one interval).
- **The inventory match** is recomputed by a `status` call after any feed
  exchange while every cheaper condition holds, as it was after every
  legacy lane exchange (index-only counts, cached per connection while the
  database is unchanged).
- **Status fields only:** the hub, the GUI and `manage status` read the feed
  counters directly and were already right; this changes the worker's own
  `status` reply (CLI `tkfs status`, scripts).
- **`folder-details` still embeds the legacy claim** (fix round 1 review):
  its `status` member is `Engine::control`'s, so on a v3 folder its
  `caught_up` and `metadata_caught_up` stay `false` and its `upload_state`
  says `peer-not-configured`. The GUI reads only its conflict members;
  scripts and the `manage` passthrough should read the `status` op.
  CCR-WP5.4-4.
- **A feed peer is never forgotten** (fix round 1 review): its counters stay
  in `ctx.sync_obs` until the worker restarts, so a peer that stops
  exchanging for good (unpaired while other peers remain, or its folder
  recreated with a new replica id) keeps `caught_up` `false` until then.
  The legacy rule had the same limit (`sync_peers` is insert-only), so this
  is not a regression; `docs/CLI.md` says so. CCR-WP5.4-5 (optional).
