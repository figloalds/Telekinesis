# Validation

How this release is checked, how to run the checks, and what they measured.
Four layers:

1. the **test suite** (`cargo test`): unit, integration and multi-daemon tests,
   about a thousand tests on Windows, a subset on Linux;
2. the **qualification scenarios** (`scripts/qualify.ps1`, `scripts/qualify.sh`):
   release-build scenarios with performance and behaviour floors, recorded
   as evidence JSON;
3. the **desktop manual checklist** (below), for what the window itself does;
4. the **soak** and the **two-machine lab**, run for the final integration.

## The test suite

```powershell
$env:CARGO_TARGET_DIR = "C:\tkfs-target\main"
cargo fmt --check
cargo build --all-targets
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
```

- Windows needs the WinFsp SDK (`WINFSP_DIR`) and builds the Slint desktop.
  On a shared machine, run the full suite through
  `scripts\suite-lock.ps1 cargo test --all-targets` so only one full suite
  runs at a time (the multi-daemon tests are timing-sensitive under load).
- Linux (or WSL) runs the same targets; FUSE tests skip without `/dev/fuse`.
  The subset that matters most there:
  `cargo test --test orchestrator --test session_core --test session_handshake --test worker_lifecycle --test mount_lifecycle`.
- Long scenarios are `#[ignore]`d in the normal suite; they are the
  qualification scenarios below and the release runs of the fuzzers:
  `cargo test --release --test derive_oracle -- --ignored`
  (`TKFS_DERIVE_SEEDS`, `TKFS_DERIVE_SEED_START`; the stress run
  `fuzz_stress_release`), `cargo test --release --test feed_fuzz -- --ignored`
  (`TKFS_FEED_SEEDS`, `TKFS_FEED_SEED_START`), and the crash workloads
  `cargo test --release --test seal_path -- --ignored`.
- `tests/docs_cli.rs` checks that every command in [CLI](CLI.md) exists with
  the options it shows, and that every command of the binary is documented.

What the suite covers, by area (file names under `tests/`):

| Area | Tests |
|---|---|
| Store, activation, conflicts | `core`, `activation_inbox`, `derive_oracle` (randomized histories checked against the full reducer), `offlock_commit`, `startup_snapshots`, `recovery`, `directory_progress`, `availability_index` |
| Metadata feed | `feed_v7` (acks from evidence only, rejections, regressions, private branches, forged origins, large events), `feed_fuzz` (seeded random topologies of 2–4 replicas with lost requests and replies, partial activation, restarts and restores; the oracle after every step, convergence, acks and per-id evidence at the end) |
| Objects | `objects_v3_worker` (durability, crash resume, authorization, leases), `objects_v3_lanes` (both directions, backoffs, handoff, slow links), `partial_content`, `availability` |
| Sessions | `session_core` (framing, multiplexing, heartbeats, deadlines, WSS), `session_handshake` (HMAC, replay, legacy detection both ways) |
| Security | `security` (local tokens on the control and worker endpoints, per-peer credentials and their enrollment, revocation on unpair within 1 s, rotation, replay, no code, secret or token in status or logs) |
| Operations | `logging` (log rotation, redaction, the desktop's daemon log, `worker.log` rotation, diagnostics bundles without secrets) |
| Daemon | `catalog_actor` (including the 6298d0b catalog upgrade and downgrade), `folder_controller` (lifecycle, adoption, gate), `management_api`, `peer_supervisor`, `meta_scheduler`, `inbound_v3`, `status_hub`, `orchestrator` (real daemons over the network), `w1/w2/w3_integration` |
| Worker | `worker_lifecycle`, `worker_process`, `worker_concurrency`, `mount_lifecycle` (WinFsp and FUSE) |
| Desktop | `gui_model` (every chip, row and action from status snapshots), the binary's own window tests |
| Contracts | `contracts` (frozen wire shapes, error-code table), `harness_selftest`, `seam_stubs` |

## Qualification

The scenarios live in `tests/qual_*.rs`, run in release with
`--ignored --test-threads=1`, and print `QUAL {json}` records that
`scripts/qualify.ps1` compares with `tests/qualification-floors.json`.

```powershell
$env:CARGO_TARGET_DIR = "C:\tkfs-target\qual"
powershell -NoProfile -File scripts\qualify.ps1 -All                      # every scenario and floor
powershell -NoProfile -File scripts\qualify.ps1 -Wave 3                   # floors of waves 1-3
powershell -NoProfile -File scripts\qualify.ps1 -Wave 3 -Files qual_wp3_int_e2e -Merge
powershell -NoProfile -File scripts\qualify.ps1 -SelfTest                 # the runner itself
```

```sh
CARGO_TARGET_DIR=$HOME/tkfs-target/qual bash scripts/qualify.sh --wave 3   # Linux subset
```

| Option | Meaning |
|---|---|
| `-Wave N` / `-All` | Run the files of waves ≤ N (all) and enforce their floors. `-All` also raises the budgets the long scenarios need (`-BudgetS 2400`, `-FileTimeoutMin 150`) unless they are given, and writes `qualification-all.json`. |
| `-Files a,b` / `-Filter NAME` | Only these files / scenarios. |
| `-Merge` | Keep the other files' results from the existing evidence and replace only those run now. |
| `-BudgetS S` | Measured-phase budget per scenario (`TKFS_QUAL_BUDGET_S`, default 120). |
| `-FileTimeoutMin M` | Kill a file's run after M minutes (default 90). |
| `-Priority` | `Idle`, `BelowNormal` (default) or `Normal` for the test processes. |
| `-Baseline` | Record every floor without enforcing it (`qualification-baseline.json`). |
| `-NoBuild` | Reuse the release test binaries. |

- Evidence goes to `test-evidence/qualification-<kind>.json` (the git commit
  and dirty files at the start, toolchain, hardware, every file's exit code
  and result line, every scenario's metrics, every floor's verdict); logs to
  `test-runs/qualify-<kind>-<time>/`.
- A file passes only if cargo exits 0 and its last `test result:` line says
  `ok.`. A floor is `pass`, `fail`, `missing`, `incomplete` (its scenario
  timed out) or `not_run`. The script exits 0 only when every enforced floor
  holds and every selected scenario passed or was skipped.
- `TKFS_BASELINE_BIN` (a `tkfs` binary built from `6298d0b`) enables the
  scenario that upgrades one of two previous-release daemons and checks that
  both sides fail closed; without it that scenario is skipped.
- `TKFS_QUAL_SCALE` (default 1) scales scenario sizes down for quick dry runs;
  `TKFS_QUAL_ENFORCE=0` reports a scenario's floors without failing it.

### Floors and the latest measurements

Release build, Windows 11, AMD Ryzen 9 7950X3D (16 cores, 128 GB), a shared
development machine (no other build running at the start). Measured on
2026-10-17 by the final integration (`test-evidence/qualification-all.json`,
`qualify.ps1 -All -Priority Normal` at `27802c3`: 91 scenarios, 90 passed,
1 skipped — SIGTERM has no Windows equivalent — and 54 of 54 floors held;
scenario data on a SATA SSD, `TMP` on `F:`, as WP4.2's run). The baseline
column is the previous release (`6298d0b`); `test-evidence/qualification-final.json`
holds the §15 before/after table with the Linux numbers too.

| Floor | Scenario | Baseline | Floor | Measured |
|---|---|---|---|---|
| Remote linear create, per event, 2K / 10K | `qual_wp1_6_activation` | 15.9 s @2K | ≤ 5 ms | 0.63 / 0.90 ms |
| Local create after a resolved conflict (p90) | `qual_wp1_6_activation` | 4.2 s | ≤ 10 ms | 1.2 ms |
| Two-way backlog, per event, 2K / 6.4K | `qual_wp1_7_derive` | 15.9 s / 218 s | ≤ 100 / 300 ms | 35 / 132 ms |
| 50 concurrent namespace ops on 5K history (max) | `qual_wp1_7_derive` | seconds to minutes | ≤ 500 ms | 229 ms |
| Restart to Ready at 20K objects | `qual_wp1_8_objects` | full re-hash | ≤ 3 s | 0.95 s |
| `content_status` at 50K objects | `qual_wp1_8_objects` | 6.5 s | < 50 ms | 0.06 ms |
| 12K tiny pending objects complete | `qual_wp1_8_objects` | page cliff | completes | completes |
| Worker `watch` latency | `qual_wp1_4_worker` | no `watch` op | ≤ 1.5 s | 1.0 s |
| Fast call behind a slow one (p99) / control under 30 MiB queued | `qual_wp1_9_session` | head-of-line blocking | < 50 ms | 0.2 / 2.2 ms |
| Session loopback throughput | `qual_wp1_9_session` | JSON+hex request/response | ≥ 300 MiB/s | 2,732 MiB/s |
| Healthy repo beside a hung one (legacy lanes) | `qual_wp1_12_legacy` | lane-wide resets, 30 s backoff | ≤ 45 s | 0.7 s |
| Reconnects at `sync_interval_ms = 20000` over 60 s (legacy, v3) | `qual_wp1_12_legacy`, `qual_wp3_1_transport` | every idle cycle | 0 | 0 / 0 |
| Restarts of a crashing worker in 20 s | `qual_wp1_12_legacy` | every 2 s | ≤ 5 | 5 |
| Management API p99 under lifecycle churn (lifecycle / scale) | `qual_wp2_1_lifecycle`, `qual_scale` | 23–41 s | < 100 ms | 1.3 / 2.0 ms |
| Status subscription latency | `qual_wp2_1_lifecycle` | 5 s poll | ≤ 1 s | 26 ms |
| Shutdown with a busy folder | `qual_wp2_1_lifecycle` | aborts on the first busy folder | ≤ 30 s | 0.39 s |
| File-system op p99 during a 2K catch-up | `qual_wp2_3_worker` | seconds | < 50 ms | 12 ms |
| Status p99 during activation | `qual_wp2_3_worker` | blanked | < 20 ms | 7.5 ms |
| Ready with 5K staged events | `qual_wp2_3_worker` | unbounded startup drain | < 3 s | 0.18 s |
| Engine-lock hold for a slow event at 2K | `qual_wp2_4_offlock` | full replay | < 50 ms | 1.7 ms |
| 150K-event feed / 8 MiB event | `qual_wp2_5_feed` | `INVENTORY_TOO_LARGE` / stall | completes | completes |
| Idle exchange at 50K events (CPU per side / bytes) | `qual_wp2_5_feed` | 1.13 s / 251 KB @6.4K | ≤ 10 ms / 4 KiB | 0.8 ms / 1,621 B |
| Extra delay of a healthy repo beside a hung one (v3) | `qual_wp3_1_transport` | 30 s backoff | < 2 s | 1.0 ms |
| Worker to worker, 256 MiB object / 10K tiny objects | `qual_wp3_2_objects` | about 2 MiB/s / stop-and-wait pages | ≥ 100 MiB/s / ≥ 300/s | 120 MiB/s / 907/s |
| 256 MiB object between daemons on loopback / 1 GiB object | `qual_wp3_int_e2e`, `qual_scale` | 2.0 MiB/s | ≥ 40 MiB/s | 108 / 115 MiB/s |
| 1 GiB through a 100 Mbit/s, 50 ms RTT proxy | `qual_wp3_int_e2e` | not measured | ≥ 80 % of the link | 97.9 % |
| 10K × 4 KiB files between daemons (e2e / scale) | `qual_wp3_int_e2e`, `qual_scale` | page cliff | ≥ 300 files/s | 771 / 770 files/s |
| 10K-event catch-up between daemons | `qual_wp3_int_e2e` | hours | ≤ 60 s | 10.8 s |
| Objects before activation | `qual_wp3_int_e2e` | 0-byte bulk rounds | ≥ 1 | 54 |
| Mutual pairing, duplicate bytes | `qual_wp3_int_e2e` | up to 2× | ≤ 1.1× | 1.002× |
| Two-way backlog between daemons | `qual_wp3_int_e2e` | slow path | ≤ 180 s | 3.7 s |
| Idle exchange between daemons | `qual_wp3_int_e2e` | 251 KB @6.4K | ≤ 4 KiB | 1,913 B |
| 150K-event feed / 8 MiB event between daemons | `qual_wp3_int_e2e` | stall | completes | completes |
| Remote linear activation 2K / 6.4K, two-way backlog (daemons) | `qual_scale` | 15.9 s / 218 s | ≤ 5 / 5 / 100 ms | 0.86 / 1.04 / 1.9 ms |
| Startup to Ready at 50K objects | `qual_scale` | minutes | < 10 s | 2.6 s |
| Idle worker CPU (5K events, 10K objects) | `qual_scale` | 4 sweeps + 2 parses per 500 ms per lane × repo | ≤ 1 % | 0.52 % |
| Bytes written by seals in a 150K catch-up / longest seal | `qual_wp4_4_seal` | 14 GB / 2.9 s per seal | ≥ 10× less / < 50 ms | 72× / 8.6 ms |
| Seal after a 150K-commit gap on another branch | `qual_wp4_4_seal` | 355 ms | < 50 ms | 0.30 ms |
| Restart at 50K entries, rows against inline layout | `qual_wp4_4_seal` | 1.0 | ≤ 1.15 | 1.006 |

The Linux subset (`qualify.sh --all` in WSL 2, Ubuntu, kernel 6.18,
scenario data on the image's ext4, `test-evidence/qualification-all-linux.json`,
at `2a7d96b` with `qual_wp1_9_session` re-run at `2e62858`): 92 scenarios,
91 passed, 1 skipped (the 6298d0b upgrade needs a Linux baseline binary);
52 of 54 floors held, every enforced one. The two misses are the
Windows-only tiny-file floors (219 and 196 files/s; WSL's ext4 image runs a
bare create/write/fsync/rename loop at about 140/s), recorded and not
enforced there. Linux measured, among others: idle worker CPU 0.37 %,
startup at 50K objects 2.2 s, the 10K catch-up 12.9 s, the longest seal
16.9 ms, 1 GiB through the 50 ms proxy at 97.9 % of the link.

Floors bound by fsync latency (tiny files, the 256 MiB worker transfer, the
catch-up) and by single slow operations (`fs_p99_during_catchup`,
`concurrent_namespace_ops_5k`) depend on the disk and miss on a busy
machine: a preliminary `-All` run of this integration with 25 release fuzz
and crash processes running beside it measured 28 tiny objects/s, 263 files/s,
a 72 s catch-up and a 74 ms FS p99 (five floors missed); the evidence above
is the run on a quiet machine. On the NVMe system disk the tiny-file
floors measured 310–315/s at Wave 3 and 180–280/s in WP4.5's runs, made
while that disk was full (0–11 GB free); WP4.2's and this run kept scenario
data on the SATA SSD (`F:`), where they hold with margin.

### Interoperability and upgrade checks

- `qual_wp3_1_transport::upgrade_against_a_6298d0b_peer_fails_closed_on_both_sides`
  (needs `TKFS_BASELINE_BIN`): two previous-release daemons paired both ways,
  one upgraded in place: the upgraded dialer shows the peer `incompatible`
  with `PEER_NEEDS_UPGRADE` within milliseconds and never offers the code; the
  old side's lanes report `PEER_NEEDS_UPGRADE` within a second; the upgraded
  side lists the old side's attempts.
- `qual_wp3_1_transport::v2_peer_fails_closed`, `qual_wp3_int_e2e::v2_peer_fails_closed`:
  both directions against the legacy client.
- `qual_wp3_int_e2e::legacy_fixture_upgrade_then_sync`: a data directory
  written by the previous release (`tests/fixtures/legacy/datadir`) is opened
  by the new daemon and its folder syncs to a new host and back.
- `catalog_actor` opens the previous release's catalog, checks every folder,
  gate, peer and the verifier survive, and runs a previous-release daemon on
  the migrated catalog.

## Desktop manual checklist

The window's flows are tested through the view model (`gui_model`) and the
command pool; these checks cover the window itself. Run them on Windows with
two hosts (or two data directories on one machine).

1. Start `tkfs gui` with no daemon running: the banner says it is starting the
   daemon and names `DATA/logs/orchestrator.log`; the folders appear.
2. Create a folder (**New folder**): the row shows `Starting…` (busy), then
   `Running`, `Mounted`, `Not shared with any peer`.
3. Pair with a second host (**Pair with a host**, with the code from its
   **Settings**), browse and receive a folder: the received row shows
   `Initial sync from <host>` and `Waiting for initial sync [Mount now]`, then
   `Up to date` and `Mounted`.
4. Copy a large file into one side: the other side's row shows
   `Downloading x of y · rate · ETA` with a progress bar, then `Up to date`.
5. Stop the other host's daemon: the row shows `Waiting for <host> (offline N
   min)`; the sidebar badge `Offline N min`.
6. Create a folder whose mount path already exists: `Mount failed [Retry]`
   with `[needs action] MOUNT_PATH_MUST_NOT_EXIST` and its suggestion.
7. Open a file in a mounted folder and stop it: `Stopping — 1 file open
   [Force stop]`; Force stop asks for confirmation.
8. Pause sync: `Paused [Resume]`.
9. Generate a new pairing code in Settings: it is shown once and cleared when
   Settings closes; the other host's badge becomes `Unauthorized — re-pair`.
   Then generate another code and leave Settings through **Shut down the
   daemon** (both Cancel, and Shut down followed by **Start the daemon**):
   reopening Settings shows no code.
10. Pair with a host running the previous release: its badge reads
    `Peer needs upgrade`.
11. Shut the daemon down from Settings: the banner offers `Start the daemon`;
    nothing restarts it until clicked. `tkfs manage shutdown` from a terminal
    has the same effect on an open window. Then start the daemon from a
    terminal (`tkfs orchestrator`): the window reconnects; end that daemon's
    process: the window starts a new one.
12. While a Stop is in flight (a folder with open files), Start another
    folder: it starts at once.
13. Screenshots for documentation are rendered without a window:

    ```powershell
    $env:TKFS_GUI_RENDER = "out.bmp"
    $env:TKFS_GUI_RENDER_VIEW = "folder:<state>:sync"    # or folders, activity, peer:<installation>
    tkfs gui
    ```

    `TKFS_GUI_RENDER_SNAPSHOT=status.json` renders a saved
    `manage status --json` snapshot instead of the running daemon's;
    `TKFS_GUI_RENDER_SIZE=WxH` sets the size.

## Soak

The final integration runs a two-hour soak: two daemons connected through a
fault-injecting proxy (`tests/support/proxy.rs`, `FaultProxy`: latency,
bandwidth limits, stalls, connection resets) whose faults change
periodically, with continuous edits on both sides. It passes when, after the
edits stop and the link is healthy:

- both folders read `Up to date` (no `stalled`, `blocked` or `needs attention`
  state, no open errors other than resolved ones);
- both replicas hold the same shared events (`metadata caught up` both ways,
  and `accepted_metadata_equal` from `scripts/inspect_metadata.py --compare`
  of the two states);
- every object is available on both sides (`content complete`) and every event
  is acknowledged (`journal.outgoing_unacknowledged` = 0);
- the derived-state oracle (`Store::verify_derived_state`) finds no difference
  on either store, checked on the stopped folders.

The proxy and the oracle are test-support and library APIs; there is no
command-line soak tool.

## Two-machine lab

`test-evidence/lab/` holds the two-machine lab run of 2026-10-16: the
previous release (`6298d0b`) against the Wave 4 base of this one
(`e8c2979`, before the Wave 4 packages merged), on two Windows machines (a
laptop as the source and listener, the 16-core desktop as the dialer and
receiver) whose peer traffic crossed an `ssh -L` tunnel over Wi-Fi (about
7 MB/s each way, 2-3 ms). The harness (`lab_e2e.py` 1.4, `mounted_check.py`
1.1) is not part of this repository; each `results-*.json` records its
parameters, the binaries' hashes, the link probes and the machines' load,
and `results-comparison.md` puts the runs side by side. Every cell is one
run (n = 1), the desktop was shared with other builds, and the link varied
between runs: compare the builds with each other, not with the probe.

| Scenario | previous, 200 files | this, 200 files | previous, 2,000 files | this, 2,000 files |
|---|---|---|---|---|
| S1 receive: every path and hash visible | 161.6 s | 2.0 s | not reached in 30 min | 6.8 s |
| S1 receive: all content (543-603 MB) | 188.5 s (2.9 MB/s) | 75.3 s (7.2 MB/s, the tunnel's ceiling) | 478 of 2,001 files at 30 min | 84.2 s (7.2 MB/s) |
| S2 two-way edits (200 each way) converged | 1031.6 s | 52.5 s | 0 of 200 and 7 of 200 at 30 min | 120.8 s |
| S3 45 s outage during a 512 MiB transfer: restore to complete | 215.3 s | 152.0 s | transfer never started (backlog) | 156.4 s |
| S3 restore to transfer resumed | 28.0 s | 85.8 s | — | 88.5 s |
| Worker `status` calls answered busy or stale (S1) | 63 of 95 | 0 of 38 | 454 of 892 | 0 of 45 |

Every completed transfer resumed from its durable offset and verified. Two
results are worse in the measured build: after the outage the lanes
reconnected in 36-37 s against 28 s (the dial backoff grows during the 45 s
outage, 1 s doubling up to 60 s) and the transfer resumed about 50 s after
that. The second wait is consistent with the base's lease rules: the pull's lease
from before the outage lapsed (60 s without progress) and the lapse backed
the object off for its only source for another 60 s. WP4.5, merged after
this run, releases the leases of a round whose session was lost (no lapse,
no stall backoff); the lab was not run again on the final build, so that
gain is not measured here. The laptop's ingest of 2,000 files into its
WinFsp mount took 163.6 s against 102.6 s, measured once on a laptop shared
with Windows servicing.

The mounted check (this build only) received the 201 files into a WinFsp
mount: the gate opened when the metadata was complete and the mount
attached 2.1 s after `receive`, while the 512 MiB file was still
downloading. All 201 files were listed with the right sizes and 200 hashed
correctly through the mount; reading the large file failed with "invalid
argument" (`STATUS_FILE_IS_OFFLINE`: a read waits up to 15 s for content
that is still downloading, then fails; README, *Initial-sync gate*). A file
written through the mount reached the laptop and hashed correctly there,
and the folder stopped cleanly with its mount point removed.

## Earlier validation records

The validation records of the previous release (October 2026: the mount
failure that blocked a server's sync, the parallel object lanes, the
startup-snapshot benchmark, the Visual Studio directory-cleanup stall) are in
the git history of this file. Their findings fed the root-cause analysis in
`docs/revamp/ROOT_CAUSES.md`; their measurements describe the previous
release, not this one.
