# tkfs two-machine lab: baseline 6298d0b vs revamp e8c2979

Every number below comes from the result files `results-{baseline-small,revamp-small,baseline-full,revamp-full,revamp-mounted}.json` (tables by `make_comparison.py`; this page by `scripts/assemble_comparison.py`).

Audit note (2026-10-16): four cells/sentences were corrected by hand after generation (headline baseline-full S1 content cell, probe-ceiling caveat, mounted-check timing reference, S3 baseline-small kill-time cell); re-running `scripts/assemble_comparison.py` would revert them.

## Setup

- Harness `lab_e2e.py` v1.4 for all four runs, unchanged (the revamp smoke run `runs/smoke-revamp-results.json` needed no harness change). Every run used `--fresh`; small = `--small-count 200`; all other parameters at their defaults (30 min cap per scenario, 512 MiB S1/S3 files, 200 S2 files, link ceiling measured).
- Run order, back to back (`scripts/run_matrix.sh`, UTC ms starts): baseline-small 1792163059977, revamp-small 1792164883757, baseline-full 1792165528803, revamp-full 1792170265114; then `mounted_check.py` v1.1 (1792171158011).
- Revamp e8c2979: laptop `git rev-parse HEAD` in `C:\tkfs-lab\src-revamp` = e8c297962cea6047e1618968aa61b79ad555f79f; sha256 laptop cd6687e09cfe365cc35f9820fa732dd66b64c338c02109e8e966a8975510c604, desktop 424e37612df4247c40ad738f5e6f2a54c9dd18b8340dfd508a78c2a58ed0fbff. The desktop binary is the previous run's build (its source worktree no longer exists; see runs/build-logs/build-record.json).
- Baseline 6298d0b: sha256 laptop 274b7de7e12e67b15c31b1559026857d9067f17b211c074cdb94834a35c3872c, desktop 69c6e8f8414f85ed5e760303786152a8b7ee431258e3e17d16bff78f3dc5c2a9.
- Laptop = source and WebSocket listener; desktop = dialer and receiver; peer traffic goes through an `ssh -L` tunnel over Wi-Fi.

## Headline (before / after)

| Metric | baseline-small | revamp-small | baseline-full | revamp-full |
|---|---|---|---|---|
| S1 all paths+hashes visible (s after receive) | 161.6 s | 2.0 s | not reached (cap) | 6.8 s |
| S1 all content (s after receive) | 188.5 s | 75.3 s | cap: 478/2001 files matching in projection, 553.1/603.3 MB objects received | 84.2 s |
| S1 effective MB/s | 2.9 MB/s | 7.2 MB/s | n/a | 7.2 MB/s |
| S2 desktop has laptop's 200 mods | 112.3 s | 14.6 s | not reached (cap) | 48.5 s |
| S2 laptop has desktop's 200 new | 1031.6 s | 52.5 s | not reached (cap) | 120.8 s |
| S2 two-way convergence | 1031.6 s | 52.5 s | cap: 0/200 mods, 7/200 new | 120.8 s |
| S3 restore -> lanes connected | 28.0 s | 37.4 s | transfer never started | 36.0 s |
| S3 restore -> transfer resumed | 28.0 s | 85.8 s | transfer never started | 88.5 s |
| S3 restore -> complete | 215.3 s | 152.0 s | transfer never started | 156.4 s |
| S1 laptop ingest into WinFsp (s) | 37.4 s | 31.1 s | 102.6 s | 163.6 s |

## Caveats

- **Wi-Fi variance.** The 256 MiB link probes before each run ranged tunnel up 8.56-8.84 MB/s, tunnel down 6.97-7.32 MB/s, scp up 7.05-8.78 MB/s, scp down 7.6-7.81 MB/s; ping max 3-4 ms. Each cell is a single run (n=1), and runs were sequential, not interleaved, so link drift between runs is not controlled.
- **Effective rate above the probe ceiling.** baseline-small: S1 effective 2.88 MB/s vs tunnel-down probe 7.22 MB/s; revamp-small: S1 effective 7.22 MB/s vs tunnel-down probe 7.32 MB/s; revamp-full: S1 effective 7.16 MB/s vs tunnel-down probe 6.97 MB/s. Both builds use 4 lanes per peer (1 control + 3 bulk), and all lanes and the probe cross the same kind of single `ssh -L` TCP connection, so parallel lanes do not explain the excess; the revamp's 99-103% of the probe is within the probe's own run-to-run spread (6.97-7.32 MB/s, measured before, not during, each run), i.e. the revamp's S1 is at the tunnel ceiling. Compare builds to each other, not to the probe. [Corrected in audit: the earlier text attributed the excess to the revamp's 3 bulk lanes vs a single TCP stream.]
- **Concurrent desktop builds.** Other agents were building and testing on the desktop throughout. Idle desktop CPU before the runs was 18.9-40.3%. Busiest scenario window: revamp-small S3 at 35.3% mean CPU; most build processes seen: 41 (baseline-full S2). Per-scenario CPU and build-process counts are in the CPU table. Mean build processes per scenario window (S1/S2/S3): baseline-small 1/1.6/1.6; revamp-small 1/1/6.0; baseline-full 2.7/1.8/0.7; revamp-full 1/1/1.7. Interference is therefore uneven between runs, and n=1 cannot separate it from build differences.
- **Laptop load.** Idle load samples before the runs: 1-12% (top CPU-time processes: MsMpEng, System, Taskmgr, perfmon). Scenario means ranged 9.3-25.1%; busiest revamp-full S2 at 25.1%. Windows servicing and Defender share the laptop with the source daemon.
- **Baseline full hits its caps by design.** baseline-full S1 and S2 stopped at 30 min, and its S3 transfer never started within the harness's 600 s start wait (the S1/S2 backlog was still unapplied), so its S3 row has no fault data. baseline-small gives the converged baseline numbers.
- **S3 `resumed_from_offset` (harness).** The v1.4 harness takes the kill-time status read and the minimum progress after restore while `complete` is false. baseline-small's kill-time status read returned no data (partial None -> 0), and revamp-small's last read before completion showed partial 0 with current_missing 0 but `complete` not yet true, so the harness says "no" for both. The "from partial_series" row re-derives it from the recorded series (last successful read before the kill; reads where the object is still missing): every completed S3 resumed from its offset.
- Milestones are polled (about +/-2 s; S2 one pass per ~4.5 s or slower). Laptop timestamps are clock-offset corrected. Control-lane counters differ in meaning between protocols; compare projection/content milestones.
- **Mounted check (revamp only, not a throughput comparison).** It also polls `manage status --json`. The gate went `awaiting` -> `none` and the mount attached 2.1 s after receive returned (3.1 s after the check began pairing), while big.bin was still downloading (6.1 of 536.9 MB). All 201 files listed with correct sizes and 200 of 201 hashed correctly through the mount, but reading big.bin failed: big.bin: [Errno 22] Invalid argument (big.bin is the first file read; the whole pass took 31.6 s). A file written through the mount reached the laptop and hashed correctly there. The folder stopped cleanly and the mount point was gone afterwards.

## Tables

## Comparability checks

**Comparability: all checks passed** (same harness version and dataset/run parameters per pair).

### baseline-small vs revamp-small

| Check | baseline-small | revamp-small | Verdict |
|---|---|---|---|
| harness_version | 1.4 | 1.4 | ok |
| resummarized_with | n/a | n/a | ok |
| scenarios run | S1,S2,S3 | S1,S2,S3 | ok |
| link ceiling measured | yes | yes | ok |
| S1 dataset files | 201 | 201 | ok |
| S1 dataset bytes | 543508342 | 543508342 | ok |
| S2 desktop new files | 200 | 200 | ok |
| S2 laptop modified files | 200 | 200 | ok |
| S3 file bytes | 536870912 | 536870912 | ok |
| S3 outage s (planned) | 45 | 45 | ok |
| S1 source-settle threshold cores | 0.25 | 0.25 | ok |
| endpoint | ws://127.0.0.1:43281 | ws://127.0.0.1:43281 | ok |
| baseline-small build commits (desktop / laptop) | 6298d0b / 6298d0b | expected 6298d0b | ok |
| revamp-small build commits (desktop / laptop) | e8c2979 / e8c2979 | expected e8c2979 | ok |

### baseline-full vs revamp-full

| Check | baseline-full | revamp-full | Verdict |
|---|---|---|---|
| harness_version | 1.4 | 1.4 | ok |
| resummarized_with | n/a | n/a | ok |
| scenarios run | S1,S2,S3 | S1,S2,S3 | ok |
| link ceiling measured | yes | yes | ok |
| S1 dataset files | 2001 | 2001 | ok |
| S1 dataset bytes | 603326745 | 603326745 | ok |
| S2 desktop new files | 200 | 200 | ok |
| S2 laptop modified files | 200 | 200 | ok |
| S3 file bytes | 536870912 | 536870912 | ok |
| S3 outage s (planned) | 45 | 45 | ok |
| S1 source-settle threshold cores | 0.25 | 0.25 | ok |
| endpoint | ws://127.0.0.1:43281 | ws://127.0.0.1:43281 | ok |
| baseline-full build commits (desktop / laptop) | 6298d0b / 6298d0b | expected 6298d0b | ok |
| revamp-full build commits (desktop / laptop) | e8c2979 / e8c2979 | expected e8c2979 | ok |

### revamp-mounted (not a throughput comparison: it also polls `manage status --json`)

- harness (lab_e2e helpers) 1.4 vs revamp-small 1.4: ok
- dataset 201 files / 543508342 bytes vs revamp-small S1 201 / 543508342: ok
- build commits e8c2979 / e8c2979: ok

### Builds and link

| Metric | baseline-small | revamp-small | baseline-full | revamp-full |
|---|---|---|---|---|
| Desktop / laptop commit | 6298d0b / 6298d0b | e8c2979 / e8c2979 | 6298d0b / 6298d0b | e8c2979 / e8c2979 |
| Desktop / laptop exe sha256 (12) | 69c6e8f8414f / 274b7de7e12e | 424e37612df4 / cd6687e09cfe | 69c6e8f8414f / 274b7de7e12e | 424e37612df4 / cd6687e09cfe |
| Harness | 1.4 | 1.4 | 1.4 | 1.4 |
| Run started (UTC ms) | 1792163059977 | 1792164883757 | 1792165528803 | 1792170265114 |
| Tunnel up / down MB/s (256 MiB) | 8.8 / 7.2 | 8.8 / 7.3 | 8.6 / 7.1 | 8.8 / 7.0 |
| scp up / down MB/s | 8.7 / 7.8 | 8.8 / 7.8 | 8.6 / 7.6 | 7.0 / 7.7 |
| Ping avg ms | 2.2 | 2.1 | 2.7 | 2.5 |
| Desktop idle CPU % (build procs) | 19.9 (1) | 18.9 (1) | 40.3 (2) | 23.8 (1) |
| Laptop idle load % | 4/6/5 | 1/6/2 | 7/1/5 | 4/12/4 |
| Errors | 0 | 0 | 0 | 0 |

### S1 initial receive (laptop -> desktop)

| Metric | baseline-small | revamp-small | baseline-full | revamp-full |
|---|---|---|---|---|
| Status | complete | complete | incomplete (cap reached) | complete |
| Dataset files / MB | 201 / 543.5 | 201 / 543.5 | 2001 / 603.3 | 2001 / 603.3 |
| Laptop ingest s (MB/s) | 37.4 (14.5) | 31.1 (17.5) | 102.6 (5.9) | 163.6 (3.7) |
| Source settled / waited s | yes / 14.3 | yes / 14.5 | yes / 14.6 | yes / 15.1 |
| Receive returned (s after pair) | 0.8 | 1.1 | 0.9 | 0.8 |
| Worker status OK (s after receive) | 2.0 | 2.0 | 2.0 | 4.2 |
| inspect `running` (s after receive) | 4.0 | 2.0 | 4.0 | 4.2 |
| First file in projection (s after receive) | 19.8 | 2.0 | 85.0 | 2.2 |
| First object bytes (s after receive) | 21.8 | 2.0 | 87.1 | 6.8 |
| **Metadata caught up**: all paths+hashes (s after receive) | 161.6 | 2.0 | n/a | 6.8 |
| Worker `metadata_caught_up` (s after receive) | 163.7 | n/a | n/a | n/a |
| **All content** (s after receive) | 188.5 | 75.3 | n/a | 84.2 |
| Effective MB/s (receive -> complete) | 2.9 | 7.2 | n/a | 7.2 |
| Transfer-phase MB/s | 3.3 | 7.4 | n/a | 7.8 |
| Peak 10 s window MB/s | 4.0 | 7.5 | 2.6 | 8.9 |
| Effective / tunnel-down ceiling | 40% | 99% | n/a | 103% |
| At cap: files visible / matching | - | - | 479 / 478 | - |
| At cap: object MB received | - | - | 553.1 | - |
| Verification backing ok / checked | 201 / 201 | 201 / 201 | n/a | 2001 / 2001 |

### S2 concurrent two-way incremental (+200 new on desktop, 200 modified on laptop)

| Metric | baseline-small | revamp-small | baseline-full | revamp-full |
|---|---|---|---|---|
| Status | complete | complete | incomplete (cap reached) | complete |
| Desktop imports done (s) | 15.6 | 14.0 | 30.2 | 46.7 |
| Laptop modifications done (s) | 10.4 | 13.7 | 643.5 | 28.4 |
| Desktop has laptop's 200 mods (s) | 112.3 | 14.6 | n/a | 48.5 |
| Laptop has desktop's 200 new (s) | 1031.6 | 52.5 | n/a | 120.8 |
| Metadata converged (s) | 1031.6 | 52.5 | n/a | 120.8 |
| **Two-way convergence** incl. content (s) | 1031.6 | 52.5 | n/a | 120.8 |
| Max seen mods / new (of 200) | 200 / 200 | 200 / 200 | 0 / 7 | 200 / 200 |
| State reads failed desktop / laptop (polls) | 0 / 0 (205) | 0 / 0 (15) | 0 / 58 (74) | 0 / 0 (20) |
| Conflicts desktop / laptop | 0 / 0 | 0 / 0 | None / None | 0 / 0 |
| Verification backing ok / checked | 25 / 25 | 25 / 25 | n/a | 25 / 25 |

### S3 tunnel outage (45 s) during a 512 MiB transfer

| Metric | baseline-small | revamp-small | baseline-full | revamp-full |
|---|---|---|---|---|
| Status | complete | complete | transfer never started | complete |
| Transfer started (s after copy began) | 31.7 | 24.5 | n/a | 23.1 |
| Downloaded at kill (MB; harness kill-time read) | n/a (kill-time status read failed) | 39.8 | n/a | 47.4 |
| Downloaded at last successful read before kill / min after restore (MB) | 16.8 / 18.9 | 39.8 / 45.4 | n/a | 47.4 / 57.9 |
| Outage s | 45.4 | 45.4 | n/a | 45.3 |
| Restore -> lanes connected (s) | 28.0 | 37.4 | n/a | 36.0 |
| Restore -> transfer resumed (s) | 28.0 | 85.8 | n/a | 88.5 |
| Resumed from offset (harness) | no | no | n/a | yes |
| Resumed from offset (from partial_series) | yes | yes | n/a | yes |
| **Restore -> complete** (s) | 215.3 | 152.0 | n/a | 156.4 |
| Start -> complete (s) | 266.3 | 202.8 | n/a | 207.1 |
| Verified | yes | yes | n/a | yes |
| Lane errors seen in outage | 3 | 3 | n/a | 3 |
| Worker health errors in outage | 0 | 0 | n/a | 0 |

### CPU (whole machine, sampled by the harness)

| Metric | baseline-small | revamp-small | baseline-full | revamp-full |
|---|---|---|---|---|
| S1 desktop CPU mean / p95 % (build procs mean) | 24.7 / 52.2 (1) | 15.4 / 25.7 (1) | 30.6 / 62.3 (2.7) | 11.7 / 22.9 (1) |
| S1 laptop CPU mean / max % | 12.2 / 20.7 | 11.9 / 16.6 | 17.9 / 36.3 | 16.2 / 30.4 |
| S2 desktop CPU mean / p95 % (build procs mean) | 22.4 / 44.1 (1.6) | 10.1 / 18.4 (1) | 29.7 / 52.7 (1.8) | 17.4 / 23.3 (1) |
| S2 laptop CPU mean / max % | 18.1 / 39.7 | 17.2 / 25.7 | 22.7 / 44.6 | 25.1 / 32.2 |
| S3 desktop CPU mean / p95 % (build procs mean) | 19.7 / 37.9 (1.6) | 35.3 / 59.7 (6.0) | 25.3 / 38.6 (0.7) | 13.6 / 38.6 (1.7) |
| S3 laptop CPU mean / max % | 11.9 / 32.2 | 9.3 / 20.7 | 18.8 / 28.2 | 14.6 / 28.7 |

### Observability: status calls during each scenario

| Metric | baseline-small | revamp-small | baseline-full | revamp-full |
|---|---|---|---|---|
| S1 desktop worker `status`: busy/stale of calls; p50/p95 ms | 63 of 95; 36/83 | 0 of 38; 28/40 | 454 of 892; 45/118 | 0 of 45; 29/88 |
| S1 desktop `manage inspect` p50/p95 ms | 32/45 | 29/42 | 32/63 | 29/37 |
| S1 laptop worker `status` p50/max ms (failures) | 68/83 (0) | 20/22 (0) | 507/709 (0) | 21/27 (0) |
| S2 desktop worker `status`: busy/stale of calls; p50/p95 ms | 44 of 514; 104/142 | 0 of 27; 26/38 | 52 of 906; 148/211 | 0 of 60; 29/148 |
| S2 desktop `manage inspect` p50/p95 ms | 32/50 | 27/35 | 32/54 | 30/38 |
| S2 laptop worker `status` p50/max ms (failures) | 27/295 (0) | 20/79 (0) | 27/640 (0) | 28/554 (0) |
| S3 desktop worker `status`: busy/stale of calls; p50/p95 ms | 26 of 149; 109/140 | 0 of 114; 32/45 | 52 of 299; 136/169 | 0 of 115; 27/36 |
| S3 desktop `manage inspect` p50/p95 ms | 31/43 | 35/50 | 31/42 | 27/35 |
| S3 laptop worker `status` p50/max ms (failures) | 126/161 (0) | 20/25 (0) | 27/673 (0) | 21/31 (0) |

### Mounted receive check (revamp only, WinFsp mount on the desktop)

| Metric | revamp-mounted |
|---|---|
| Status | incomplete or mismatched |
| Check / harness version | 1.1 / 1.4 |
| Desktop / laptop commit | e8c2979 / e8c2979 |
| Mount point (did not exist before) | C:\tkfs-build\lab\mnt\revamp-mounted (yes) |
| Dataset files / MB | 201 / 543.5 |
| Receive returned (s after pair) | 1.0 |
| Gate first seen open (s after receive) | 0.1 |
| Gate cleared (s after receive), as | 2.1, none |
| Mount attached (s after receive) | 2.1 |
| inspect `running` (s after receive) | 2.1 |
| Mount usable (s after receive) | 2.2 |
| Content complete when mount usable | no |
| Listing: files listed / expected (all match) | 201 / 201 (yes); 0.18 s |
| Hash via mount: ok / files (all match) | 200 / 201 (no) |
| Hash via mount: MB/s; large file s | 0.2; n/a |
| New 1 MiB file: desktop projection / laptop projection / laptop content (s after write) | 2.7 / 0.7 / 3.3 |
| New file hash through the laptop mount matches | yes |
| Gate: manage status samples (failures); status p50/p95 ms; inspect p50/p95 ms | 1 (0); 26/26; 37/37 |
| Stop folder rc / s / mount point gone | 0 / 0.05 / yes |
| Mount point exists after teardown | no |
| Errors | 0 |
