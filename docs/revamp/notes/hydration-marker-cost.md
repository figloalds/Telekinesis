# Track pending hydration markers without scanning the whole draft

`Scratch::hydrate` checked every hydrated chunk against the recorded set after
each write, even when both sets were already identical. A fully materialized
1 GiB file has 16,384 chunks; 10,000 small overwrites repeated that full search.
The check protects recovery markers and cannot simply be removed.

The replacement tracks whether newly materialized immutable ranges still need
their marker. It sets that indication when adding a range and clears it only
after successful marker publication. A failure keeps it set across subsequent
operations on different ranges. New/recovered drafts start with matching sets.
The persisted format, scratch/marker synchronization and retry ordering stay
the same. A conservative extra retry after unrelated state changes is safe.

An opt-in release diagnostic performs 10,000 random 4 KiB writes to matching,
fully materialized zero-filled native drafts, checking the final bytes:

| Draft | Whole-set check | Pending indication |
|---|---:|---:|
| 4 MiB, 64 chunks | 55.6 ms | 51.1 ms |
| 1 GiB, 16,384 chunks | 3,984.9 ms | 334.8 ms |

These are one execution per size/implementation on native backing files,
excluding fixture construction and including backing-file I/O. They demonstrate
the scaling improvement; they are not a mounted throughput gate. A regression
injects marker replacement failure and
checks that a later append outside the immutable base cannot bypass the
pending marker, then verifies the marker and all unchanged/appended bytes.
Existing failed-marker, truncate/re-extension and recovery cases also pass.

Candidate `cad2200f...` passes 186 host tests, strict Clippy, formatting/release
and five bounded inert binding fixtures. Three opt-in tests are ignored by the
ordinary suite. The [host evidence](../../../test-evidence/wave-6/hydration-pending-host-20261024.json)
retains the diagnostic logs, current source and hashes. All 44 isolated lifecycle
cases and six additional timestamp laps pass (forced exit max 0.012 s; ordinary
graceful max 0.177 s). Its targeted three-sample mounted overwrite median is
2.708 s, versus the earlier full gate's 10.631 s; their native medians differ,
so those absolute times should not be presented as matched ratios. The new
matched native overwrite median is 0.216 s, giving a still-failing 12.54x ratio.
Its measured repetitions deliver about 9,400 Write callbacks each, with
273–275 ms of cumulative Write time, versus about 4.2 s in the earlier gate.
Asynchronous Close/seal work still crosses sample boundaries; callback timer
deltas cannot be added to or subtracted from wall time as disjoint costs.
Copy remains 6.43x and sequential write/read 3.88x/1.51x native. The daemon stops
normally in 2.052 s. The 2.234 GB export passes CRC, SHA-256 and exact-timestamp
manifest size verification
([VM evidence](../../../test-evidence/wave-6/hydration-pending-sandbox-20261024.json)).
The complete five-sample gate also finished on this binary. All 54 build
observations are correct (18 native, 18 mounted and 18 with an idle peer).
Mounted/native medians are copy 5.13x, sequential write/read 3.51x/2.00x,
random overwrite 14.57x and Git 9.59x, all failing their targets. Clean/no-op
builds pass at 1.03x/1.05x. The matched native random median is 0.216 s and the
mounted median 3.143 s; the earlier targeted 2.708 s is a different experiment.

Initial peer import reaches source event 143,175 in 201.498 s within the explicit
300-second bound. All idle-peer workloads complete correctly, including five
remote-visibility samples (37.658 ms median, 55.972 ms maximum). Idle-peer
overhead still fails the 20% limit: copy +29.3%, sequential write/read
+35.0%/+32.3%, random +61.5% and Git +26.2%. Clean/no-op overhead passes at
+11.1%/+6.6%. File fsync is measured separately: its median rises from 1.482 s
to 7.423 s with the peer; it has no separate numeric target in the gate.

Both daemons exit normally in 0.343/0.256 s, without kills or stranded
processes. The complete 5.634 GB export (738 members) passes CRC, SHA-256 and
exact-timestamp manifest size checks. After timing and shutdown, a separate
read-only verifier reconstructs all six seeded overwrite laps and compares
every byte of the final 1 GiB directly with the committed native packs. All
16,385 objects and 53,560 changed pages agree, including the full SHA-256.
Its independent small fixture also rejects an injected corrupt pack. Original
SQLite files remain untouched; the verifier reads copies including their WAL.
The [full-gate evidence](../../../test-evidence/wave-6/hydration-full-sandbox-20261024.json)
retains the reports, callback observations, traces, exits and verifier source.
No host compilation overlaps timed phases. P0 remains open on performance.
