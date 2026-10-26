# Known-event echo: counter change without measured WAL writes

The peer-copy gate still fails at +23.3%. Reapplying a known event executes an
unconditional clock UPDATE, even when its value is unchanged. A native
regression shows that this increases SQLite's `total_changes` and invalidates
the Store's cached metadata durability stamp.

A host-only candidate makes that UPDATE conditional. Receipts deliberately
keep an unconditional real FULL WAL write, including replay of an existing
receipt after namespace-only changes. Four focused regressions pass: stable
echo/stamp, new/replayed receipt durability, waiting-parent recovery including
unknown self-authored history, and clock advancement/collision rejection.
The candidate passes 211 host tests, strict Clippy, formatting, release and
five inert binding fixtures. No real WinFsp DLL, driver or mount runs on host.

That counter result does not prove physical write amplification. A separate
optimized native stress executes 1,000 direct Store
requests of 64 known events, with explicit metadata sync after each request.
One warmup plus five samples per phase, on fresh native databases:

| Phase | Version | Median | Written bytes during measured loops |
|---|---|---:|---:|
| A1 | Qualified source | 326.9 ms | 0 in every sample |
| B1 | Conditional clock | 328.4 ms | 0 in every sample |
| B2 | Conditional clock | 319.1 ms | 0 in every sample |
| A2 | Qualified source | 331.2 ms | 0 in every sample |

An independent SQLite reader observes no new commit in either version. Both
versions verify all 64 events/entities after reopening every sample. The
baseline library is rebuilt from the exact qualified source and its hash
matches the initial baseline library. Process I/O counters bracket the loop;
setup and reopen validation are outside timing. This isolates stable echoes,
not real peer-copy traffic or an active foreground mutation workload.

SQLite avoids physical WAL writes for the unchanged row in this case. The
conditional-clock candidate establishes no meaningful repeatable wall-time
benefit and is not promoted. The exact production sources and selected
VM-qualified executable `272a1194...` are restored. The
[rejected-candidate record](../../../test-evidence/wave-6/metadata-echo-rejected-host-20261026.json)
retains both source snapshots, patch, failing-before/passing-after regression,
all checks, raw native runs and exact library/executable identities. P0 remains
open; this result rules out the proposed stable-echo WAL-write explanation.
