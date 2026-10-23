# Step 1: benchmark gate

`scripts/bench.py` runs one warm-up and five samples by default. The native
baseline was rerun on 2026-10-23 and is in `test-evidence/bench/native.json`.
Medians: small-file copy 975 ms; sequential write/read 340/294 ms; random
overwrites 57 ms; git status 78 ms; clean/no-op build 4,941/508 ms.

Comparison now exits unsuccessfully for exceeded limits, incompatible workload
parameters, and failed builds. Source staging is regenerated deterministically
and cannot overlap the target. Remote visibility requires a peer mount.

Mounted measurements will run after the adapter cutover. Native measurements
alone do not establish any mounted performance claim.
