# Bound shared locks around open and close

Date: 2026-10-24. Part of W6-P0 performance acceptance.

The running Git diagnostic exposed large cumulative scratch-open and Close
callback costs. Two regressions independently confirmed unnecessary contention:
Close held the global handle map while waiting for its scratch mutex, and a
remote manifest fetch during Open held the scratch index used by every entity.
Both tests failed before their respective fixes and pass afterwards. The
counters alone do not establish how much of the benchmark cost they explain.

Close now removes its handle under the map lock, then releases the scratch
outside that lock. Scratch initialization uses a weak per-entity gate and
releases the global index before hydration and host file creation. Concurrent
opens of one entity still share a single draft; an eight-thread regression
checks the identity, handle/writer counts and single on-disk recovery record.
Unused initialization gates are reaped without retaining content pins.

All 150 Rust tests, format and Clippy pass. The release also contains the
[live receipt retry correction](recovery-timestamps.md). It still needs its own
isolated VM lifecycle matrix and measurements. Earlier binary results do not
qualify these changes. Caching, cleanup flushing, access checks, fsync barriers
and the no-self-handle rule remain enabled.
