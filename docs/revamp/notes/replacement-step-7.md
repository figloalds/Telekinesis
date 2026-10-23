# Step 7: runtime cutover and qualification

The replacement now occupies the canonical content/core/runtime/native paths.
The old write journal, overlay maps, save pipeline, admission engine, lease
models, cone projection and qualification test apparatus have been removed.
The pairing catalog shares the metadata database. The CLI and desktop conflicts
view use entity versions. Session framing/authentication/TLS suites are retained.

WinFsp and FUSE use scratch handles with kernel caching and remote notification.
The real Windows smoke passes create/read/write/fsync/rename and paired cached
invalidation. FUSE testing found a metadata-successor scratch-sharing bug; a
content-version anchor and exact regression tests now cover that case.

Qualification is **open**. The initial full matrix failed performance targets
and exposed stale unsealed sizes to Git. The current working projection now
supplies draft sizes; further full-gate runs are required. These are measured
failures, not passing qualification. See `test-evidence/bench/wave6-*.json` and
the benchmark command in `docs/VALIDATION.md`.
