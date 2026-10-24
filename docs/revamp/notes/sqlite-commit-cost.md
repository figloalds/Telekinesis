# SQLite commit cost: measured split and next experiment

P0 remains open. No cache, fsync, access or checkpoint policy has been relaxed.

On `eff2afbc...`, the three-sample copy diagnostic measured 25.309 s in COMMIT
of 25.499 s in the inclusive `sqlite_batch` scope. Savepoint/release together
cost 0.095 s; switching synchronous mode cost 0.018 s. There were no projection
map copies. The paired phase waited 15.833 s for initial metadata catch-up,
then measured 87.39 ms median remote visibility. Both daemons stopped normally.
Copy and peer overhead still miss the targets. The
[report](../../../test-evidence/wave-6/transaction-split-sandbox-20261024.json)
retains exact counters, scripts, exits and a verified native store archive.

The read-only mode probe on `80105f08...` attributes 15.056 s to 4,703 NORMAL
commits and 8.291 s to 554 FULL commits, with zero unknown-mode commits.
The daemon stopped gracefully in 0.586 s. This was a three-run copy diagnostic
without a native baseline, not performance qualification; see its
[record](../../../test-evidence/wave-6/commit-modes-sandbox-20261024.json).

SQLite documents that its default automatic checkpoint runs on the thread that
commits past 1,000 WAL pages. With synchronous=NORMAL, checkpoint work issues
the relevant sync barriers. This makes automatic checkpoint work a plausible
source of the NORMAL cost, but the mode counters do not establish how much
time actually belongs to checkpoint versus ordinary WAL writes. See the
[SQLite WAL performance discussion](https://sqlite.org/wal.html#performance_considerations).

Before introducing another writer/checkpointer connection, the bundled engine
was upgraded from SQLite 3.50.2 to 3.53.2 (`rusqlite 0.40.2`). The former
predates the [WAL-reset race fix](https://sqlite.org/wal.html#walreset), which
specifically concerns concurrent commit/checkpoint connections. Statement
caching, hooks and the previous fallible unsigned SQL conversions remain
enabled. A host version-floor regression protects this prerequisite. The
upgrade does not itself change checkpoint policy.

The upgraded engine with corrected WinFsp import binding (`805b8634...`) passes
the 44-case VM matrix and six timestamp laps. Its three-run copy diagnostic
measures 17.076 s in 4,763 NORMAL commits and 7.664 s in 544 FULL commits;
the daemon exits gracefully in 0.540 s. See the
[retained record](../../../test-evidence/wave-6/iat-sqlite-sandbox-20261024.json).
This does not show a commit-cost improvement and is not a full performance lap.

Next, attribute the automatic-checkpoint portion of the NORMAL commit cost
before selecting an implementation. A replacement checkpoint worker must
still run regular checkpoints, preserve the existing metadata commit window,
serialize with explicit FULL checkpoint barriers, report failed/incomplete
durability and retain WAL/recovery evidence. Turning off checkpoints without
a working replacement is not an accepted optimization. File fsync must still
make pack bytes, index entries, metadata and receipts durable in order.

The next diagnostic candidate wraps the bundled SQLite default WAL callback
with timing. It still invokes the same synchronous PASSIVE checkpoint on the
committing connection whenever the WAL contains at least 1,000 frames. The
installer refuses a differently configured threshold. Its callback preserves
the default hook's non-failing commit result on a checkpoint error and counts
such errors; explicit FULL barriers still check and report their own results.
No worker or asynchronous checkpoint policy is introduced.

Registering a WAL hook replaces SQLite's built-in automatic hook, so
`PRAGMA wal_autocheckpoint` reports zero afterward even though the replacement
still performs the same 1,000-frame checkpoints. The behavior and threshold
are covered by a native SQLite regression against the unmodified default:
below-threshold commits do not checkpoint, a retained reader limits progress
without rejecting the commit, and the next commit after reader release
completes the checkpoint. Another test refuses a non-default threshold.
See the [automatic-checkpoint API](https://sqlite.org/c3ref/wal_autocheckpoint.html),
[PASSIVE checkpoint equivalence](https://sqlite.org/c3ref/wal_checkpoint.html)
and [WAL hook contract](https://sqlite.org/c3ref/wal_hook.html). The bundled
`sqlite3.c` implementation of `sqlite3WalDefaultHook` was also checked.

Candidate `33c3d36e...` passes 172 host tests, strict Clippy, formatting and a
release build. Its three-run VM copy diagnostic attributes **18.155 s of
22.844 s COMMIT time (79.5%)** to 167 automatic checkpoints, with no reported
checkpoint errors. NORMAL/FULL commit times are 16.406 s/6.363 s; checkpoint
time is nested in both modes and must not be added to them. The copy median
is 11.550 s without a native baseline; the daemon exits gracefully in 0.485 s.
The [retained diagnostic](../../../test-evidence/wave-6/checkpoint-profile-sandbox-20261024.json)
includes the counters, workload, exit record and CRC-verified complete export.
This establishes automatic checkpoint work as the next measured cost to move
off the foreground Store lock, subject to the constraints above.

FULL commit cost can be reduced only by preserving each explicit fsync point
and coalescing eligible background seals. The delayed-seal contract must not
drop successor writes, timestamps, retained-reader paging data or recovery
bytes. All such changes require meaningful concurrency/recovery regressions,
the isolated 44-case lifecycle matrix and a controlled complete gate.
