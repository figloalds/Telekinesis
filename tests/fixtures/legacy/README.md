# Legacy fixtures (format of 6298d0b)

Captured by WP0.5 with the baseline code and binary (`revamp/wave-0-base`,
content identical to 6298d0b plus the revamp docs). Every integration package
checks that they still open and upgrade (REVAMP_PLAN §9, §13 rule 8.3).
Never edit or re-capture them after Wave 0: they stand for data written by the
old release. Tests use temp copies:

- `support::copy_legacy_worker()` → `worker/state`
- `support::copy_legacy_datadir()` → `datadir/data` (+ a fresh `orchestrator.toml`)

## `worker/`

A worker state directory written by the baseline library (`Store`, the same
code the baseline binary runs):

| Content | Where |
|---|---|
| one unresolved and one resolved content conflict | `conflicts` |
| 2 staged rows waiting for a parent that never arrives (`error` NULL) | `incoming_events` |
| 2 quarantined rows: `INVALID_KIND` and its `QUARANTINED_CAUSAL_ANCESTOR` child | `incoming_events` |
| a partial spool: 64 KiB of a 256 KiB object | `incoming-objects/<hash>` |
| one pending save with its stage file | `pending_saves`, `staging/<uuid>.stage` |

`manifest.json` records the counts, the partial object, and the pending save's
path and content hash. The pending-save row is written with the exact SQL and
payload layout of `Engine::retain_pending` at 6298d0b (private there), because
the baseline has no public way to make a flush fail.

## `datadir/`

The data directory of a dialer daemon after a graceful `shutdown`, captured by
`capture_compat_goldens_and_legacy_datadir` in `tests/harness_selftest.rs`
from two real baseline daemons:

| Content | Where |
|---|---|
| a received folder (running, `hello.txt`) | `projects`, `states/<id>` |
| a received folder gated by `initial_sync` (mount requested) | `projects`, `initial_sync` |
| a stopped local folder (`desired=0`) | `projects` |
| one outbound peer (the dead capture-time acceptor) | `peers` |
| a pairing verifier (salt + hash) | `settings.pairing` |

SQLite WAL files were checkpointed into the database files; worker logs and
`runtime.json` files were removed. `projects.directory` and `projects.mount`
hold absolute capture-time paths: `copy_legacy_datadir()` relocates them from
`manifest.root` to the temp root, as a user moving the directory would.
