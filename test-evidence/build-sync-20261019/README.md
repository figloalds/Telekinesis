# Mounted-build sync investigation, 2026-10-19

This is investigation evidence, not runtime qualification or a confirmed
reproduction of the incident.

- [summary.json](summary.json) normalizes observations from the local
  diagnostic bundle and read-only database inspection. Full logs, addresses,
  project paths and object identifiers were deliberately not copied.
- [sqlite_snapshot_probe.py](sqlite_snapshot_probe.py) uses two connections
  to a temporary WAL database to model a failed read-to-write upgrade. It
  demonstrates how swallowing that failure can leave a derived availability
  row absent while a subsequent reference transaction succeeds. It never
  reads or modifies the running TKFS installation.
- [sqlite_snapshot_probe.result.json](sqlite_snapshot_probe.result.json)
  records the successful model run on SQLite 3.51.1. Run again with
  `python sqlite_snapshot_probe.py` (Python 3.11 or newer with SQLite WAL
  support). The program asserts the extended error code and removes its
  temporary directory after closing both connections.

The model is intentionally smaller than TKFS: it does not verify object
hashes, model provenance/authorization, reproduce Rust call ordering, test
fsync/rename durability or prove the cause of the production incident.
Those require the proposed Rust fault-injection and mounted-build scenarios.

See the [study and work-package proposal](../../docs/revamp/notes/wp6-build-sync-juicefs-study.md)
for confidence levels, source references and the next investigations.
