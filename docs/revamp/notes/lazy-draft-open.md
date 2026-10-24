# Immutable opens without copying bytes

Date: 2026-10-24. Candidate 1 in the W6-P0 performance triage.

Regular-file handles share an in-memory draft and pin their immutable base.
Open and read create no host scratch or sidecar. Reads combine contiguous
immutable ranges from the pack/LRU with materialized ranges from the shared
scratch. The first write or size change creates the host file and recovery
record; partial writes hydrate only touched chunks. Constrained paging writes
through retained readers use the same shared draft and access rules as before.
Manifest pinning still runs on open to protect the base and its chunks from GC.

Recovery markers precede edits to newly materialized ranges. A failed marker
update restores the in-memory marker so a retry cannot accept unrecorded edits.
The existing receipt comparison, file-fsync barriers, cache settings, cleanup
flushing and no-self-handle rule remain enabled.

Host regressions verify no scratch creation on open/read, shared reader/writer
overlays across chunk boundaries, and restart recovery of mixed immutable and
materialized ranges both before and after a prior seal. The no-scratch regression
failed on the preceding implementation. Existing access, cached identity,
timestamp and recovery tests remain part of validation.

Before the VM lap, the prediction is: open-stage `scratch` time and cold Open
callback time fall, and a reopened immutable read workload creates zero scratch
files. Read callback time may rise because bytes now come directly from packs;
wall time must be measured as well. `scripts/mount_open_diagnostic.py` records
per-round before/after counters, native scratch counts, binary hashes and bounded
exit records. It reads 500 32-KiB files plus one 64-MiB file after reopening the
store. It is a diagnostic, not the full performance gate. The lifecycle matrix
now embeds counters before each requested stop and save/restart check.

The first VM diagnostic (`3ad93ba2...`) confirmed the predicted movement:
501 host scratches became zero; cold open-stage scratch time fell from 658 ms
to 14 ms, and first-round wall time from 1,640 ms to 965 ms. Its lifecycle
matrix stopped at a [timestamp discrepancy](cached-write-times.md), after 19
of 20 attempted cases passed. All diagnostic daemons stopped cleanly. These
measurements are not a full performance gate.

The timestamp follow-up needs its own VM qualification. Earlier releases do not
qualify it. The P0 remains OPEN.
