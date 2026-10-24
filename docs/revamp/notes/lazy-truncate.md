# Truncate without hydrating discarded immutable data

`Engine::truncate` previously hydrated every immutable chunk before changing
the scratch length. Reopening a sealed 1 GiB file with overwrite semantics
therefore read and copied the whole old file, with hydration durability work,
before discarding its bytes. This is independent of WinFsp cache purging.

Truncation now changes the physical scratch length directly. Unhydrated
retained bytes continue to read from the immutable prefix. The persisted
`base_visible_len` clips that prefix at every shrink; later extension returns
zeros beyond the boundary. A first shrink materializes only the retained
length, rather than allocating the original length first.

The physical shrink precedes publication of the clipped boundary. Publication
syncs the scratch and atomically replaces the hydration/recovery record with
write-through semantics. On publication failure the in-memory operation stays
dirty and the record retains its previous boundary. A same-size retry, later
extension or extending write must persist the clipped boundary successfully
before it can expose additional bytes. A graceful error path alone cannot
establish this crash contract.

Three regressions failed before the change and pass afterward:

- Shrink to a retained prefix or zero without immutable pack reads or hydrated
  chunks, then extend and verify the prefix plus zeros.
- Block the recovery-record replacement, verify physical shrink still needs no
  hydration, reject same-size retry and extension until the marker can persist,
  then verify the recovered boundary and zero tail.
- Drop an unsealed, shrunk/re-extended draft and recover its original prefix
  and zero tail from the scratch and durable marker.

The host suite has 170 passing tests and two intentionally ignored opt-in
tests; strict Clippy, formatting and release build pass. The five native
import-binding child fixtures were already exercised separately and their
native implementation is unchanged by this patch. Candidate SHA-256:
`b9908f33e82c0ea7e6e055bfb61af363d31a1807e559983bd31d49218c1771e7`.

All 44 isolated lifecycle cases and six timestamp laps pass. Maximum forced
exit is 0.015 s and ordinary graceful exit 0.169 s. The three-sample sequential
diagnostic compares the old and new binaries with the same WinFsp/SQLite:
write median falls from 11.413 s to 5.647 s (50.5%); operation 6 cumulative
time falls from 22.972 s to 1.513 s across the warm-up plus measured rounds.
The two daemons stop gracefully in 0.104 s and 0.112 s. Read median changes
from 2.087 s to 2.551 s, with overlapping sample ranges; this is no evidence
of a read improvement. Native write/read medians are 1.887 s/1.136 s.

The [retained diagnostic](../../../test-evidence/wave-6/lazy-truncate-sandbox-20261024.json)
includes all reports, callback deltas, exit states and the CRC-verified export
(833 members), with exact timestamps alongside the local archive. Host
compilation finished before the timed sequential runs. No checkpoint, cache,
durability or access policy changes. Write/read still miss the 1.2x target;
this is not the complete performance or purge-cost acceptance.
