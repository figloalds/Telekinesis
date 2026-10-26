# Request-local metadata echo, 2026-10-26

The corrected-boundary gate still fails peer copy at +21.208%. Its existing
source-side copy/delete counter brackets have median version-decode counts of
18,084 without the peer and 30,084 with it; version queries rise by the same
12,000. New events encoded and projection deltas stay equal. These inclusive
global counters bracket both copy and deletion, not just the timed copy body;
their durations cannot be added as wall-time attribution.

This identifies actual repeated version processing, a different question from
the rejected [conditional clock-update experiment](metadata-echo-cost.md),
which wrote zero bytes in its stable-echo native stress. It also differs from
the rejected [cached presence experiments](cached-prefetch-presence-cost.md).

## Contract and candidate

Candidate `60eb4c78...` keeps normal request validation and `apply_remote`
before constructing the reply. It omits a reply event only when all its decoded
fields compare equal to an event in that same request. The comparison includes
ID, parents, operation, state, branch, author and clock. The temporary lookup
contains at most the existing 256-request-event limit and retains no knowledge
across requests or connections.

The reply cursor and `more` flag still describe the complete scanned journal
page before filtering. An empty filtered page can therefore have `more=true`;
the unchanged client continues through it. No event is removed from the journal.
An empty-request replay still retrieves the complete history. Author-based
filtering would be incorrect: a restored device can need unknown events with
its own author ID, so authors are not used to infer knowledge.

The original apply path still rejects ID collisions and processes waiting
children. Independent history and children resolved from an earlier request
remain in the reply. Repository authentication, request/page limits, data fetch,
metadata/receipt and pack barriers, notifications, cache policy and access checks
are unchanged. Only `src/orchestrator/sync.rs` changes among compiled sources.

Five regressions cover full/partial page cursors and replay, independent
history and collisions, waiting parents and recovery from a real pre-event
SQLite backup with the same device ID, oversized requests, and the actual
authenticated default-bound session with the unchanged client exchange loop.
Three initial backend regressions fail before filtering and pass afterwards.
All 214 host tests, strict Clippy, release and five inert binding fixtures pass.

## Native authenticated measurement

The [host evidence](../../../test-evidence/wave-6/feed-echo-host-20261026.json)
retains every sample, exact source/binary identities and the prior full-gate
counter deltas. A native source sends 6,000 valid create events through real
authenticated default-bound sessions. Both journals and projections verify
after every repetition. No mount or real WinFsp DLL is loaded on the host.
Each phase discards one warmup and retains five repetitions:

| A–B–B–A phase | Baseline | Candidate | Candidate | Baseline |
|---|---:|---:|---:|---:|
| Median replay, ms | 441.161 | 378.935 | 403.367 | 421.954 |
| Requests | 24 | 24 | 24 | 24 |
| Reply events | 6,000 | 0 | 0 | 6,000 |
| Reply body bytes | 3,785,803 | 934 | 934 | 3,785,803 |

This is a single-connection metadata replay, not the mounted copy workload
or its two-worker peer overhead. It reduces redundant transfer and known-version
validation without changing the actual received event set. It does not establish
the same percentage gain for copy or solve the native performance targets.

## Isolated qualification

The exact `60eb4c78...` executable passes all 44 lifecycle cases, six timestamp
laps and four additional 4 MiB near-255 paired cases in the isolated guest.
The full original-size five-sample native/mounted/idle-peer matrix is running,
with the corrected metadata-plus-payload initial boundary and unchanged targets.
No host compilation or heavy host I/O overlaps timing. P0 remains open pending
the complete measured result and verified native export.
