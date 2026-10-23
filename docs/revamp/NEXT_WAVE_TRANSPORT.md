# Next-wave direction: file-aware, peer-aware transfer planner

Status: agreed planning direction, 2026-10-20. The user wants this workstream
considered for the next wave, with a flexible foundation for experimenting with
strategies before selecting a strong default set. The initial strategies,
delivery packages and acceptance thresholds remain open for discussion. This
is not the complete next-wave plan or an implementation/completion claim.

## Intent

The [personal-use deployment and authority scope](AUTHORITY_SCOPE.md) applies
to this direction. Reuse TLS/WSS or VPN channel guarantees; add no custom
security layer, Users or ACLs. Authorization below means existing DVFS
repository/reference scope. Codec negotiation and bounds support reconstruction,
interoperability and resource correctness, not a new security handshake.

Choose how to transfer content using knowledge of the file revision, its chunks,
the receiving peer and current resource conditions. Permit strategies to be
added, compared, disabled and replaced without rewriting the transfer scheduler
or changing canonical content identity. Optimize end-to-end completion and
foreground responsiveness, with wire size as one measured cost.

Wave 6 remains governed by [its plan](WAVE_6_PLAN.md) and outstanding gates.
This direction does not add compression or delta work to its current scope.

## Proposed foundation

- Keep canonical file manifests and chunk identities independent of transport
  encoding. Every received representation reconstructs the expected bytes and
  passes the existing identity, authorization and durable-install checks.
- Separate planner policy from strategy implementations and transport execution.
  A strategy describes eligibility, required peer capabilities/base objects/
  dictionaries, estimated costs and resource bounds, then encodes or reconstructs
  a bounded unit. The common executor owns cancellation, scheduling, accounting,
  verification and completion semantics. Exact APIs are a design decision.
- Support a raw independent-transfer baseline and explicit capability negotiation.
  Optional strategy unavailability can select a supported fallback within the
  request budget; corruption or authorization failures must remain visible.
- Maintain discardable optimization profiles for immutable revisions/chunks:
  observed compression ratio, CPU cost, similarity hints and candidate encodings.
  File type/path is a hint, not proof. Allow chunk-level exceptions for mixed data.
  Combine these with peer-held content/dictionaries, link estimates, priority and
  CPU/memory pressure. Profiles must not become correctness or metadata authority.
- Update summaries incrementally from changed chunks and already-required I/O.
  A small edit or foreground range request must not require scanning the whole
  file, flattening its manifest or loading its complete chunk list.
- Bound strategy attempts, discovery, encoding/decoding memory, CPU work and cache
  size. Use existing class/peer/repository reservations, including capacity for
  foreground reads and completion. Avoid trying every strategy on every chunk.
- Make strategy selection observable and reproducible. Record the selected
  strategy, decision reason, dependencies, estimates, actual costs and fallbacks.
  Provide controlled experiment configuration and a way to disable an optional
  strategy without making canonical content inaccessible.

Flexibility means a small interchangeable strategy interface with shared
invariants; dynamic plugin loading, an unrestricted composition graph and an
autonomous learning system are not requirements established by this discussion.

## Candidate strategies to discuss

| Candidate | Motivation | Open questions |
|---|---|---|
| Adaptive raw versus ordinary compression | Compress suitable text/structured data; avoid wasted work on already compressed or encrypted bytes | Codec/level, sampling, minimum savings and CPU/link policy |
| Shared dictionary compression | Reuse patterns across related chunks/files while retaining independent chunk decoding given the dictionary | Grouping, training/refresh budgets, dictionary sizes, distribution and cache policy |
| Delta transfer against receiver-held content | Reduce changed-byte transfer beyond exact chunk reuse, potentially including shifted content | Base discovery, bounded matching/reconstruction, fallback and base lifetime |
| Cached alternative encodings | Amortize encoding work across repeated transfers | Cache keys, eviction, storage budget and permission scope |

These are candidates, not a selected implementation set or priority order.
Fixed-chunk reuse remains the existing foundation. Content-defined chunking,
pack storage and changes to canonical chunking require separate decisions;
transport extensibility does not automatically authorize them.

For shared dictionaries, evaluate content similarity as well as folder locality.
Use immutable identities and explicit versions; a dictionary may contain source
fragments, so training and distribution must respect the relevant authorization
scope. Include dictionary transfer, cache misses, refresh and retirement in cost
measurements. Preserve a supported transfer path when a peer lacks the dictionary.
Avoid making random access depend on decoding all preceding file chunks.

For deltas, investigate a bounded directly available base first. Verify the
reconstructed canonical chunk before installation, and account for base reads,
pin lifetimes and discovery round trips. Unbounded delta chains are not a desired
foundation. Compression benefit and exact-chunk reuse are independent: encrypted
or archive content may still contain unchanged reusable chunks.

## Experiment and qualification direction

Use the same corpus, peer state and source identity to compare the baseline and
each candidate. Include source/JSON/XML/logs, archives/media, encrypted or random
bytes and mixed files; test first transfer, repeated revisions, text insertion,
atomic replacement and small random overwrites. Evaluate fast LAN and constrained
or high-latency links, cold and warm dictionaries/bases, and foreground demand
during bulk work across repositories.

Measure completion time and foreground tail latency, actual wire bytes including
dependencies/protocol overhead, encoding/decoding CPU, disk reads/writes, memory,
cache/training cost, recovery and fallback behavior. Report individual samples and
limitations; select defaults from end-to-end evidence rather than compression
ratio alone. Exact thresholds and the foundational strategy set remain undecided.

Exercise unsupported capabilities, missing/stale bases, missing/wrong dictionaries,
malformed or oversized decoded output, cancellation, disconnection/restart and
permission changes. Preserve existing acknowledgement/durability semantics and
resource bounds. Experimental encodings must reconstruct the same canonical bytes.

## Context and references

At discussion time, legacy `src/wire.rs` has optional gzip, while current v3
session frames carry raw bodies through `src/session/frame.rs`; Wave 6 content
transport uses that session path. Existing pipeline improvements are not evidence
for compression gains. Recheck implementation and evidence before designing the
next wave.

- [Wave 6 implementation record](notes/wp6-progress.md)
- [Zstandard dictionary training guidance](https://github.com/facebook/zstd/blob/dev/lib/zdict.h)
- [Zstandard frame and dictionary format](https://www.rfc-editor.org/rfc/rfc8878.html)
- [Dictionary transport and security considerations](https://www.rfc-editor.org/info/rfc9842/)
- [Rsync delta-transfer algorithm](https://rsync.samba.org/tech_report/)
