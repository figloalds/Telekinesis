# Wave 6 supplement: TigerBeetle lessons for offline metadata sync

Planning update: [WAVE_6_PLAN.md](../WAVE_6_PLAN.md) owns current scope, package
numbering and gates. WP6 labels below refer to the earlier exploratory plan.

2026-10-19. **Source study and proposal; no runtime implementation.** Builds on
the [JuiceFS/build investigation](wp6-build-sync-juicefs-study.md). Inspected
the sibling `..\tigerbeetle` at clean revision
`0450c32b2c3c3ab6114f3402cc601341cc8192aa`. Source links below pin that revision.
No TigerBeetle binaries, benchmarks or tests were run.

## 1. Authority and offline operation

A single logical authority does not prohibit all offline access: cached data
can be read, and disconnected clients can record tentative changes for later
submission. It does prohibit *every disconnected client independently making
globally final decisions* while retaining one linearizable global history and
unrestricted operations. Replicating the authority across several machines
improves availability but does not remove that partition tradeoff.

An isolated client can guarantee its own local durability and read its own
writes. It cannot know that another isolated client has not renamed the same
entity, created the same name, deleted its parent or revoked access. Global
agreement must wait, or the system must define reconciliation. Restricted
ownership/escrow can permit selected offline operations, but is not a general
replacement for TKFS's offline editing model. Revocation during disconnection
also needs an explicit policy; no transport/runtime solves it automatically.

Keep TKFS's locally durable causal events and deterministic reconciliation.
Distinguish **locally committed**, **received/staged by a peer**, **validated
and activated by that peer**, **content available**, and **converged with
currently observed peers**. None of these alone proves agreement with every
offline peer. A single metadata writer *inside each local process/store* is
compatible with this model; it is not a single global authority.

TigerBeetle uses a primary and quorum-backed replicated state machine to agree
on one order. A disconnected minority cannot independently commit new global
transactions. Conflicting proposals are coordinated before commitment; it is
not an algorithm for merging independent offline branches. The useful lessons
are how it executes, persists, transports and repairs state.

## 2. Techniques to adapt

| TigerBeetle evidence | Proposed TKFS adaptation | Boundary to preserve |
|---|---|---|
| [State machine](https://github.com/tigerbeetle/tigerbeetle/blob/0450c32b2c3c3ab6114f3402cc601341cc8192aa/src/state_machine.zig): explicit prepare, asynchronous prefetch and commit phases. [Replica](https://github.com/tigerbeetle/tigerbeetle/blob/0450c32b2c3c3ab6114f3402cc601341cc8192aa/src/vsr/replica.zig): bounded pipelining with ordered execution. | Prepare/read expensive inputs concurrently, revalidate affected state, then execute short deterministic commits. Compare keyed mutation locks with a bounded transaction owner per local SQLite database. Object hashing/copying stays with bounded byte workers; reads use appropriately fresh views. | A single owner is an experiment for the already-serialized persistence boundary, not a requirement to serialize the metadata engine. Lock scope follows invariants; holding a global mount mutex across copying/fsync is not the same design. A queue alone does not remove the bottleneck. |
| [Performance](https://github.com/tigerbeetle/tigerbeetle/blob/0450c32b2c3c3ab6114f3402cc601341cc8192aa/docs/concepts/performance.md) and replica pipeline: batches amortize fixed work, with smaller batches at low load. | Batch independent admitted metadata commits and feed processing using event-count, byte and time budgets. Reuse one prepared causal context for compatible events. Reuse prepared SQL statements and avoid per-event status rebuilds. | Never wait for a full batch during an interactive operation. Preserve dependency order, operation results, flush barriers and atomic namespace operations. Transport batching alone does not amortize one SQLite durable commit per event. |
| [Replica counters](https://github.com/tigerbeetle/tigerbeetle/blob/0450c32b2c3c3ab6114f3402cc601341cc8192aa/src/vsr/replica.zig): distinguishes observed committed progress from locally executed progress and pipeline occupancy. | Expose received, validated, activated and acknowledged progress separately from object bytes. Record the reason, owner and age of each waiting stage. | Do not copy TigerBeetle's global sequence or watermark acknowledgements: TKFS feed cursors are source/epoch scoped, and acknowledgements require bound committed-event evidence. |
| [Message pool](https://github.com/tigerbeetle/tigerbeetle/blob/0450c32b2c3c3ab6114f3402cc601341cc8192aa/src/message_pool.zig): capacity accounts for journal I/O, pipeline messages, replies and connection queues, including progress headroom. | Size end-to-end budgets and reserve resources for metadata replies, parent repair, cancellation and admission notifications on both network and worker IPC. Observe queue occupancy and oldest age. | Bounded queues can deadlock if every slot is held by work waiting for a reply that also needs a slot. Reserve completion capacity, not just separate socket lanes. Do not copy fixed financial-record layouts for unbounded filenames/events. |
| [Repair budget](https://github.com/tigerbeetle/tigerbeetle/blob/0450c32b2c3c3ab6114f3402cc601341cc8192aa/src/vsr/repair_budget.zig): per-replica inflight limits, measured repair latency, expiry and occasional alternative-source exploration. | Missing metadata parents and object demands choose eligible sources with bounded budgets. Use source-specific evidence and recover permits after timeout/session loss. Demand can prefer a responsive source without waiting for an unrelated source's backoff. | A peer may not possess or be authorized to serve the requested event/object. No broadcast retry storm; do not transplant the 500 ms expiry cap to WAN/large-object transfers. |
| [State sync](https://github.com/tigerbeetle/tigerbeetle/blob/0450c32b2c3c3ab6114f3402cc601341cc8192aa/docs/internals/sync.md): installs checkpoint metadata, fetches referenced data lazily, persists incomplete repair state, and ratchets repair ranges. | Maintain separate metadata readiness and content availability. Persist missing work and its admission generation; prioritize blocks/objects needed by reads. New metadata generations must not continually abandon still-needed old repairs. | A TigerBeetle checkpoint comes from a shared authoritative history. TKFS must preserve unsent local events, causal ancestry, conflicts and provenance. Never replace an offline branch with a peer's newer-looking snapshot or mark a hydration job complete merely because its root was installed. |
| [Sync stages](https://github.com/tigerbeetle/tigerbeetle/blob/0450c32b2c3c3ab6114f3402cc601341cc8192aa/src/vsr/sync.zig): explicit valid transitions; cancellation waits for uninterruptible phases and outstanding I/O. | Admission reconciliation/snapshot changes have explicit phases: fence new affected work, drain or cancel safely, install durable generation, reconcile, resume. Tag completions with generation/operation identity. | Late completions must not publish obsolete authority or discard a locally acknowledged save. A mount request deadline does not imply a mutation never happened. |
| [Journal](https://github.com/tigerbeetle/tigerbeetle/blob/0450c32b2c3c3ab6114f3402cc601341cc8192aa/src/vsr/journal.zig) and [repair protocol](https://github.com/tigerbeetle/tigerbeetle/blob/0450c32b2c3c3ab6114f3402cc601341cc8192aa/docs/internals/vsr.md): missing/unwritten and previously written-but-corrupt entries have different meaning. | Distinguish unseen metadata, missing parents, corrupt persisted data, unsupported input and denied sharing. Repair verified immutable records from authorized peers while retaining evidence. | A checksum failure is not proof that a committed event never existed. Never truncate history, advance acknowledgements or discard offline edits merely to make a gap disappear. SQLite remains the persistence engine; no custom WAL is proposed here. |
| [Client sessions](https://github.com/tigerbeetle/tigerbeetle/blob/0450c32b2c3c3ab6114f3402cc601341cc8192aa/docs/reference/sessions.md): stable request identity and explicit uncertainty after lost replies; persistent event IDs outlive sessions. | Retries retain event identity and payload digest; duplicate application returns the prior outcome; changed payload under the same identity is rejected. Keep durable inbox/outbox progress across reconnects. | TKFS already has event identity checks and request binding. Strengthen their fault tests; do not recreate them. TigerBeetle's indefinite client retry policy cannot become an indefinitely blocked filesystem callback. |
| [Checksums](https://github.com/tigerbeetle/tigerbeetle/blob/0450c32b2c3c3ab6114f3402cc601341cc8192aa/src/vsr/checksum.zig): hardware-accelerated AEGIS-based 128-bit checksums, chained records and checked blocks. | Measure bytes hashed, redundant passes and digest reuse; consider separately versioned transport/checkpoint integrity evidence only for an identified gap. | Its fixed-zero-key checksum detects corruption; it does not authenticate a peer. Do not replace SHA-256 CAS identity or alter immutable event formats as a performance shortcut. A local source-feed chain is not proof of a global order or permission. |
| [VOPR](https://github.com/tigerbeetle/tigerbeetle/blob/0450c32b2c3c3ab6114f3402cc601341cc8192aa/docs/internals/vopr.md), [simulated I/O](https://github.com/tigerbeetle/tigerbeetle/blob/0450c32b2c3c3ab6114f3402cc601341cc8192aa/src/testing/io.zig), [packet simulator](https://github.com/tigerbeetle/tigerbeetle/blob/0450c32b2c3c3ab6114f3402cc601341cc8192aa/src/testing/packet_simulator.zig): seeded production-state-machine execution with controlled clocks/network/storage faults. | Extend the current oracle and FaultProxy tests with a deterministic metadata/admission scheduler. Reproduce failure seeds and include convergence and liveness checks after faults stop. | TKFS replicas need logically equivalent authorized event sets/projections, not byte-identical SQLite files. A simulated store does not replace real SQLite WAL, Windows rename/fsync and WinFsp integration tests. |

TigerBeetle also deliberately introduces bounded commit delays to keep lagging
replicas closer (`commit_stall_*` in `replica.zig`). That is a policy for its
replicated durability/availability model, not a reason to slow a TKFS local
build whenever an offline peer lags. Offline backlog must not become a hidden
global write gate. If local durable storage capacity is exhausted, report
that actual resource limit explicitly.

## 3. A concrete metadata pipeline

Retain the existing feed and immutable events. The proposed lifecycle is:

1. **Admit:** validate bounds, repository/epoch, origin and payload identity;
   durably stage inbound events with unresolved dependencies identified.
2. **Prepare:** select a bounded set of causally ready events; prefetch needed
   metadata and compute affected conflict/namespace changes outside the shared
   mutation critical section. Remote content is not an input dependency.
3. **Commit:** revalidate relevant generations, apply deterministic changes in
   a short local transaction, record per-event outcomes and durable object
   admission changes. Unrelated branch movement should not force a full redo.
4. **Publish:** expose the committed view, wake object/read owners and emit
   acknowledgements derived from committed-event evidence. Recover publication
   from durable state if notifications were dropped or the process crashed.

Batching must define partial failure semantics: for example, isolate an invalid
event and descendants that require it while continuing independent ready
events, using the existing inbox/quarantine semantics. Do not acknowledge a
whole batch because its valid prefix committed. Never collapse causally
distinct immutable events into one new event to save work.

For a local save, byte durability remains a separate prerequisite completed
outside the metadata critical section. The local metadata commit applies the
event without waiting for any peer. Other peers may later contribute concurrent
events; deterministic reconciliation remains responsible for the merged view.

Transport prefetching/pipelining can proceed concurrently with ordered local
mutation. This is independent of choosing Tokio: TigerBeetle itself demonstrates
that high I/O concurrency need not mean parallel mutation of the same state.
For TKFS, a bounded writer owner may reduce SQLite contention, but must be
benchmarked against the current implementation. Immutable read views need an
explicit generation/read-your-writes contract and bounded retention.

### Key contention to the protected invariant

User clarification: do not serialize operations that can safely proceed in
parallel. The design must inventory each shared invariant and assign the
smallest sufficient coordination scope. Candidate scopes to validate:

| Resource/invariant | Candidate coordination key | Concurrent work to preserve |
|---|---|---|
| Handle cursor, cancellation, outstanding read | Handle/session identity | Other handles, especially reads of immutable bytes. |
| Dirty content revision and save completion | Repository + branch + stable entity ID | Writes to other files; off-lock hashing/copying of immutable save snapshots. |
| Directory membership and name uniqueness | Repository + branch + parent ID + normalized name; directory generations for enumeration | Independent names where the directory representation permits it, and unrelated directories. |
| Rename/move/delete touching multiple entries | Explicit affected entity and source/destination namespace keys | Unrelated namespace mutations. Acquire multiple keys in a canonical order and revalidate ancestry/cycle constraints. |
| Installing/verifying an immutable object | Actual CAS/storage namespace + hash | Other hashes; shared reads of the same verified installed bytes. Include the correct scope if storage is shared across repositories. |
| Conflict preparation and derived metadata | Affected causal cones/registers plus validated generations | Preparation for unrelated cones; never substitute filename-only locking for causal correctness. |
| Durable SQLite write transaction | One metadata database | Parallel reads where WAL permits, preparation, other databases and object I/O. |

These are candidate lock domains, not a ready-to-implement lock table. Directory
enumeration consistency, ancestor changes, cross-directory rename, branch
checkout and reference revocation need explicit linearization points. Use
optimistic generation checks where they avoid large lock sets; test retries
under sustained edits. Lifecycle operations may need a broader short fence,
but ordinary filesystem callbacks must not inherit that fence's scope.

SQLite WAL supports concurrent readers and a writer, but only **one writer per
database** ([SQLite WAL documentation](https://www.sqlite.org/wal.html)). More
connections do not change that limit; long read snapshots can also interfere
with checkpoint progress. Separate the measurements:

- engine/entity/namespace lock wait and hold time, labeled by operation;
- SQLite writer-acquisition wait, transaction duration, busy/snapshot errors,
  commit latency, WAL growth and checkpoint progress;
- hashing/copy/fsync time and queueing outside metadata transactions.

Current evidence demonstrates engine contention and a plausible lost-index
hazard. It does **not** quantify SQLite's contribution to build latency. Report
SQLite as a measured bottleneck if short transactions still saturate the writer
or its wait time dominates after unnecessary global locking and work have been
removed. First assess transaction amortization and indexing. Splitting databases
or changing storage engines requires a separate design for atomic rename,
cross-entity invariants, recovery and migration; do not recommend it solely
because SQLite has one writer.

## 4. Proposed extensions to Wave 6

- **WP6.0:** record phase duration, queue occupancy, batch fill/age, causal-ready
  versus waiting-parent counts, stale-preparation retries and per-source repair
  progress. Connect these with build callback and engine-lock measurements.
- **WP6.1 / WP5.1:** specify durable admission, invariant-scoped coordination,
  concurrent bounded preparation, partial batch outcomes and recovery. Compare
  keyed locking and a transaction owner at the SQLite boundary. Preserve current
  oracle, source-bound acknowledgements and private-CAS invariants.
- **WP6.2:** introduce per-source repair budgets with reserved demand capacity
  and generation-aware continuation of still-needed work. Never make metadata
  activation wait for file content.
- **WP6.4:** compare batching/ownership improvements before and after a runtime
  prototype. Measure interactive p99 and whole-build wall time, not just events
  per second. Small/idle requests must not wait for batch occupancy.
- **WP6.INT:** deterministic fault scenarios plus real store/mount tests below.

Required scenarios:

1. Partition two peers; both durably edit overlapping and independent names;
   crash/restart either; reconnect with duplicates and reordered messages.
   Preserve all acknowledged local events and converge to the oracle view.
2. Lose a commit reply; reconnect under a new transport session and retry the
   same event. Apply it once; reject a changed payload under the same ID.
3. Exhaust bulk buffers and saturate repair requests while a metadata reply,
   a missing parent and a foreground read arrive. Reserved capacity permits
   forward progress; memory and outstanding work remain bounded.
4. Keep receiving newer generations during hydration. Still-authorized demand
   finishes; withdrawn/private references do not receive stale grants; restart
   resumes precisely the necessary durable work.
5. Corrupt a persisted event/checkpoint component. Do not silently truncate or
   over-ack. Repair only with matching verified evidence; if no copy remains,
   expose the unrecoverable condition instead of inventing a consistent state.
6. Delay/reorder disk completions across snapshot install and cancellation.
   Old completions cannot overwrite new admission state or erase offline work.
7. Batch independent events with a malformed event and a missing-parent event.
   Independent commits progress; failures retain their individual outcomes.
8. Run equivalent clean and incremental builds with native storage, mounted
   local activity and mounted activity plus sync. Record phase costs and
   repeatability. Do not use TigerBeetle's financial throughput figures as a
   forecast for filesystem performance.

The recommendation is to borrow **short owned mutation phases, bounded
pipelines, useful batching, explicit progress, repair discipline and fault
testing**. Adopting TigerBeetle as TKFS's metadata database, introducing quorum
requirements for local edits, replacing SQLite with its LSM engine, or changing
content hashes is outside this proposal.
