# Wave 6: chunked content, independent engines and fast local builds

Status: implementation in progress, 2026-10-21. Content activation remains off;
see the [implementation record](notes/wp6-progress.md) for completed work,
verification and outstanding gates. The package and release requirements below
remain the complete Wave 6 scope.
Baseline: integrated local `master` at `b1b8e52`; see the
[Wave 5 handoff](notes/wave-5-handoff.md). The user selected one next wave
covering the 64 KiB foundation, independent metadata/object engines,
Git-ready seams and build performance. Actual Git integration stays deferred.

This is the authoritative Wave 6 scope and package numbering. It supersedes
the exploratory WP6.0–WP6.4 tables in the earlier studies. The original
[revamp plan](REVAMP_PLAN.md) remains the historical Waves 0–5 specification;
its invariants apply except where an explicit change below requires new
contracts and compatibility evidence. This document authorizes planning scope,
not a claim that implementation, qualification or deployment has occurred.

## 1. Deliverable and boundaries

Ship a VFS in which local builds use short metadata operations and bounded
local byte work, metadata continues converging while content is unavailable,
and large-file revisions reuse unchanged 64 KiB chunks. Foreground reads can
obtain independently verified ranges ahead of background replication. Durable
recovery, permissions and offline conflict preservation remain requirements.

In scope:

- Versioned file-content descriptors, fixed 65,536-byte chunks, bounded indexed
  manifests, direct small-file objects and legacy whole-file reads.
- Independent metadata and object scheduling, durable admission/reconciliation,
  generation-owned work and fair DB/CPU/disk/memory budgets.
- Private dirty-range write sessions, recoverable saves, chunk verification,
  range reads, reference retention and conservative local reclamation.
- Correct local metadata caching/invalidation and a resumable workspace change
  journal; measured clean, incremental and no-op build performance.
- Generic immutable-content provider, workspace/file/generation identities,
  pin/lifetime contracts and a snapshot-root interface for future COW views.

Deferred:

- Git object provider, Git checkout/filter conversion, worktree command,
  fsmonitor adapter, Git semantics, and Git-to-TKFS mapping cache.
- A user-facing workspace clone command or persistent shared directory-tree
  implementation. Exercise content-sharing isolation through internal tests.
- CDC, syntax/line-aware chunking, physical pack files, delta compression,
  semantic chunk merging and rewriting all historical content.
- SQLite replacement, a custom WAL, new global consensus, distributed reference
  counting, or a mandatory Tokio migration. No global authority is introduced.

The runtime stays `std`/`polling` for this wave unless measured evidence shows
it prevents a required gate. Record that evidence and a scoped ADR before any
runtime change; a runtime comparison is no longer a mandatory side project.

## 2. Decisions and contracts to freeze in WP6.0

The selected default is **64 KiB**, with files at or below 64 KiB represented
directly and a short final chunk stored without padding. Compare 16/256 KiB
in experiments to explain tradeoffs, not to silently alter this selected scope.
All representation parameters are immutable/versioned. Empty content keeps
one explicit canonical representation. Existing whole-file identities remain
valid. A manifest digest is never presented as the SHA-256 of file bytes.

WP6.0 produces a contract document and golden fixtures before consumers land.
The following names describe required contracts, not existing Rust APIs:

| Contract | Required information / behavior |
|---|---|
| `FileContentRef` | Explicit legacy/direct/manifest kind, format and digest algorithm, identity, total length; no ambiguous interpretation of existing hash strings. |
| `ManifestNode` | Canonical encoding, ordered child/range references, lengths and coverage, bounded fanout/depth, typed leaf versus internal references. |
| `ContentProvider` | Immutable-version range read, availability, verification and lifetime/pin operations; bounded buffers, deadlines and cancellation. Local provider locators are not wire references. |
| `WriteSession` | Workspace/entity/base generation, dirty ranges, pending operation identity, retained base and durable recovery record. |
| `AdmissionChange` | Durable cursor, repository/branch/reference identity, authorized root, generation and grant/withdraw action; idempotent replay. |
| `Demand` | Referenced file revision and byte range, priority, independent waiter identity/deadline/cancel, eligible source scope. |
| `WorkspaceChange` | Workspace/epoch/sequence, affected entries/entities/directories and generation; resume or explicit rescan-required. |
| `SnapshotRootHandle` | Immutable selected view generation and lifetime; no host-path identity and no claim that current disposable startup snapshots are retained COW roots. |

Manifest plan: use immutable indexed pages from the first large-file format,
with no more than 256 child entries per node, a 64 KiB encoded-node cap and
depth at most 8. Validate u64 arithmetic, ordering, exact coverage and declared
length at every step. A small manifest is one root/leaf; large edits path-copy
only affected pages. WP6.0 freezes exact serialization, digest domain separation,
empty/tail/hole encoding, canonical tree shape and maximum supported file size.
Zero ranges may be explicit authenticated ranges; sparse support must never
turn missing bytes into zeros. Version 1 writes aligned fixed chunks, although
the provider/session interfaces can represent general ranges for future use.

Do not require a whole-file digest on every chunked save. Legacy materialization
may calculate it on demand. File revision identity, physical byte layout and
Git identity remain distinct. Fixed chunks do not guarantee small deltas after
text insertion or atomic editor replacement; measure those cases explicitly.

### Local save and durability contract

The plan deliberately closes Wave 5's burst-close recovery gap. A completed
successful close must leave recoverable bytes **and** the information needed
to recover the local mutation. This can be a durable private overlay plus
save intent; it need not already be a canonical published manifest. Ordinary
writes remain buffered until a durability boundary. Explicit flush/fsync waits
for a durable local file revision and the corresponding local metadata commit;
it never waits for a peer. On adapters without a fallible close callback, put
fallible work at the correct flush/cleanup boundary and retain/report failed
saves; never discard an error or claim durability that the adapter cannot give.
WP6.0 specifies the exact WinFsp and FUSE callback mapping before coding.

Required sequence:

1. Capture/pin base generation and dirty ranges under affected entity ownership.
2. Perform recovery checkpointing and chunk construction/install outside broad
   metadata locks. Bound producers and batch compatible durability work.
3. Revalidate the generation; atomically commit the file revision/event and
   durable admission work. A stale completion cannot replace a newer save.
4. Publish view changes, notify engines and retire recovery records only after
   the committed revision is recoverable. Replaying a lost reply is idempotent.

Expose local visibility, recoverable pending save, committed revision and peer
availability separately. Retain pending creates, names, deletes/renames and
ordering against later operations across restart, including the previously
fixed deferred-create collision. Shutdown/checkout fences must account for
durable but not yet canonical saves. No fsync weakening is a performance fix.
For an unchanged existing legacy base, its first conversion may scan the file;
do not claim chunk-local cost until that conversion is complete.

### Independent engine contract

Metadata owns causal validation, activation, conflicts, authorization and the
durable outbox of reference changes. It can activate an authorized remote
descriptor without its manifest or file bytes. Objects owns manifest/chunk
availability, discovery, verification, transfer, repair and demand scheduling.
The local workspace path owns mutable sessions and consumes published metadata
views. These are ownership boundaries, not a requirement for new processes.

Commit the outbox with metadata in one transaction. Object consumers persist
their own cursor and apply changes idempotently; notifications are disposable.
Reconcile committed roots and recoverable saves after lost notifications,
crashes and snapshot changes. Never expand millions of descendant references
inside a metadata commit. Traverse manifests incrementally with bounded work.

Admission proceeds root first: a committed shared reference admits its manifest
root, and verified manifest edges admit children for that reference. The sender
checks the requester's current authorization before serving bytes. A hash alone
never authorizes disclosure; private references and another peer's grants do
not leak through deduplication. Retraction is reference-scoped, so withdrawing
one of several roots does not drop another valid demand. Define the revocation
linearization point and recheck before issuing further transfer frames; bytes
already delivered cannot be recalled. Offline revocation is not globally instant.

Manifest payloads remain on object/bulk surfaces, using a reserved structural
priority within that engine. Control/metadata lanes carry bounded descriptors
and admission messages, not file bytes. Reserve capacity for metadata replies,
missing-parent repair, cancellation and completion on peer and loopback surfaces.
Missing manifest pages cannot create a circular activation/fetch dependency.

### Concurrency and resource contract

Use workspace/entity keys for mutation, parent/name keys for namespace entries,
and content identity for joined installation. Multi-entry operations acquire
keys consistently or prepare/revalidate optimistically. Ancestor/cycle checks,
causal dependencies and branch checkout require explicit wider invariants;
per-filename locking alone is insufficient. Reads use appropriately fresh
views. Never hold an engine-wide lock across hashing, copying, waits or fsync.

Keep SQLite initially. Short writes may use a bounded transaction owner per
database; unrelated preparation and reads remain concurrent. Instrument writer
wait separately from entity locks, SQL execution, fsync and checkpoints. Report
a measured writer bottleneck after batching/indexing rather than assuming that
adding connections creates parallel writers. Avoid long-lived WAL read snapshots.

Budget bytes as well as job counts. WP6.0 records explicit defaults for queued
dirty bytes, active save workers, verification buffers, manifest expansion,
SQLite queue depth, foreground demand and per-peer/per-repository background
work. Reserve forward-progress capacity and guarantee background progress under
continuous demand. No unbounded thread, connection or task fanout. An old run
can release only its own generation's leases.

## 3. Packages, dependencies and ownership

These packages are dependency units, not instructions to launch parallel agents.
Use a single integration line; merge in order 6.0, 6.1, 6.2, 6.3, 6.4, 6.5, 6.6,
6.INT. Read-only measurement can continue throughout. Content activation stays
off until 6.6 passes. Existing source paths below are ownership starting points;
WP6.0 records exact file ownership after carving new modules.

| Package | Dependencies | Deliverable | Principal source seams | Exit evidence |
|---|---|---|---|---|
| WP6.0 Baseline and contracts | Wave 5 | Reproducible native/mounted build harness; phase counters; frozen format, API, resource and migration contracts; golden fixtures | `src/model/`, `src/util/`, integration-owned `src/core.rs`, qualification scripts | Release baseline, callback/durability map, format vectors and ownership map; no placeholders for correctness-critical decisions |
| WP6.1 Content representation and lifetimes | 6.0 | 64 KiB CAS, direct-file path, indexed manifests, provider and root-handle interfaces, pins and reachability accounting | New content/manifest/provider modules, `src/core/objects.rs` | Legacy read compatibility, malformed manifest limits, verified ranges and shared-content isolation; mock provider with delayed/failing reads |
| WP6.2 Metadata engine and admission | 6.0, 6.1 | Durable root outbox/inbox replay, content-independent activation, scoped coordination, incremental view publication | `src/core/{commit,inbox,derive,sharing}.rs`, `src/worker/activator.rs`, `src/orchestrator/meta_exchange.rs`, `src/runtime/sync_rpc.rs` | Offline convergence oracle with no bytes present; lost-notification/index-failure recovery; independent metadata progresses during object stalls |
| WP6.3 Object engine and demand | 6.1, 6.2 | Bounded manifest traversal, descendant authorization, cross-repository demand, joined verification, source retry and generation leases | `src/core/{transfer,demand}.rs`, `src/orchestrator/{objects,inbound_objects,scheduler}.rs`, `src/runtime/objects_rpc.rs`, `src/worker/maintenance.rs` | Demand under saturation, authorized partial hydration, independent cancellation, source loss and stale completion tests |
| WP6.4 Local COW reads/saves and durability | 6.1–6.3 | Dirty-range sessions, recovery checkpoints, durable close/flush, chunk-local finalization, provider-independent mounted reads | `src/runtime/{engine,content}.rs`, `src/mount.rs`, `src/fuse_linux.rs` | Crash/restart save matrix, no full-base staging for a small edit to chunked content, bounded resources, real Windows/FUSE adapter tests |
| WP6.5 Build performance and change journal | 6.2, 6.4 | Correct cache invalidation, resumable local/remote changes, view freshness fixes, transaction/notification batching and build tuning | New change-journal/cache modules; owned adapter hooks from 6.4; `src/runtime/published.rs`, `src/core/status.rs` | Real builds meet section 5; remote changes visible through cached views; journal overflow/restart safely requests rescan |
| WP6.6 Compatibility, retention and status | 6.1–6.5 | Explicit feature activation, restartable migration, retention audit/reclamation, meaningful per-engine state, bounded error episodes | New migration/retention modules, `src/startup.rs`, `src/worker/status_cell.rs`, `src/model/{status,health,errors}.rs` via integrator | Mixed-version fixtures, recovery/downgrade rehearsal, GC/pin race tests, no retry spam or unrelated breaker trips |
| WP6.INT Integration | All | Release gates, migration rehearsal, evidence, docs and handoff | Integration-owned shared files, contract fixtures and validation docs | All correctness gates plus matched build evidence; deviations explicit, actual Git integration absent |

The integrator owns shared contract edits, `src/core.rs`, protocol/wire
definitions, schema migrations, module registries and dependency files. Packages
request changes through a CCR rather than editing another package's contract.
Because this wave intentionally changes content representation, WP6.0 replaces
the old frozen interfaces with versioned seams; it must not silently retrofit
new meanings into existing fields. Ownership may transfer between sequential
packages only when recorded in the integration notes.

## 4. Compatibility, retention and rollout

New binaries read existing whole-file events and objects unchanged. They write
legacy content until an explicit repository feature activation. Do not rewrite
historical causal events, their IDs, feed cursors or provenance. New writes after
activation use direct or chunked descriptors; old revisions remain readable.
Existing bytes can be reused while their new-format descriptor is computed.

The initial policy requires capable peers for the activated repository.
Negotiate descriptor/manifest capability on both worker IPC and peer discovery
before exchanging unsupported events. An old or disconnected unsupported peer
gets an actionable upgrade-required state for that repository; supported
repositories continue. Do not quarantine unsupported valid events as corrupt,
advance their acknowledgements, or synthesize downgraded events with new IDs.
Automatic legacy-peer materialization is deferred; explicit byte export is
separate from causal synchronization.

WP6.0 must prove the database-version fence with the actual previous executable.
A new marker alone cannot make an old binary obey a new rule. If the old binary
does not reject the upgraded store, require an isolated new store location and
a tested launcher/deployment fence before activation. No unsupported in-place
downgrade: rollback after activation uses a backup in a separate store and
preserves/exports all post-activation work before switching. Disabling new writes
does not make already-written descriptors readable by an old binary.

Startup migration is bounded, resumable and measured; no eager whole-repository
rehash. Pins include current revisions, retained history and conflict alternatives,
pending saves, retained snapshot handles and in-flight reads. Preserve existing
history retention rather than silently collecting older revisions. Reclamation
in this wave is conservative local cleanup of provably unreferenced temporary
and orphan content under a generation barrier. Unknown reachability retains data.
Deleting a reference is distinct from deleting bytes, and an offline peer's
absence is not a collection signal. Keep deduplicated ownership and permissions
separate. A reader cannot lose its pinned content during a GC pass.

Rollout stages: fixture-only readers -> opt-in local test store -> two capable
peers -> mounted build qualification -> documented explicit activation. No
live user workspace is migrated merely to run a benchmark.

## 5. Build-performance work and acceptance

WP6.0 captures the actual integrated Wave 5 behavior; the earlier slow deployed
build ran a different executable and is not this baseline. Use release binaries,
the same toolchain, source revision, configuration, parallelism, antivirus policy,
storage location/class and outputs. Record host, executable/source hashes,
configuration, workload and durability settings with every result.

Use a checked-in representative many-small-file solution harness and, when
available, a copy of the user's real solution. Keep outputs/intermediates inside
the mount for the primary comparison; external outputs are a separate diagnostic
experiment. Compare native, Wave 5 mounted, Wave 6 mounted with no peers, and
Wave 6 mounted with sync. Native close semantics may be weaker than the selected
TKFS recovery contract: disclose that and also compare explicit-flush workloads.
Do not disable durability, exclude ignored outputs or warm only the mounted case
to make numbers pass. If the real solution is unavailable, name that missing
evidence and do not claim its slowdown is resolved.

Workload matrix:

- Clean build; no-op build; one-source-file incremental build; rebuild after
  interruption; IDE/watch activity with build; timestamp-sensitive incremental
  rebuild after a remote change. Verify outputs and which targets rebuild.
- Warm runs (one warm-up, at least five measured repetitions) and controlled
  cold-cache runs (at least three, separate results). Record individual samples,
  medians, tail ranges and noise; do not label five-run maxima a p99.
- Tiny creates/reads/deletes, directory enumeration, stat/open hot paths,
  editor atomic replacement, beginning/middle insertion, append/truncate,
  boundary-crossing edits, and random reads of partially hydrated files.
- 4 KiB in-place overwrite of a prechunked 1 GiB file; cross-chunk overwrite;
  a 512 MiB background transfer plus two-way metadata edits and foreground
  reads; a 45-second outage and recovery. Include more than one repository.

Measure callback counts/latencies, bytes copied/hashed/read/written/transferred,
manifest page work, view reloads, cache invalidations/hits, CPU/thread count,
resident and queued bytes, lock wait/hold by operation, SQLite acquisition and
transaction/fsync/checkpoint time, queue age and actual demand-slot admission.
Separate source unavailability, network duration and scheduler delay. Observe
small-file and large-file costs independently; chunking is not a small-file fix.

### Release gates (targets, not measured results)

| ID | Gate |
|---|---|
| W6-BUILD | Warm median clean and one-file incremental builds <= 1.5x matched native; no-op <= max(1.5x native, native + 250 ms). In addition, no scenario regresses >10% against matched Wave 5 without an explicit measured durability explanation and reviewed plan adjustment. |
| W6-LOAD | Same build with bulk plus two-way metadata traffic <= 1.2x unloaded Wave 6 median; metadata activation p99 <= 2x matched unloaded and retain the applicable existing <50 ms filesystem callback gate under its original workload. |
| W6-DEMAND | Eligible foreground demand gets service capacity within 250 ms under the controlled saturated-load test, including competition from another repository. Measure queue admission separately from bytes becoming available; missing source/network time is not a scheduler pass or failure. Background makes sustained progress. |
| W6-AMPLIFICATION | An aligned contained 4 KiB overwrite of prechunked content creates at most one new 64 KiB data chunk plus O(tree depth) manifest pages; no whole-file hash/copy. A two-boundary-chunk case changes only the touched chunks. Instrument logical and physical amplification separately. |
| W6-READ | Once required manifest pages and chunks are verified, a range read completes while an unrelated file chunk remains unavailable; a corrupt demanded chunk is never served. |
| W6-SCALE | Cached lookup and unchanged-content bookkeeping do not scan total history/tree size; test fixed local work at 5K and 50K entries/events and explain any growth. Record cache misses separately. |
| W6-BOUNDS | All recorded queue/byte budgets enforced; cancellation/shutdown complete within documented deadlines; no sustained post-warm-up resource growth in the targeted final run. |
| W6-SAFETY | All new crash, authorization, oracle, migration and retention gates pass, plus applicable historical invariants/floors. No data-loss or permission exception is traded for a timing pass. |

WP6.0 may expose an unrealistic target on the reference workload. Preserve the
original result and propose a concrete revised target with attribution before
implementation proceeds against it; never silently relax a failing gate in the
final report. Passing synthetic throughput alone does not close W6-BUILD.

## 6. Required correctness and failure scenarios

1. Offline peers edit overlapping and independent files; reconnect with reorder,
   duplicate and missing-parent delivery. Preserve alternatives and converge to
   the causal oracle; content is never required for metadata activation.
2. Crash after recovery checkpoint, chunk install, metadata/outbox commit, cursor
   advance and notification; also before each boundary. Recover promised local
   changes exactly once, including creates, renames and competing same-file saves.
3. Lose availability-index writes and notifications, including real SQLite
   snapshot-upgrade contention. Repair from durable installed evidence; do not
   request a locally present object solely because its index update was lost.
   The original incident remains a hypothesis until reproduced in the Rust path.
4. Corrupt/truncate manifests or chunks; forge lengths, types, nesting and child
   hashes. Reject before exposing bytes; retain valid unrelated work. Never turn
   corruption into holes or silently truncate committed history.
5. Shared and private roots contain the same chunk; revoke one reference while
   another reader remains authorized. Request by bare hash, replay stale grants
   and deliver late lease releases. No disclosure or removal of newer ownership.
6. Saturate background transfers, buffers and DB work across repositories; issue
   metadata replies and foreground reads; cancel one of several joined readers;
   disconnect the source. Required work progresses and permits are reclaimed.
7. Full and partial overwrite of unhydrated base chunks, shrink/grow, holes,
   delete-on-close, atomic replace and retained open handles. Full overwrite
   avoids unnecessary hydration; partial overwrite preserves untouched bytes.
8. Clone an immutable content handle in an internal test, mutate either view and
   collect concurrently. Shared base stays unchanged and pinned. Exercise the
   same path with a fake provider; no actual Git process/provider is required.
9. Cache lookup/listing/data while remote updates, rename, delete, conflict
   resolution and foreign SQLite commits occur. Observe coherent changes;
   change-journal overflow/restart/epoch replacement triggers safe rescan.
10. Slow disk/fsync, ENOSPC, SQLite busy, unsupported peer and absent source have
    distinct bounded outcomes. Expected readiness/retry episodes do not flood
    fatal errors or trip unrelated engine breakers; actual corruption stays visible.
11. Restart during migration and GC, reopen with supported/old executables and
    resume a disconnected old peer. Prove feature fencing, retained data and
    acknowledgement correctness before enabling new-format writes.

## 7. Verification, closure and retained knowledge

Use focused tests per package and run expensive integration checks once at
the final release gate. The user's earlier instruction to skip long tests was
for Wave 5 closure; this plan does not retroactively reinterpret it as new test
results or start long runs. No tests are launched by writing this document.

Extend the existing `local_fs_throughput`, `availability_index`,
`objects_v3_worker`, `objects_v3_lanes`, `peer_supervisor`, `feed_v7`,
`replication_regressions`, `startup_snapshots`, `derive_oracle`,
`worker_feed_status` and `contracts` coverage where ownership permits. Add
focused manifest/provider/save-recovery/change-journal/migration tests instead
of duplicating the old assertions. Run deterministic interleaving/fault seeds
alongside real SQLite WAL and Windows/FUSE tests; simulated storage is not a
substitute for platform durability evidence.

Per package: format, compile/lint affected targets, focused correctness tests,
and matched measurements for changed hot paths. At integration: full applicable
Windows suite once, relevant WSL/FUSE coverage, two-peer compatibility/recovery,
the section 5 build matrix and a bounded 30-minute saturation/recovery run.
Retain the existing long qualification floors as outstanding until rerun or
explicitly waived; neither a short run nor Wave 5 evidence proves them for a
changed storage format. A waiver must identify the omitted gate and limitation.

Write evidence with source/executable identity, commands, configuration,
individual samples, fault seeds and pass/fail/skipped outcomes. Never turn an
unavailable environment or missing solution into a passing test. Update
ARCHITECTURE, VALIDATION, configuration/upgrade guidance and the final handoff.
Do not alter live mounts or automatically activate/migrate repositories merely
because code merged. Merge/push/deployment follow the user's later instruction.

Carried knowledge and owners:

| Existing item | Wave 6 handling |
|---|---|
| Wave 5 off-lock saves and verification reuse | Extend in 6.4; do not rebuild the old under-lock path. |
| Burst-close durability and same-file pending-save risk | Resolve with 6.0 callback contract and 6.4 crash tests. |
| CCR-WP5.2-3 overlapping-commit view freshness | Resolve in 6.2/6.5 before caching is enabled. |
| CCR-WP5.INT-1 demand behind other repositories | Resolve in 6.3 with measured admission. |
| CCR-WP5.INT-2 old-run lease release | Resolve in 6.3 with generation ownership. |
| CCR-WP5.4-5 stale peer status | Address in 6.6 status reconciliation. |
| CCR-WP5.1-2 conflict header cost; CCR-WP4.4-1 staging parse cost; O(history) paths | Attribute in 6.0; fix the measured path in 6.2/6.5 if it blocks gates, otherwise retain explicit continuation issues. |
| JuiceFS and TigerBeetle lessons | Preserve bounded ownership, batching, range reuse, repair and deterministic fault tests; do not import shared-authority semantics. |

Sources and rationale: [build incident / JuiceFS](notes/wp6-build-sync-juicefs-study.md),
[TigerBeetle / contention](notes/wp6-tigerbeetle-metadata-study.md),
[chunking / Git-ready seams](notes/chunked-cow-and-git-workspaces.md),
[Wave 5 evidence and limits](notes/wave-5-handoff.md).
