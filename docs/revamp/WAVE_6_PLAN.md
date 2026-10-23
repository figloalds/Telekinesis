# Wave 6: chunked content, independent engines and fast local builds

Superseded, 2026-10-23: the scope below is replaced by
[WAVE_6_REPLACEMENT.md](WAVE_6_REPLACEMENT.md) (passthrough working layer,
seal on close, pack store, per-entity conflicts, one benchmark gate). This
file is kept as the historical record of the chunked-journal design and its
evidence; do not continue its packages.

Status: implementation in progress, 2026-10-23. Default current-worker
initialization and fatal retention pass scoped regressions; single-path
consolidation and release gates remain open. Typed Core producer/notification
checks pass in the recorded scopes. Legacy descriptor/provider conversion is
removed with scoped current-content qualification. Current Saved lifecycle and
heartbeat checks also pass; earlier API/transport failures and pending route
removal remain recorded. No complete current-path qualification is claimed. No release/deployment
has been performed. See the [implementation record](notes/wp6-progress.md) for completed work,
verification and outstanding gates. The package and release requirements below
remain the complete Wave 6 scope.

Scope revision, 2026-10-21: the user confirmed this is greenfield. Support for
prior releases, stores, journals and peers is no longer required. WP6.G below
is the current implementation phase, inserted after CCR-WP6.4-30 and before the
remaining Wave 6 work. This revision supersedes earlier compatibility and
migration obligations in contracts, progress notes and validation guidance;
their recorded implementation and test results remain historical evidence.

Transport security boundary, 2026-10-22: the user selected standard TLS/WSS or
the enclosing VPN to secure the channel. Reuse those mechanisms; do not add
custom channel cryptography, security negotiation or another transport-security
layer. Keep implementation within this boundary when consolidating transport.
Simplify the existing custom HMAC handshake/enrollment wrapping in that work;
do not preserve those mechanisms merely as historical compatibility. Ordinary
application authority remains scoped by the decision below.

Personal-use authority scope, 2026-10-22: [deployment assumptions and DVFS
boundaries](AUTHORITY_SCOPE.md) govern security-related requirements throughout
this plan and inherited documents. Rely on deployment-provided protection;
implement only DVFS pairing/routing, repository/reference scope and filesystem
correctness. Users, roles, ACLs, multi-tenancy and hostile-user defenses are out
of scope. The selected simple hashed pairing-code model does not require custom
HMAC/enrollment infrastructure. Existing authorization/permission requirements
refer to DVFS authority boundaries, not a new user-permission system. This adds
no security phase or gate before completing current implementation.

Pairing clarification: the server may be online. The hashed pairing-code
challenge remains mandatory admission control against unwanted use, even over
TLS/WSS or VPN. Peers that pass pairing are trusted; checks after admission
enforce DVFS routing/reference authority and filesystem correctness, without
assuming malicious authenticated users or adding Users/ACLs.

Baseline: integrated local `master` at `b1b8e52`; see the
[Wave 5 handoff](notes/wave-5-handoff.md). The user selected one next wave
covering the 64 KiB foundation, independent metadata/object engines,
Git-ready seams and build performance. Actual Git integration stays deferred.

This is the authoritative Wave 6 scope and package numbering. It supersedes
the exploratory WP6.0–WP6.4 tables in the earlier studies. The original
[revamp plan](REVAMP_PLAN.md) remains the historical Waves 0–5 specification;
its invariants apply except where an explicit change below requires new
contracts and correctness evidence. This document authorizes planning scope,
not a claim that implementation, qualification or deployment has occurred.

## 1. Deliverable and boundaries

Ship a VFS in which local builds use short metadata operations and bounded
local byte work, metadata continues converging while content is unavailable,
and large-file revisions reuse unchanged 64 KiB chunks. Foreground reads can
obtain independently verified ranges ahead of background replication. Durable
recovery, permissions and offline conflict preservation remain requirements.

In scope:

- Versioned file-content descriptors, fixed 65,536-byte chunks, bounded indexed
  manifests and direct small-file objects on a single current content path.
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

Removed from this release scope: prior-format reads/writes, legacy promotion,
old-store or private-journal upgrades, mixed-version interoperability, old-reader
qualification, previous-executable launcher fences and downgrade/export-to-old-
version workflows. These are not deferred release gates. Fresh stores and peers
running the selected current format/protocol are the supported baseline.

The runtime stays `std`/`polling` for this wave unless measured evidence shows
it prevents a required gate. Record that evidence and a scoped ADR before any
runtime change; a runtime comparison is no longer a mandatory side project.

## 2. Decisions and contracts to freeze in WP6.0

The selected default is **64 KiB**, with files at or below 64 KiB represented
directly and a short final chunk stored without padding. Compare 16/256 KiB
in experiments to explain tradeoffs, not to silently alter this selected scope.
All representation parameters are immutable/versioned. Empty content keeps
one explicit canonical representation. WP6.G removes legacy descriptor handling
and freezes the current-only contract. A manifest digest is never presented as
the SHA-256 of file bytes.

WP6.0 produces a contract document and golden fixtures before consumers land.
The following names describe required contracts, not existing Rust APIs:

| Contract | Required information / behavior |
|---|---|
| `FileContentRef` | Explicit direct/manifest kind, format and digest algorithm, identity, total length; legacy tags and untyped hash strings are unsupported after WP6.G. |
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

Do not require a whole-file digest on every chunked save. File revision identity,
physical byte layout and Git identity remain distinct. Fixed chunks do not guarantee small deltas after
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
Fresh files enter the current direct/manifest path from creation; no legacy
conversion stage is required or supported.

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
Use a single integration line. Preserve completed work through CCR-WP6.4-30,
execute WP6.G next, then finish remaining 6.1–6.4 obligations and proceed to
6.5, 6.6 and 6.INT. Read-only measurement can continue throughout. Production
release stays gated on 6.6 and 6.INT; WP6.G enables the current path for fresh
development stores. Existing source paths below are ownership starting points;
WP6.0 records exact file ownership after carving new modules.

| Package | Dependencies | Deliverable | Principal source seams | Exit evidence |
|---|---|---|---|---|
| WP6.0 Baseline and contracts | Wave 5 measurement reference | Reproducible native/mounted build harness; phase counters; frozen format, API and resource contracts; golden fixtures | `src/model/`, `src/util/`, integration-owned `src/core.rs`, qualification scripts | Release baseline, callback/durability map, format vectors and ownership map; no placeholders for correctness-critical decisions |
| WP6.1 Content representation and lifetimes | 6.0 | 64 KiB CAS, direct-file path, indexed manifests, provider and root-handle interfaces, pins and reachability accounting | New content/manifest/provider modules, `src/core/objects.rs` | Current-format reads, malformed manifest limits, verified ranges and shared-content isolation; mock provider with delayed/failing reads |
| WP6.2 Metadata engine and admission | 6.0, 6.1 | Durable root outbox/inbox replay, content-independent activation, scoped coordination, incremental view publication | `src/core/{commit,inbox,derive,sharing}.rs`, `src/worker/activator.rs`, `src/orchestrator/meta_exchange.rs`, `src/runtime/sync_rpc.rs` | Offline convergence oracle with no bytes present; lost-notification/index-failure recovery; independent metadata progresses during object stalls |
| WP6.3 Object engine and demand | 6.1, 6.2 | Bounded manifest traversal, descendant authorization, cross-repository demand, joined verification, source retry and generation leases | `src/core/{transfer,demand}.rs`, `src/orchestrator/{objects,inbound_objects,scheduler}.rs`, `src/runtime/objects_rpc.rs`, `src/worker/maintenance.rs` | Demand under saturation, authorized partial hydration, independent cancellation, source loss and stale completion tests |
| WP6.4 Local COW reads/saves and durability | 6.1–6.3 | Dirty-range sessions, recovery checkpoints, durable close/flush, chunk-local finalization, provider-independent mounted reads | `src/runtime/{engine,content}.rs`, `src/mount.rs`, `src/fuse_linux.rs` | Crash/restart save matrix, no full-base staging for a small edit to chunked content, bounded resources, real Windows/FUSE adapter tests |
| WP6.G Greenfield interlude | Implemented work through CCR-WP6.4-30; before remaining implementation | Remove legacy content, fallback transport/save paths and compatibility scaffolding; make fresh stores use one current path | Model/codec/schema, startup, Core, runtime, object RPC/dispatch, WinFsp/FUSE, fixtures and harnesses | Section 3.1 removal inventory, current-only contracts, fresh-store native/peer/restart checks and no reachable legacy fallback |
| WP6.5 Build performance and change journal | 6.G, 6.2, 6.4 | Correct cache invalidation, resumable local/remote changes, view freshness fixes, transaction/notification batching and build tuning | New change-journal/cache modules; owned adapter hooks from 6.4; `src/runtime/published.rs`, `src/core/status.rs` | Real builds meet section 5; remote changes visible through cached views; journal overflow/restart safely requests rescan |
| WP6.6 Fresh-store readiness, retention and status | 6.G, 6.1–6.5 | Current-format startup/recovery, protocol validation, retention audit/reclamation, meaningful per-engine state, bounded error episodes | Retention modules, `src/startup.rs`, `src/worker/status_cell.rs`, `src/model/{status,health,errors}.rs` via integrator | Fresh-store initialization and same-format recovery, unsupported-format refusal, GC/pin race tests, no retry spam or unrelated breaker trips |
| WP6.INT Integration | All | Release gates, fresh-store release rehearsal, evidence, docs and handoff | Integration-owned shared files, contract fixtures and validation docs | All correctness gates plus matched build evidence; deviations explicit, actual Git integration absent |

The integrator owns shared contract edits, `src/core.rs`, protocol/wire
definitions, schema migrations, module registries and dependency files. Packages
request changes through a CCR rather than editing another package's contract.
Because this wave intentionally changes content representation, WP6.0 replaces
the old frozen interfaces with versioned seams; it must not silently retrofit
new meanings into existing fields. Ownership may transfer between sequential
packages only when recorded in the integration notes.

### 3.1 WP6.G — greenfield interlude (in progress)

Purpose: stop extending two implementations to support unreleased history.
Complete this interlude before more legacy promotion, compatibility work or
performance tuning. Preserve the qualified current-path behavior and simplify
its dependencies. The [removal/replacement inventory](notes/wp6-greenfield-inventory.md)
records landed changes and pending routes; partial consolidation does not close
the interlude. CCR-WP6.G-2 removes the private-journal upgrade chain and keeps
one selected current layout. CCR-WP6.G-3 consolidates Core metadata initialization
and current derived-index repair, with fresh/current-store qualification recorded.
CCR-WP6.G-4 consolidates and qualifies the separate current catalog, including
rebased current restart, credential rotation and enrollment coverage. Descriptor,
execution and transport consolidation remain pending.
CCR-WP6.G-5 qualifies native file unlink and durable per-handle dispositions,
including cancellation and restart after persistent WinFsp Cleanup failure.
That scoped qualification used fresh metadata layout 2, with no prior-layout
reader or upgrade. The scoped proof passes 512 Windows/520 Linux ordinary cases
and seven actual kernel parents per platform. Complete directory/lifetime work,
default typed execution and the other greenfield removals remain open.
CCR-WP6.G-6 qualifies directory creation outside the Engine lock and the native
WinFsp operation-guard replacement. Its scoped proof passes 522 Windows/530 Linux
ordinary cases and nine actual kernel parents per platform. It did not qualify
directory rename/delete/basic metadata or dispositions; the separate Windows
timestamp probe still fails.
CCR-WP6.G-7 qualifies the scoped directory mutations, durable dispositions and
retained-handle cases in
[native-directory-metadata-checks.json](../../test-evidence/wave-6/native-directory-metadata-checks.json).
Its 429-input source passes 539 Windows/548 Linux ordinary cases across 12
targets/platform and eleven actual kernel parents/platform. The selected fresh
metadata layout is 3 (114 objects), without a prior-layout reader or upgrade.
CCR-WP6.G-8 removes custom channel proofs and enrollment wrapping under the
selected TLS/WSS/VPN boundary, with scoped pairing/session/lifecycle evidence
on both platforms. Its broader Windows receipt/readiness diagnostic remains
failed and unwaived. Full remote ordering, deadline/lost-observation lifetimes,
default execution, transport consolidation and every remaining Wave 6 gate are
still open.
CCR-WP6.G-9 qualifies retained directory mutation/Cleanup reconciliation after
a committed removal loses observation to a deadline (184 staged ordinary cases
per platform). Read-only enumeration and full remote lifetimes remain open;
the passing rerun does not waive the earlier Windows receipt-counter failure.
CCR-WP6.G-10 closes the scoped retained-enumeration observation gap using the
same receipt. It passes 78 ordinary cases and two actual WinFsp/FUSE directory
parents per platform. Full remote/concurrent-parent lifetime work, default
current execution and the remaining greenfield inventory stay open.
CCR-WP6.G-11 starts default worker initialization without fixture gates.
Its candidate passes the three new ordinary startup/recovery cases on both
systems and one actual FUSE mount, but exposes fatal-retention and existing
producer/fixture integration failures. It remains failed/unqualified; see the
implementation record before treating the default path as ready.
CCR-WP6.G-12 closes scoped default-worker fatal-retention/integration defects
with 288 ordinary, one exact unit and three actual kernel parents per platform,
recorded with exact source/image cohorts. CCR-WP6.G-13 now consolidates Core
producers and off-lock import preparation. Nine complete targets pass 275 cases
per platform at its producer cohort; broader ordinary diagnostics remain 318
pass / 36 fail. The subsequent typed-notification fix passes its focused 13
cases and an actual default-kernel parent per platform. Source cohorts and
remaining API/transport/consumer qualification are recorded explicitly; these
are not a complete matrix or interlude closure.

G14 removes legacy descriptor codecs/provider conversion and qualifies its
current-content scope. G15 removes the unused standalone poison hook, moves the
retained lifecycle obligations to current native saves and fixes heartbeat
accounting for durable Saved revisions. Its scoped 305 cases/platform and actual
default-kernel parent do not remove the remaining Engine/staging/adapter/table,
string consumer and duplicate transport routes in the inventory.
G16 closes the direct-mount initialization gap and removes the WinFsp C bridge's
optional native-content mode. Its single runtime cohort passes 209 Windows/215
Linux cases and both RPC/direct actual-kernel parents per platform. This is a
prerequisite for further callback/staging removal, not completion of that removal.

G17 removes whole-file handle hydration, CAS reference staging and writable-copy
conversion from both adapters. Current range/progress/capacity/activation cases
are rebased, and native mutation completion releases a premature dirty marker
when no bytes changed. Scoped qualification passes 235 Windows/241 Linux cases
and both explicit actual-kernel parents/platform. Standalone memory/disk saves,
tables, string consumers and duplicate transport remain pending; broader raw-API
diagnostics are recorded separately and are not passing targets.

G18 closes Core's string-valued metadata/projection acceptance and removes the
regular-file size fallback. Public nonlocal commit, bundle reception and feed
precheck validate descriptors before old byte/provenance work. Bulk fixtures now
use public typed Core commits instead of fabricating rows. Its one final cohort
passes 282 scoped cases/platform and both explicit actual-kernel parents/platform,
including all 45 feed cases and current-store checks. This does not consolidate
the remaining null-field status/preview/demand consumers, standalone execution,
old file-object tables or duplicate transfer routes.

G19 moves status and folder-details to current descriptors and verified chunk/
manifest indexes. Missing, pending indexing and known corruption have distinct
states; logical missing bytes include repeated chunks and missing page coverage.
Shared completeness keeps receipt provenance separate from private deduplication.
Folder-details reads its secondary SQLite snapshot outside Engine and rejects
checkout races. Bounded local maintenance verifies installed data without peers.
Its final cohort passes 319 scoped ordinary cases and both explicit actual-kernel
parents per platform. Preview/demand APIs, old tables, standalone saves and the
remaining Wave 6 integration/performance gates are still open.

G20 replaces the file-preview and explicit-request hash consumers. GUI conflict
previews resolve the registered event/entity reference and read only a verified
64 KiB prefix. Current range demands hydrate absent shared data; explicit bounded
prefetches join/cancel by their own owner and keep provider pins. Byte work and
waits are outside Engine/SQL transactions, with view/admission rechecks, session
deadlines/cancellation, reserved handler/read capacity and shutdown cleanup.
Five complete targets plus four exact real-peer cases pass 94 cases/platform;
this is not the full transport suite or a new kernel/release qualification.
Generic artifact fetching, old tables/transport, standalone execution and the
remaining Wave 6 gates stay open.

CCR-WP6.G-21: G21 removes standalone mutation fallback selection from the WinFsp/FUSE
callbacks. Mounted ordinary operations require the current service installed
before attachment and use its private byte/metadata/namespace owners. Root,
directories and discovery retain explicit virtual/metadata behavior. Closed and
readonly immutable rename sources now prepare a temporary current metadata owner
without hydrating file bytes. Five complete
targets pass 194 ordinary cases/platform; focused adapter coverage adds
1 Windows/10 Linux cases, with eight actual-kernel parents/platform.
This qualifies adapter dispatch, not deletion of shared savers, standalone APIs,
pending_saves, old tables/transport or remaining Wave 6 gates.

CCR-WP6.G-22: G22 removes the deferred-create API, its string-valued empty-file constructor,
parallel pending-name index, first-save creation, late name restoration and
TKFS_DEFERRED_CREATE. Current file names are canonical before the first byte
write; dirty sizes remain private COW observations. Rename also retains source
and destination operation handles through metadata gates, so the last user close
cannot evict a selected session. The old local test mechanisms
have an explicit replacement ledger: five ordinary current COW/range parents and
all eight retained actual-mounted scenarios, including 300-file copy and eight
writers x150 files with the original 5s per-operation/probe bounds. Eleven full
targets plus focused cases pass 361 Windows/370 Linux ordinary cases and sixteen
actual-kernel parents/platform. Remaining standalone staging/snapshot/pending
APIs/table and all broader Wave 6 gates remain open.

CCR-WP6.G-23 validates the private journal before pending directory recovery
can commit metadata. Unsupported private input preserves the pending disposition,
directory incarnation, projection and events through both RPC startup and direct
attachment. A fresh current test journal then recovers the same intent. Two full
targets pass 86 ordinary cases/platform, plus the actual RPC/direct WinFsp/FUSE
parents/platform. This closes the scoped unsupported-journal ordering gap; full
standalone removal and the remaining Wave 6 gates stay open.


1. **Inventory and freeze scope.** Trace production entry points, codecs, schema,
   workers and tests. Record each legacy-only component as remove, replace or
   retain with a current-path reason. Distinguish obsolete compatibility from
   required authorization, causal ordering, ownership and recovery mechanisms.
2. **Consolidate storage and contracts.** Keep typed direct/manifest content and
   one selected private-journal layout. Remove legacy whole-file descriptors,
   string-hash event fallbacks, conversion caches, promotion/import adapters,
   upgrade chains and old-reader support. Update schemas, version tags and golden
   fixtures together; fresh initialization must not traverse historical migrations.
   Direct small files remain supported, including their byte-digest verification.
3. **Consolidate execution.** Route fresh-store create/read/write/flush/close,
   rename/replacement and recovery through the current native COW path. Remove
   deferred legacy saves, whole-file staging and optional fallback selection.
   Remove legacy-only WANT/pull/push RPCs, capability fallbacks and duplicate
   schedulers after routing current needs through bounded typed object demand.
   Retain intended-peer/route binding under the selected pairing/deployment
   model, cancellation, generation ownership and fairness. Do not retain custom
   authentication/enrollment solely to satisfy historical security requirements.
   If a required ordinary operation has no current implementation, implement
   that replacement before removing its only working route; do not substitute
   a stub, disable the operation or silently fall back to legacy behavior.
4. **Rebase fixtures and acceptance.** Move applicable regression scenarios to
   fresh current-format stores; remove tests whose sole purpose is prior-version
   interoperability. Preserve historical evidence and known failures as such.
   Keep Wave 5 only as an isolated performance reference, without shipping its
   runtime paths. Record why each removed test is obsolete and where retained
   correctness coverage now lives; do not erase a still-relevant failing test.
5. **Qualify the single path.** Format/lint affected targets and run current-format
   codec/provider, admission/demand, namespace and save/restart regressions.
   Exercise fresh-store startup, actual WinFsp/FUSE create/read/write/fsync/close,
   rename/replacement and same-version two-peer partial hydration/reconnection.
   Verify unsupported formats/protocols fail clearly without mutation or fallback.
   Inventory reachable production routes again and attach source/image evidence.

Exit: the current path is the default for every fresh qualification store,
legacy runtime branches and compatibility gates are removed, and all retained
behavior has an owner and applicable tests. Document any remaining generic
infrastructure explicitly; a hidden legacy fallback does not close this phase.
Update contracts, architecture, validation and the progress record. Then resume
the remaining Wave 6 gates; do not restart already completed work wholesale.

This interlude does not waive the Windows timestamp-policy failure, structural
demand reliability, crash durability, permissions, GC races, resource budgets,
build targets or final platform/peer/outage/soak qualification. Namespace and
lifetime work still required by the single current path stays in scope.

## 4. Greenfield stores, retention and rollout

Support fresh stores, current-format restarts and peers/workers implementing
the selected current protocol. Fresh stores use direct/chunked descriptors
without an opt-in legacy/new-content switch after WP6.G. Keep explicit format
and protocol checks: unsupported input is refused with an actionable error,
without silent reinterpretation, destructive reset or acknowledgement advance.
There is no requirement that an old executable safely open a new store, or that
a new executable migrate an old store. Previously created development stores
are unsupported; use a fresh test directory rather than altering existing data.

Startup and recovery remain bounded and measured. Pins include current
revisions, retained history and conflict alternatives,
pending saves, retained snapshot handles and in-flight reads. Preserve existing
history retention rather than silently collecting older revisions. Reclamation
in this wave is conservative local cleanup of provably unreferenced temporary
and orphan content under a generation barrier. Unknown reachability retains data.
Deleting a reference is distinct from deleting bytes, and an offline peer's
absence is not a collection signal. Keep deduplicated ownership and permissions
separate. A reader cannot lose its pinned content during a GC pass.

Rollout stages: current-path fixtures -> fresh local store -> two current-version
peers -> mounted build qualification -> documented fresh-store release. Production
release remains gated on 6.6 and 6.INT; making the path default in development
does not deploy it. Existing stores and archived evidence are not deleted by
this scope change.

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
| W6-SAFETY | All current-format crash, authorization, oracle, fresh-store/restart and retention gates pass, plus applicable historical invariants/floors. Prior-version compatibility is excluded. No data-loss or permission exception is traded for a timing pass. |

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
11. Initialize a fresh store, restart during current-format recovery and GC,
    and reconnect a current-version peer. Prove retained data and acknowledgement
    correctness. Refuse unsupported store/journal/protocol versions without
    modifying their data; no old-executable or mixed-version rehearsal is required.

## 7. Verification, closure and retained knowledge

Use focused tests per package and run expensive integration checks once at
the final release gate. The user's earlier instruction to skip long tests was
for Wave 5 closure; this plan does not retroactively reinterpret it as new test
results or start long runs. No tests are launched by writing this document.

Rebase or extend applicable scenarios from `local_fs_throughput`, `availability_index`,
`objects_v3_worker`, `objects_v3_lanes`, `peer_supervisor`, `feed_v7`,
`replication_regressions`, `startup_snapshots`, `derive_oracle`,
`worker_feed_status` and `contracts` where ownership permits; legacy-only
targets may be retired under the WP6.G coverage inventory. Add
focused manifest/provider/save-recovery/change-journal/fresh-store tests instead
of duplicating the old assertions. Run deterministic interleaving/fault seeds
alongside real SQLite WAL and Windows/FUSE tests; simulated storage is not a
substitute for platform durability evidence.

Per package: format, compile/lint affected targets, focused correctness tests,
and matched measurements for changed hot paths. At integration: full applicable
Windows suite once, relevant WSL/FUSE coverage, current-version two-peer recovery,
the section 5 build matrix and a bounded 30-minute saturation/recovery run.
Retain the existing long qualification floors as outstanding until rerun or
explicitly waived; neither a short run nor Wave 5 evidence proves them for a
changed storage format. A waiver must identify the omitted gate and limitation.

Write evidence with source/executable identity, commands, configuration,
individual samples, fault seeds and pass/fail/skipped outcomes. Never turn an
unavailable environment or missing solution into a passing test. Update
ARCHITECTURE, VALIDATION, fresh-store setup guidance and the final handoff.
Do not alter live mounts or automatically reset existing repositories merely
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
