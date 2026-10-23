# Proposed Wave 6: independent metadata and object engines

Scope amendment, 2026-10-22: [personal-use deployment and DVFS authority
boundaries](../AUTHORITY_SCOPE.md) govern inherited security requirements.
References to security floors retain DVFS authority/correctness coverage, not
Users, ACLs, custom authentication or defenses supplied by the deployment.

Planning update: [WAVE_6_PLAN.md](../WAVE_6_PLAN.md) now owns the selected wave
scope, package numbering and gates. The package table here is historical
investigation material; its evidence and unresolved causal claims are retained.

Investigation: 2026-10-19. **Design proposal, not implemented or qualified.**
This records the mounted solution-build incident, a source study of JuiceFS,
and the user's direction: prioritize metadata convergence, give objects their
own robust engine, and admit urgent reads without coupling either engine's
overall progress to the other. Production code and the running installation
were not changed during this investigation.

**After Wave 5 integration:** the observed installation and inspected base
predate several relevant fixes. See the
[closure handoff](wave-5-handoff.md#what-the-live-build-investigation-does-and-does-not-describe)
for which findings Wave 5 addresses and which remain open. Keep the incident
as historical evidence; re-measure the integrated version before continuing.

The [TigerBeetle metadata supplement](wp6-tigerbeetle-metadata-study.md)
extends this proposal with offline-authority boundaries, bounded metadata
pipelines, batching, repair and deterministic simulation lessons.

## 1. Evidence and confidence

The installation under `C:\Users\felyp\Desktop\v` reported
`0.1.0+b14d7bd79566`. The Telekinesis source inspected was
`c6948c9569751fbb96d35dec8266bd4d8c57b06f`; the deployed revision was unavailable
in this checkout. Source findings below are therefore **candidate explanations
for the binary**, not proof of the exact deployed execution path.

Local diagnostics were captured at
`C:\Users\felyp\Desktop\v\data\diagnostics\20261019T045133.507Z`.
Activity, status, heartbeat, process lifetime, Windows events and read-only
SQLite queries were inspected. The checked-in
[evidence summary](../../../test-evidence/build-sync-20261019/summary.json)
omits peer addresses, full project paths, identifiers and object hashes.
The user supplied the association with a solution build; no build trace was
captured. The local diagnostic bundle is needed to repeat the raw-log analysis.

### Observed timeline

All times below are 2026-10-19, UTC-03:00.

| Time | Observation |
|---|---|
| 01:44:05.785–01:45:14.607 | 19 `NOT_PERMITTED` errors while **pulling** objects from the peer. Metadata exchanges continued during this period. |
| By 01:45:24.996 | All 19 errors were reported resolved. |
| 01:45:15.807, 01:45:32.075 | Activity recorded uploads of one and two objects respectively. |
| 01:46:32.946 | `stalled: no progress for 1 min`. |
| 01:49:00.764 | `stall cleared`; no intervening exchange/error entry in the captured activity. |
| Around 01:51:33 | Current content 9,697/9,697 available; shared content 9,935/9,935; pending saves, unflushed sessions, inbox counts, unacknowledged events and conflicts all zero. |

The stall indicator lasted **147.818 seconds**; clearance was 208.689 seconds
after the last logged upload. This is longer than the one-minute threshold
shown in its message. A later remote inventory observation can decrease the
estimated upload backlog and clear the indicator without a new upload entry.
The captured records do not establish whether that happened here.

The 19 hashes joined to **locally authored** metadata events, created only
127–870 ms before their download errors. They included `.suo`, `apphost.exe`,
NuGet caches and dependency specifications, and assembly/assets caches.
That makes “why did the author request its own just-installed bytes from a
peer?” a more useful first question than “why did the peer refuse them?”
No object error rows remained in the later database inspection.

The daemon and worker had been alive since the previous day. All four peer
lanes were Ready, with no incident-time reconnect indicated. The worker's
lifetime maximum engine-lock hold was 663 ms; its captured current hold and
waiters were zero. This weighs against a minutes-long **local engine-lock**
freeze. It does not exclude disk contention, a remote backlog, lease/backoff
waiting, or stale counters. An earlier lease lapse, pairing failures, and a
Windows ephemeral-port-exhaustion event occurred hours earlier; there is no
evidence connecting them to this interval.

### The stall flag is not a failsafe action

[`hub_rules::stall`](../../../src/orchestrator/hub_rules.rs) reports work
remaining without observed progress while the pair can progress. The hub
uses a threshold of at least 60 seconds (or 20 sync intervals). The flag
itself does not suspend transfers, restart the worker, or shut down lanes.
Object backoff and lease expiry are separate mechanisms and can delay work.
The investigation should not call all three mechanisms a single failsafe.

The current hub also resolves object errors after an idle/content-complete
grace period (`SOURCED_GRACE_MS`), not only after a successful download of the
failed hash. “Resolved” therefore does **not** establish that the refused
download subsequently succeeded. This matters when interpreting error spam.

### Ranked explanations and missing evidence

1. **Availability-index gap: concrete source hazard, incident causality
   unproven.** [`objects::note_available`](../../../src/core/objects.rs)
   discards failure of a savepoint that reads and updates
   `object_availability`. Object installation can succeed while that derived
   index update fails. [`demand::want_rows`](../../../src/core/demand.rs)
   trusts the index when excluding available, granted objects, without a
   physical-file fallback. A later local commit verifies the bytes and grants
   publication, but does not necessarily repair that index. A valid local
   object can consequently become a download candidate. Successful
   `open_object` verification itself does not record availability;
   `verify_existing` does, again best effort. Meanwhile
   [`status::scan_content`](../../../src/core/status.rs) can count physical
   files as available, so “complete” and a queued want can disagree.
2. **SQLite snapshot-upgrade contention is one plausible trigger.** A
   savepoint reads a WAL snapshot, another connection commits, then the first
   tries to write: `SQLITE_BUSY_SNAPSHOT` can occur despite a busy timeout.
   The [isolated SQLite probe](../../../test-evidence/build-sync-20261019/sqlite_snapshot_probe.py)
   models that interleaving and the lost-index consequence. It does not run
   TKFS, reproduce its exact call context, or prove that the incident took
   this path. An instrumented Rust regression must establish that next.
3. **Admission/activation lag at the peer can explain a refusal.** A source
   serves a hash only through an authorized committed shared reference.
   Sending an event does not mean the peer has activated it. Today's want
   generation also includes staged references. Either route can produce a
   readiness mismatch; neither justifies weakening the serving check.
4. **The later stall remains undiagnosed.** Error resolution preceded the
   stall. The source has object backoffs (30 s increasing to 1 h), unserved
   retries, 60 s leases, and remote-inventory-based backlog estimates. Their
   existence does not identify which, if any, explains these 148 seconds.
   There is no transition-time snapshot of the outstanding hash, eligibility,
   lease owner, next retry, remote activation or counter freshness.

First reproduce the unnecessary request; independently capture the actual
reason for remaining work at stall entry. Do not equate correlation with a
confirmed causal chain or simply suppress `NOT_PERMITTED` globally.

## 2. The intended separation and ordering

### Follow-up: live Visual Studio build at 02:18–02:19

The user reported severe build latency while Visual Studio had a solution
open on the mount. Two additional local bundles,
`20261019T051806.060Z` and `20261019T051901.718Z`, captured the same worker
instance and deployed version. The orchestrator had restarted/adopted that
worker at about 02:13:54; the worker itself remained alive from the previous
day. Do not combine daemon lifetimes with the earlier incident.

At the first heartbeat the engine had zero waiters and zero recent p99 wait.
About 55 seconds later it had **three waiters, a 121 ms current hold and
339 ms recent p99 lock wait**. Its lifetime maximum hold was 920 ms. The
histogram covers the most recent 1,024 acquisitions, with integer-millisecond
resolution; it is not a build-wide latency distribution. Both snapshots had
all current content available, no requested/download-in-progress objects, no
inbox work and no pending saves. Journal events increased from 23,807 to
24,142; outgoing unacknowledged events fell from 274 to 111.

A roughly ten-second process sample recorded 14.62 CPU-seconds in the worker,
0.16 in the orchestrator and 0.23 in Visual Studio. Two sampled MSBuild child
processes accumulated no measurable CPU in that interval; other build
processes were not sampled for this delta. This supports a busy local worker
and intermittent serialization, not a stopped engine or a demonstrated wait
for remote acknowledgements. It does not establish the critical path of the
entire build or identify the thread holding the mutex.

Source mechanisms worth profiling, at the inspected revision:

- `mount::tk_call` takes the shared engine lock around ordinary filesystem
  operations. `Engine::flush_entity` can hash/copy/install bytes and commit
  metadata while that lock remains held. Initial mutation of a referenced
  large file can copy it into staging under the lock too.
- `native/mount.c::create_open` explicitly disables kernel data caching;
  mount setup sets `FileInfoTimeout=0`. These preserve freshness with current
  semantics but increase callbacks for build workloads. Do not enable caching
  without a proven remote-change invalidation contract.
- A newly referenced read session verifies the whole object on its first
  access, outside the engine mutex. `Staged::reference` stops requesting that
  verification after `prime` populates its `OnceLock`: this is **not** a
  full-file rehash on every subsequent read of that session. Reopen/session
  churn and concurrent first readers still merit measurement.
- SQLite's writer and local disk/CPU remain shared with background work.
  There is no evidence here of an explicit global “wait until synced” barrier.

Extend WP6.0/WP5.2 profiling with operation and lock-holder labels, time in
lookup/verification/staging/install/SQLite commit, and build callback counts.
Compare identical mounted builds with sync enabled versus a controlled paused
sync run, plus a native-filesystem baseline, only when a repeatable build is
available. No sync setting or running build was changed for this observation.
Keep this local-I/O work distinct from the object admission fix: independent
network engines alone will not remove serialization inside the mount.

Metadata owns causal validation, namespace/conflict derivation, reference
authorization, feed cursors and acknowledgements. Objects own verified byte
availability, demand, source selection, transfer leases, durable offsets,
retry, and installation. Neither owns the other's event loop or waits for
the other while holding a shared engine lock or SQLite transaction.

“Metadata first” has three distinct meanings:

1. **Local durability before announcing a local write.** The author still
   installs and durably retains its bytes before committing metadata that
   promises them. Independent synchronization does not permit acknowledging
   an unsaved file or losing its only copy.
2. **Metadata replication and activation independent of remote bytes.** A
   receiving replica validates and commits metadata even if it lacks content.
   Missing content is an explicit state; a read can wait or fail with a useful
   filesystem status. Neither all object uploads nor remote acknowledgements
   are prerequisites for namespace convergence.
3. **Per-reference admission before object sharing/request dispatch.** The
   destination commits the relevant authorized reference before ordinary
   fetching, and the serving source checks its own current authority before
   exposing bytes. A source-readiness hint may improve routing; it cannot
   replace that check. Unrelated metadata need not be globally caught up.

This proposal deliberately revisits the current staged-reference optimization
in §6.4 of the plan. Quarantine, missing parents and rejected metadata must
not become ordinary object demand. Activation must remain content-independent
before that optimization is removed, or a circular wait would be introduced.
Test that property explicitly, including checkpoints and history imports.

“Resolved” means metadata has a deterministic, validated committed view.
It cannot mean that a human has dismissed every conflict in the repository.
Authorized conflict alternatives and retained history can require bytes;
their scheduling policy is separate from current-path reads. A global
`metadata_caught_up` barrier would starve objects during continuous editing
and recreate the activation-gate problem this revamp already addressed.

### A thin, durable admission interface

Proposed message names below describe semantics, not frozen API types:

| Message | Owner → consumer | Required meaning |
|---|---|---|
| `ReferencesAdmitted(generation, references)` | Metadata → objects | Committed authority, repository/epoch/branch scope, reference identity and hash; size if known. |
| `ReferencesWithdrawn(generation, references)` | Metadata → objects | Re-evaluate queued work, leases and serving permission; a hash may retain other authorized references. |
| `DemandRaised/Released(ticket, reference, deadline)` | Read owner → objects | A cancellable waiting read, joined to existing work; revision and authorization are validated. |
| `ObjectAvailable(hash, generation)` | Objects → readers/status | Verified, durably installed bytes; wake waiters and update content state. |
| `ObjectFailure(hash, source, class, retry_at)` | Objects → readers/status | A scoped outcome with a bounded retry/wait policy, not a metadata-engine failure. |

Persist admission changes atomically with metadata commits, using an outbox
or a replayable derived-reference generation. A bounded channel carries
wakeups; it is not the sole record of admission. After a dropped notification,
restart, snapshot replacement or epoch change, consumers reconcile from the
durable view. Handlers are idempotent. Coalescing dirty-generation wakeups is
safe only when the authoritative delta or replacement snapshot is recoverable.

Object installation plus SQLite state spans filesystem and database crash
boundaries: retain the existing durable-install ordering, then commit the
availability update and notification generation. Recovery must find an
installed but unindexed object with existing valid provenance and repair its
availability without asking a peer. A failed
index update must be observable and recoverable, not silently assumed healed.
For locally authored writes, consider making reference admission and the
already-verified object's availability update part of the same short metadata
transaction. This needs crash tests and a bounded recovery path if the
transaction fails; it must not move hashing/fsync into that transaction.

Availability repair is **not** sharing registration. Preserve plan invariants
3 and 13: a crash after receiving bytes but before registering their verified
transfer still follows the established re-transfer recovery unless a separate
durable provenance design is reviewed. Never register arbitrary existing CAS
files at startup. A private-only matching hash must not become shared merely
because a remote reference mentions it. The no-network-repair regression here
is specifically a locally authored/already-granted object whose derived
availability row was lost, not that unregistered-receive case.

Do not add a synchronous message round trip per transferred chunk. Use
immutable admission snapshots/generations and revalidate at dispatch and at
the serving boundary. Define revocation linearization and the fate of
in-flight chunks explicitly; queued stale capabilities must not authorize
new reads after revocation. Hash identity alone is never authority, especially
when private and shared branches reference the same CAS bytes.

### Demand scheduling and resource ownership

The object engine should coalesce work by repository, hash and compatible
authorization scope, while tracking each read's deadline/cancellation
separately. One canceled reader must not cancel other readers or required
replication. After transfer, the mount rechecks its handle revision before
serving bytes. Never substitute a newer file revision for the requested one.

Use explicit classes: waiting reads, current committed content, retained
history, then speculation. Add aging/fair shares so a stream of reads cannot
permanently starve replication. Priority must affect **slot admission**, not
just sorting a list behind occupied bulk slots. Reserve capacity for demand
or yield background work at bounded chunk boundaries, preserving durable
offsets and the one-writer lease. Test large-object head-of-line blocking.

A demand signal can wake a retry and try a different eligible source; it must
not erase a genuine authorization denial or continually reset a failing
source's backoff. Reset only the relevant source/hash eligibility on new
evidence such as reference activation, a new source, or a new session.
If no source has bytes yet, expose `waiting_for_source` with an absolute
deadline and an escalation policy. Source health probes are not byte progress.

Preserve dedicated metadata lanes on **both peer transport and loopback
worker IPC**. Also budget shared resources: DB-writer admission, hashing,
fsync, memory, open files and handler threads. Separate sockets cannot protect
metadata latency if bulk work fills the disk queue or monopolizes SQLite's
writer. Bound queued work as well as active workers, publish occupancy, and
reserve enough metadata capacity to finish authorization/revocation. Use
fairness to avoid starving durable object completion in the opposite direction.

### Failure classes and useful progress

Treat `awaiting_metadata`, `waiting_for_source`, locally repairing an index,
and scheduled backoff as distinct scheduling states. A known temporary local
readiness mismatch should not create one user-visible fatal error per hash.
For older peers returning ambiguous `NOT_PERMITTED`, recheck local admission
and perform a bounded metadata refresh; preserve the denial if evidence does
not establish a recoverable readiness condition. A protocol extension must
not disclose private object existence to unauthorized peers.

Keep real unauthorized access, corrupt bytes and durable I/O failure visible.
Scope breakers to the failing source, object, storage device or operation
class. Do not use an expected per-object wait to disable metadata sync or mark
the whole peer unusable. Aggregate repeated errors into an episode with first
time, last time, count, representative detail and recovery reason; retain
bounded per-object diagnostics without flooding the activity ring.

Track separate progress: metadata committed/acked generations, object durable
bytes and installed objects, and user-read completion. Capture a structured
snapshot at stall **entry and clearance**: eligible/deferred work by reason,
oldest demand, active lease/session, last durable progress, next retry,
admission generation, remote counter age, queue/resource occupancy and recovery
reason. A stale remote inventory means `observation_stale`, not measured local
transfer failure. Waiting states still have age/deadline alarms; they cannot
hide a permanent deadlock by resetting a timer on every retry.

## 3. Lessons from JuiceFS, with limits

Source studied: sibling checkout `..\juicefs`, commit
`506808b0c521984e1780ff6ae8ca05cc0caddfaf`. Links below pin that revision.
JuiceFS has an authoritative metadata service and shared object storage.
TKFS replicates causal metadata and authorized content among intermittently
connected peers. Its conflict, admission and private-CAS rules cannot be
borrowed from JuiceFS. The valuable transfer is its ownership, scheduling,
recovery and diagnostic discipline. Source and test bodies were inspected;
the JuiceFS test suite was not executed.

| Source and behavior | TKFS lesson and qualification |
|---|---|
| [writer.go](https://github.com/juicedata/juicefs/blob/506808b0c521984e1780ff6ae8ca05cc0caddfaf/pkg/vfs/writer.go): `flushData` finishes data work before `commitThread` publishes the corresponding slice metadata; dependent slices preserve write order. Slow metadata calls occur outside the file mutex. | Preserve local byte durability and per-file ordering while moving slow work off locks (WP5.2). This is **not** a reason to block all TKFS metadata replication until every peer has content. |
| [cached_store.go](https://github.com/juicedata/juicefs/blob/506808b0c521984e1780ff6ae8ca05cc0caddfaf/pkg/chunk/cached_store.go): writeback stages data, tracks pending/succeeded/abandoned state, falls back on staging timeout, removes a late abandoned stage. | Model ownership and late completion explicitly. Cancel/retry must not leave a stale completion that resurrects obsolete work. Writeback's weaker remote-availability semantics are an explicit mode, not a universal durability recipe. |
| [singleflight.go](https://github.com/juicedata/juicefs/blob/506808b0c521984e1780ff6ae8ca05cc0caddfaf/pkg/chunk/singleflight.go): concurrent loads share one keyed operation and reference-counted result. | Join mounted reads and background fetches; retain TKFS cross-lane leases. The implementation's wait group does not provide per-waiter cancellation: add that requirement rather than claiming JuiceFS supplies it. |
| [prefetch.go](https://github.com/juicedata/juicefs/blob/506808b0c521984e1780ff6ae8ca05cc0caddfaf/pkg/chunk/prefetch.go): bounded queue, duplicate suppression, nonblocking drop when speculative capacity is full. | Optional prefetch must never occupy unlimited memory or block demand/metadata. Dropping speculation is safe; dropping durable admission facts is not. |
| [reader.go](https://github.com/juicedata/juicefs/blob/506808b0c521984e1780ff6ae8ca05cc0caddfaf/pkg/vfs/reader.go): explicit reader states, retries invalidate cached chunk mappings, metadata I/O outside the file lock, adaptive readahead. | Separate stale mapping from failed bytes; refresh only relevant reference/source evidence. Bound memory and adapt speculation to access patterns. Its BUSY→REFRESH path has a cancellation TODO; do not copy that limitation into a cancellation guarantee. |
| [openfile.go](https://github.com/juicedata/juicefs/blob/506808b0c521984e1780ff6ae8ca05cc0caddfaf/pkg/meta/openfile.go): TTL checks and selective slice-map invalidation on changed attributes/chunks. | Cache ownership and generation invalidation must be explicit. Avoid globally cooling unrelated object demand whenever any namespace event arrives. TKFS causal identity is stronger than mtime-based cache validation. |
| [cached_store.go](https://github.com/juicedata/juicefs/blob/506808b0c521984e1780ff6ae8ca05cc0caddfaf/pkg/chunk/cached_store.go): upload semaphore, deduplicated pending keys, delayed retries and bounded channels. | Use bounded active and queued work with per-source retry policy. Audit retries across scheduler, RPC, lease and store layers to prevent multiplicative retry storms. Debug attempts and summarize a failure episode. |
| [disk_cache.go](https://github.com/juicedata/juicefs/blob/506808b0c521984e1780ff6ae8ca05cc0caddfaf/pkg/chunk/disk_cache.go): distinct cache/staging paths, temporary-file rename, stage scanning and stale temporary cleanup. | Separate disposable cache from irreplaceable unsynced data; recover durable intent on restart. The examined `flushPage` path does not fsync: do **not** copy its persistence contract for TKFS authoritative local bytes. |
| [disk_cache_state.go](https://github.com/juicedata/juicefs/blob/506808b0c521984e1780ff6ae8ca05cc0caddfaf/pkg/chunk/disk_cache_state.go): normal/unstable/down states, probes and recovery hysteresis scoped to a cache. | A local cache failure should degrade that subsystem with bounded probes, not collapse unrelated metadata. Distinguish optional cache failure from loss of the only durable copy. Constants require TKFS measurements. |
| [redis.go](https://github.com/juicedata/juicefs/blob/506808b0c521984e1780ff6ae8ca05cc0caddfaf/pkg/meta/redis.go): transaction-conflict retries differ from transport failures with uncertain commit outcome; bounded retries, jitter and restart metrics. | Retry known contention; preserve request IDs and do not blindly replay a mutation after an ambiguous disconnect. Temporary contention needs metrics and a terminal deadline, not fatal-log spam per attempt. |
| [sql.go](https://github.com/juicedata/juicefs/blob/506808b0c521984e1780ff6ae8ca05cc0caddfaf/pkg/meta/sql.go): backend-specific retry classification and serialized SQLite transaction handling. | More connections or async tasks do not create multiple SQLite writers. Evaluate a bounded writer owner/queue and short batched transactions, preserving foreground durability and metadata fairness. |
| [accesslog.go](https://github.com/juicedata/juicefs/blob/506808b0c521984e1780ff6ae8ca05cc0caddfaf/pkg/vfs/accesslog.go), [debug.go](https://github.com/juicedata/juicefs/blob/506808b0c521984e1780ff6ae8ca05cc0caddfaf/cmd/debug.go): latency/error metrics, slow-operation logging, nonblocking log delivery and diagnostic collection. | Record phase latency and a bounded diagnostic history; observability must not become backpressure on the filesystem. Capture two snapshots to measure movement, with build versions and clock correlation. |
| [writer.go](https://github.com/juicedata/juicefs/blob/506808b0c521984e1780ff6ae8ca05cc0caddfaf/pkg/vfs/writer.go): memory/slice pressure bounds and a flush-timeout report with rate-limited stack dumps. | Bound producer pressure deliberately, and explain who is waiting on whom when a deadline expires. Do not import its sleeps, slice counts or multi-minute timeouts without workload/adapter justification. |
| [compact.go](https://github.com/juicedata/juicefs/blob/506808b0c521984e1780ff6ae8ca05cc0caddfaf/pkg/vfs/compact.go): immutable slice compaction, memory gating, explicit `SetWriteback(false)`. | Smaller immutable transfer units may reduce rebuild traffic and demand blocking. Treat chunked CAS as separate format/protocol research, with metadata growth, deduplication, GC and crash analysis; do not sneak it into this engine-separation wave. |

The [I/O model](https://github.com/juicedata/juicefs/blob/506808b0c521984e1780ff6ae8ca05cc0caddfaf/docs/en/introduction/io_processing.md)
and [cache guide](https://github.com/juicedata/juicefs/blob/506808b0c521984e1780ff6ae8ca05cc0caddfaf/docs/en/guide/cache.md)
help explain normal publication versus writeback and close-to-open visibility.
Source must qualify older documentation: current compaction explicitly turns
writeback off. TKFS needs its own peer-visibility contract rather than assuming
a shared metadata server's consistency model.

Test techniques worth adopting:

- `cached_store_test.go::TestWritebackStageTimeoutRemovesLateStage` blocks a
  stage, lets fallback finish, then releases the late stage. Use the same
  controlled interleaving for canceled transfers and obsolete save completion.
- `singleflight_test.go::TestSingleFlight` exercises many concurrent callers
  and result ownership. Add TKFS authorization scope and independent waiter
  cancellation to that shape of test.
- `prefetch_test.go::TestPrefetcher` covers duplicates and a full speculative
  queue. Extend it with a demand arrival while every background slot is busy.
- Read test bodies, not just names: reader tests mentioning cancellation
  include cases where invalidation intentionally does **not** cancel the
  ongoing read immediately. Make TKFS's intended semantics explicit.

## 4. Concurrency today and the Tokio decision

The lack of Tokio is not a lack of concurrency. Rust's standard threads can
execute concurrently and in parallel. The previous plan explicitly selected
`std` threads and `polling`, and excluded an async migration as a scope choice.
That choice deserves reevaluation with evidence, not an assertion that this
transport architecture is the only safe Windows/WSS implementation.

| Existing boundary | What it already does | Remaining question |
|---|---|---|
| [`session/io.rs`](../../../src/session/io.rs), [`session/conn.rs`](../../../src/session/conn.rs) | One nonblocking socket/TLS owner per connection, readiness polling, multiplexed requests, timers, priority writes and bounded queues. | Per-connection thread/memory cost at scale; cancellation and timer complexity; CPU fairness across simultaneous streams. |
| [`orchestrator/scheduler.rs`](../../../src/orchestrator/scheduler.rs) | Separate metadata/bulk admission, worker pools, keyed claims, generation wakeups and classified retries. | End-to-end queue budgets, demand admission and shared bottlenecks. A raw unbounded `mpsc` channel does not prove unbounded admitted jobs; inspect producer limits too. |
| [`runtime/ctx.rs`](../../../src/runtime/ctx.rs) | Secondary SQLite stores keep some work off the engine lock. | `MAX_IDLE` bounds retained idle connections, not active connections or concurrent writers. |
| [`runtime/content.rs`](../../../src/runtime/content.rs) | Missing-content reads register demand and wait outside the engine mutex, then recheck revision. | Polling/deadline policy, resource occupancy per waiter, cancellation, and demand's effect on occupied bulk slots. |
| [`worker/status_cell.rs`](../../../src/worker/status_cell.rs) | Passive snapshots and content maintenance separate status reads from engine work. | Counter freshness, different availability definitions, transition-time evidence. |

Tokio is useful for many concurrent I/O waits, timers, cancellation and task
coordination. It does not make hashing or fsync faster, or remove the SQLite
writer limit. See the official [Tokio tutorial](https://tokio.rs/tokio/tutorial).
Blocking filesystem/database work inside a future can stall executor threads.
`spawn_blocking` needs explicit concurrency limits for expensive work, and
started blocking tasks cannot simply be aborted; persistent blocking workers
may be better as dedicated threads. See
[`spawn_blocking`](https://docs.rs/tokio/latest/tokio/task/fn.spawn_blocking.html).
A runtime can coexist with synchronous mount/GUI APIs through channels, as
described in [Bridging with sync code](https://tokio.rs/tokio/topics/bridging).

**Recommendation:** approve the engine ownership/admission design first;
include a bounded Tokio prototype and runtime ADR in the future wave. Compare
the current implementation with an async transport/orchestration island,
keeping bounded dedicated SQLite and blocking byte-work owners. Prototype
WSS with a compatible async WebSocket/TLS stack; wrapping blocking socket
calls in async functions is not a migration. Carry forward request-id safety,
priority lanes, queue limits, durable leases and graceful shutdown. Do not
rewrite the filesystem, store and protocol simultaneously.

Measure thread count, resident memory, idle CPU, throughput, metadata p50/p99,
demand queue delay, cancellation and shutdown under 1/8/32 folders and 1/4/8
peers, including idle connections, build storms, large transfers and failures.
These are proposed experiment sizes, not supported-scale claims. Record
hardware, versions, repeated runs and variability. Choose Tokio if it gives
clear scaling or lifecycle-simplicity benefits while meeting the same
correctness and latency gates; retain `std`/`polling` if it does not. Either
result still requires independent engines and bounded shared resources.

## 5. Build performance: what JuiceFS avoids, and what to change first

A third live bundle, `20261019T052607.461Z` (02:26:07 local), still showed
content complete, no requested/downloading objects, no incoming backlog and
zero unresolved conflicts. The engine had one waiter, a 181 ms current hold,
230 ms recent p99 wait and a 920 ms lifetime maximum hold. The user reported
the build was still running. This reinforces intermittent local contention;
it is not a measured whole-build slowdown ratio or attribution of the holder.

### Objects, checksums and conflict semantics

JuiceFS addresses physical objects by allocated slice ID, block index and
length (`rSlice::key` in `pkg/chunk/cached_store.go`). Its optional hash prefix
is derived from the slice ID for distribution, not a cryptographic digest of
the file. The examined disk cache supports CRC32C checksums per 32 KiB, with
configurable verification. This detects cache corruption; it is not TKFS's
SHA-256 content-address identity or peer-provenance contract. Equal content
does not automatically have the same JuiceFS object key.

Files use 64 MiB logical chunks, variable written slices and normally up to
4 MiB physical blocks. Random updates add slice ranges instead of necessarily
copying and rehashing the full original file. Reads combine the effective
ranges; compaction later reduces fragmentation. TKFS currently installs a
whole-file content object for a revision. A small modification of a large file
can consequently require whole-file staging, hashing and object installation.
Chunking can improve that amplification, but will not by itself remove the
per-operation cost of a build creating thousands of small files.

JuiceFS clients submit operations to a common authoritative metadata backend.
Transactions coordinate competing namespace/metadata updates. Overlapping
file data is represented by ordered slice mappings; later mappings cover
earlier ranges. This is not semantic merging of independently edited files,
nor preservation of TKFS-style concurrent offline versions for user resolution.
Applications still need coordination for concurrent writes. TKFS's causal
events, alternatives and deterministic conflict derivation solve a different
problem. Preserve that capability while keeping unrelated local operations
incremental; do not pay for a history-wide derivation on every lookup or save.

JuiceFS also uses kernel/client data caches and metadata caching with explicit
freshness rules. Default close-to-open behavior has documented cache
exceptions; increasing its open-cache TTL changes visibility guarantees.
Borrow the disciplined cache ownership, not an assumption that arbitrary
caching preserves TKFS's current remote-update semantics.

### Ordered performance work

1. **Measure the build's actual critical path.** Obtain a representative build
   log with timings and a bounded worker CPU/operation profile. Count object
   bytes hashed/copied, verification reuse, fsync/SQLite commit time, view
   reloads, callbacks by operation and lock holds by owner. Existing wait
   gauges cannot distinguish a slow save, a view rebuild, a read, or sync work.
   Compare native, mount without sync traffic, and mount with traffic using
   the same clean/incremental workload and cache state. Record distributions
   and repeat runs, not just bulk MiB/s.
2. **Finish WP5.2's off-lock save path first.** Snapshot an immutable save input
   under the engine lock, perform copying/hashing/durable install outside it,
   then commit after a revision/generation check. Use per-file operation
   ownership, bounded byte workers and independent read snapshots. Keep the
   shared metadata critical section short. Add crash, competing-write and
   delete/rename tests before treating this as merely moving a mutex.
3. **Reuse verified immutable data safely.** `put_object` hashes input and
   installs it; `check_commit` can immediately re-read/hash a newly referenced
   object. Carry an internal install/verification proof into that commit
   (already proposed in WP5.1), tied to the store, identity and installed file.
   Reuse verification across compatible read sessions with bounded handles
   and generation invalidation. Do not trust externally supplied hash claims
   or size/mtime alone; define the external-tampering and corruption model.
   Keep verification of received bytes and crash recovery. The current
   `OnceLock` already prevents repeated hashing within one primed session.
4. **Reduce callback and metadata amplification.** Profile the cost of
   `DisableCache=1` and `FileInfoTimeout=0`, then implement safe invalidation
   before introducing bounded caching. Audit `Store::view` reloads and commit
   cache patching under concurrent secondary-store writes; expose existing
   view-load counters. Cache unrelated namespace data across object-progress
   updates. Database hot paths should scale with changed entries, not total
   history or directory-tree size.
5. **Amortize durability without weakening it.** Measure redundant flushes and
   transaction boundaries, coalesce compatible metadata work and avoid duplicate
   saves. Cleanup and close both call flush, but a clean session returns early:
   do not claim they always save twice. Keep explicit flush/close guarantees,
   `synchronous=FULL`, durable offsets and proven write ordering. Group commit
   would be separate reviewed work, not disabling fsync or silently delaying
   acknowledged durability.
6. **Apply resource isolation, then evaluate chunked CAS separately.** Metadata,
   foreground file work and background transfers need bounded CPU/disk/DB
   admission. Tokio can improve orchestration but will not cure whole-file
   amplification, uncacheable callbacks or long mutex critical sections.
   Benchmark a chunked representation on large partially rewritten build
   artifacts only after the immediate serialization costs are understood.

Fast NVMe bandwidth does not cancel a dependency chain of serialized saves,
verification passes, metadata operations and durability barriers. The observed
hundreds-of-milliseconds mutex waits justify investigation; they are not an
acceptable overhead budget merely because this is a virtual filesystem.
Set a whole-build regression gate against the same native workload after
collecting a baseline, in addition to operation p99 and recovery tests.

For immediate operational relief on the **next** build, put disposable build
outputs/intermediates on native local storage while retaining source in the
mount, or use a separate native working copy. For compatible .NET 8+ SDK
projects, an external `ArtifactsPath` can separate output by project and
configuration ([Microsoft documentation](https://learn.microsoft.com/en-us/dotnet/core/sdk/artifacts-output)).
This must be checked against project-specific targets and restore paths;
it is a workaround and an isolating experiment, not the filesystem fix.
It does not relocate every IDE cache. No files or output settings were changed
under the active Visual Studio build.

## 6. Proposed work packages and acceptance

These are planning packages, not assigned implementation ownership. Reconcile
them with the final Wave 5 changes before freezing seams or starting edits.
WP5.1 affects admission/activation; WP5.2 affects save/read lifetime; WP5.3
already promises lease recovery and the next demand slot; WP5.4 affects
freshness. Extend those results rather than creating competing schedulers.

| Package | Deliverable | Exit evidence |
|---|---|---|
| WP6.0 Incident observability and reproduction | Versioned transition snapshots, admission/want trace, index-update failure counter, repeatable mounted-build workload. | Reproduce or falsify the local-object want hypothesis on the actual code path; distinguish observed wait from inferred wait. |
| WP6.1 Metadata/object admission contract | Durable per-reference admission, replay/reconciliation, no ordinary staged wants, consistent availability repair. | Crash/restart and authorization tests; metadata activates with zero content; installed local bytes never require a network fetch solely because an index write was lost. |
| WP6.2 Object demand and bounded scheduling | Joined cancellable demand, source-aware retry, reserved/yielding slots, queue/memory limits, progress-aware bounded read waits. | Foreground demand starts despite background saturation; another reader survives cancellation; same bytes are not redundantly installed by competing lanes. |
| WP6.3 Status and error episodes | Per-engine progress/wait states, scoped failure policy, aggregated activity and diagnostic detail. | Expected readiness waits cause no fatal-error flood or unrelated metadata breaker; genuine denial/corruption remains visible. |
| WP6.4 Resource budgets and runtime ADR | Shared DB/disk/CPU budget design; measured current-versus-Tokio prototype. | Comparable correctness, WSS/Windows/Linux operation, bounded memory and shutdown, documented runtime decision. |
| WP6.INT Qualification | Integrate with Wave 5 and existing oracle/security/objects floors. | Two-machine build/read workload, fault injection, crash boundaries and repeated performance evidence. |

Required fault scenarios:

1. Lose the availability update after durable install; concurrently commit
   another SQLite writer; complete the local metadata save; query wants and
   read the file. Verify local repair, no redundant peer request, and eventual
   consistency between status and scheduling. Repeat after process restart.
2. Deliver metadata before bytes; delay activation; omit a parent; quarantine
   an event. Committed independent metadata continues; only eligible references
   admit objects. Never require content to authorize the content fetch itself.
3. Drop/coalesce every notification, crash after commit before send, replace
   a snapshot/epoch, and withdraw one of several references to the same hash.
   Replay restores exact authority; private references never grant sharing.
4. Saturate all bulk lanes with large history objects, then start several
   short mounted reads. Assert prompt slot admission, fair background
   progress, bounded memory and no duplicate writer lease. Cancel one waiter,
   mutate another handle's revision and disconnect the current source.
5. Source knows metadata but lacks bytes; source has bytes but has not admitted
   the reference; source permanently denies access. Check distinct bounded
   local handling without revealing private existence or retrying forever.
6. Inject corrupt data, a full disk, slow fsync, SQLite contention and stale
   remote inventories independently. Verify the failure scope, exact last
   durable progress, meaningful mount error and useful stall snapshot.
7. Repeated solution rebuilds plus two-way metadata edits and a 512 MiB
   download; include a 45 s outage. Preserve all Wave 5 correctness/recovery
   floors. Record request latency separately from transfer completion time.

Proposed additional performance gates, to ratify on reference hardware:
metadata p99 under saturated bulk at most 2× its matched unloaded baseline;
eligible demand receives a slot within one bounded background chunk quantum
(initial target 250 ms, excluding unavailable-source/network time); bounded
queues and no upward memory trend during a two-hour workload. These are
targets, **not measurements**. If the transport quantum cannot meet them,
reserve capacity rather than claiming priority based on queue order alone.

Validation completed for this investigation: 33 local/pinned-source link
targets checked against the checkouts; JSON parsed; the 19-error interval and
stall durations checked against the diagnostic activity; patch whitespace
checked. The isolated SQLite 3.51.1 probe produced `SQLITE_BUSY_SNAPSHOT` (517)
and demonstrated the modeled lost-index want and its repair. Its result is
saved alongside the probe. Production Rust and JuiceFS suites were not run:
no production behavior changed, and the probe is not a TKFS regression test.
Full runtime qualification belongs to implementation of the proposed packages.
