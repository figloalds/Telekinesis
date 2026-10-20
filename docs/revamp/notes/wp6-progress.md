# Wave 6 implementation record

2026-10-20. Goal: implement `WAVE_6_PLAN.md`. Work is sequential on the existing
checkout; no merge, push, deployment, live-store migration or feature activation
has been performed. The goal is **not complete**.

## Contracts and baseline (WP6.0)

CCR-WP6.0-1 introduces `model::content`, `model::workspace`,
`util::phases` and `WAVE_6_CONTRACTS.md`. Existing wire/IPC/events/schema are
unchanged. Golden binary fixtures have an independent Python reference encoder.
The six release `content_format` tests passed on Windows.

The selected representation is fixed 64 KiB, direct through 64 KiB, canonical
empty direct content, non-padded tails, indexed 256-entry pages with
domain-separated identities and a 256 TiB new-format file-size limit.
Provider/session/coordination limits and callback durability mapping are
recorded before runtime consumers. Foundation counters exist; instrumenting
all core/adapter/SQLite phases remains work, not measured evidence.

An actual Wave 5 release executable was built from an isolated source snapshot
of `02a6f9d`; `git diff b1b8e52 02a6f9d -- src Cargo.toml Cargo.lock build.rs
native ui` is empty. Build command:

```
python scripts/build.py --platform windows --profile release --keep-build-cache --jobs 4 --offline --output target/wave6/baseline
```

Executable SHA-256:
`7d9c8a386b64a563c118c4107a5bd3f165ebf59d152c06e71fedcdb46f522dbe`.
The binary's content-derived version is `0.1.0+8029c80fce14`.
It is retained locally under `target/wave6/baseline/windows-x86_64`.

The actual executable reopened a disposable store after `PRAGMA user_version=600`
and `config('schema:content')='999'` were installed. It exited **0**, so a schema
marker does not fence old binaries. Activated stores require isolation plus a
tested launcher fence. That fence is a WP6.6 implementation/qualification item;
the probe is not proof that the fence already exists.

The generated .NET harness has 8 libraries, 128 small classes each, a dependent
executable, offline package sources, Release configuration, build parallelism 4,
and all `obj`/`bin` inside the tested workspace. It validates the executable's
answer, no-op compilation avoidance and incremental compilation of only the
edited library. Each scenario has one warm-up and five recorded repetitions.
No OS cache flushing or antivirus exclusions were performed. The host's
antivirus policy was left unchanged, not independently characterized.

```
python scripts/wave6_build.py --label native-wave6-baseline --antivirus-policy "Host policy unchanged; no exclusions added"
python scripts/wave6_build.py --label wave5-mounted-baseline --exe target/wave6/baseline/windows-x86_64/tkfs.exe --fence-probe --antivirus-policy "Host policy unchanged; no exclusions added"
```

| Warm median | Native | Wave 5 mounted, no peers | Ratio |
|---|---:|---:|---:|
| Clean | 4331.69 ms | 5683.76 ms | 1.312 |
| One-file incremental | 1340.22 ms | 1852.31 ms | 1.382 |
| No-op | 433.04 ms | 894.15 ms | 2.065 |

The Wave 6 no-op target is 683.04 ms on this native baseline; the Wave 5 result
does not meet it. Targets have not been relaxed. Clean mounted samples range
5667.54–6325.42 ms, so retain the individual samples and noise. These are not
p99 measurements or real-user-solution results. The baseline has Wave 5's
weaker burst-close contract; the new durability work must be disclosed in the
final comparison.

Evidence: `test-evidence/wave-6/{native-baseline,wave5-mounted-baseline}.json`,
plus CPU and disk metadata. The earlier complete build run had a shutdown
failure (`BUSY_VIEW: 19 open contexts, 0 unsaved files`) due to retained
MSBuild handles. Its failed report is preserved as
`wave5-mounted-shutdown-failure.json`. The corrected harness force-stops only
its newly created isolated worker, preserving dirty work. The rerun completed
successfully. Earlier harness errors (unsupported `dotnet clean --no-restore`,
Windows path separators in rebuild assertions, precreating a WinFsp mount
directory) were corrected; failed local artifacts remain under `test-runs`.

Remaining baseline matrix: controlled cold runs, explicit-flush comparison,
interruption/watch/remote timestamp workloads, sync-load and multi-repository
saturation. The user's actual solution has not been copied or measured.
No Wave 6 build gate has been claimed.

## Content foundation (WP6.1, in progress)

Integration ownership transfers to `src/content/{provider,local}.rs` and
`tests/content_provider.rs` for implementation of the frozen content contracts.
The modules are public library seams, not connected to mounted writes or peer
protocol. Current additions implement bounded immutable reads, streaming
construction, chunk-local constant-length overwrites, separate manifest CAS,
root pins and immutable internal snapshot handles. Multiple handles to the same
canonical store share gates, operation limits and pin ownership. Live pins keep
that ownership alive even when the provider is closed and reopened.

Fresh Windows release verification:

| Check | Outcome |
|---|---|
| `content_format` | 6 passed |
| `content_provider` focused tests | 9 passed, 1 qualification initially ignored |
| Explicit ignored 1 GiB amplification qualification | 1 passed, 3.17 s |
| Clippy for library and both affected test targets, `-D warnings` | passed |
| Python syntax checks for the new harnesses/reference encoder | passed |

Commands and source/test-executable hashes are recorded in
`test-evidence/wave-6/foundation-checks.json`. The first provider test run
exposed a measurement bug: a missing deduplication lookup charged expected
bytes as reads. Read accounting now starts only after opening and validating
the object; the rerun passed. Phase totals on failed I/O still include attempted
bytes and timers may nest; they must not be presented as exclusive OS I/O
accounting.

The 1 GiB fixture streams a repeated chunk with bounded memory. A 4 KiB
contained overwrite created exactly one new data chunk and two pages, without
whole-file hashing/reading. A boundary-crossing overwrite created two chunks.
Logical edits and unique CAS writes are distinct measurements; this repeated
fixture is not a general compression/throughput result. Other tests cover
distinct data chunks, missing unrelated content, corrupt demanded content,
forged parent/child lengths, legacy whole-file verification, full overwrite of
an absent base chunk, cancellation/deadlines, snapshot isolation, pins across
provider reopen, and a delayed/failing nonlocal provider.

`scripts/wave6_chunk_tradeoffs.py` records a deterministic 16/64/256 KiB
representation experiment in `test-evidence/wave-6/chunk-tradeoffs.json`.
For an 8 MiB source at 64 KiB, contained/cross-boundary 4 KiB overwrites need
one/two new chunks; insertion at the beginning invalidates all 129 resulting
chunks, insertion in the middle invalidates 65, and identical atomic replacement
reuses all chunk identities but full re-import still hashes 8 MiB. This is a
representation simulation, not a TKFS runtime performance gate.

Bounded reachability traversal now reads one manifest page per slice and returns
at most 256 verified edges. It reads no data chunks and retains its root pin.
A missing/corrupt parent leaves the frontier intact for retry; children are
never admitted before verifying that parent. Both focused traversal tests passed
on Windows. Durable roots and pending-save integration are still being connected.
The present code exposes no deletion/GC operation.

## Metadata and durable admission (WP6.2, in progress)

CCR-WP6.2-1 extends the contract with a distinct projected descriptor field and
reference-scoped publication provenance. Core derivation, incremental successors,
fork/publication and restore retain typed register values. Event acceptance
requires only descriptor validation; no provider is opened. Legacy strings and
historical payloads retain their meaning and serialized shape.

Root metadata and an outbox row commit with each descriptor event. A bounded
object consumer commits root effects and its own cursor atomically. Its replay
ignores older generations. Reference withdrawal takes effect against metadata
authority immediately; it does not wait for the object cursor. The maintenance
thread reconciles durable admission on a dedicated connection with zero SQLite
busy wait, independent of the semantic engine lock.

Eight admission tests passed: content-independent activation/restart,
metadata/outbox rollback on a real SQLite trigger failure, cursor/effect rollback
and replay, generation-scoped revocation, upgrade-required refusal without
quarantine, duplicate/offline conflict convergence to the causal oracle,
worker-loop replay while the semantic engine lock is held, and preservation of
an already-staged unsupported descriptor as an incompatible pending episode.

CCR-WP5.2-3 is addressed with a projection-counter proof around view publication.
Overlapping object bookkeeping keeps the patched view; overlapping metadata
invalidates it. Preparation also captures and revalidates the branch journal
generation after taking SQLite's writer. Three deterministic real-SQLite
interleavings passed, alongside the four existing commit unit tests (7 passed).
The affected Windows release checks now cover 139 integration tests plus those
7 commit unit tests (146 passed). The successful target counts are:
`activation_inbox` 22, `availability_index` 23, `content_admission` 8,
`content_format` 6, `content_provider` 11, `contracts` 41, `offlock_commit` 20,
and `startup_snapshots` 8. Two long qualifications remain ignored in these
invocations: the 300-seed off-lock campaign and the separately run earlier
1 GiB content test. Strict Clippy passed for the library, new content/admission
tests and contracts; formatting and diff checks passed.

The initial broad run stopped at the contract audit: new error codes were not
registered and adding one to the frozen historical table changed its pinned
row count. This was fixed with a named Wave 6 error extension, preserving the
historical rows, and the contract test now verifies that extension. The missing
targets, contract failures and new content tests were rerun successfully.
Commands, artifact/source identities and limitations are retained in
`test-evidence/wave-6/metadata-admission-checks.json`. These are package-level
checks, not the full final integration suite or a release qualification.

## Object engine and demand (WP6.3, in progress)

CCR-WP6.3-1 adds typed object identities, a reusable immutable page DAG,
reference-relative positions, bounded range-to-transfer preparation, receipt
recovery and generation-owned scheduling. Root admission seeds one job; page
verification/expansion happens outside SQLite before a bounded transaction
revalidates the root generation. Maintenance reconciles roots, receipt evidence
and local pages on its own connection without taking the semantic engine lock.

An initial per-reference edge prototype failed an explicit scaling regression:
a 4 KiB revision of a 64 MiB file reread five manifest pages, including unchanged
subtrees. It was replaced before transport integration. The passing regression
now permits at most the two changed pages. Global page adjacency and typed-object
availability are stored once; new revisions do not replay reused descendants,
and identical chunks do not cause unbounded verification updates. Root-relative
range lookup uses indexed child positions. Page aliases allow work to find a
different valid root after withdrawing its original witness; stale alias history
is paged with a persisted cursor, at most 256 aliases per resolution.

Serving proves membership from immutable provider pages in addition to current
metadata authorization, so a forged derived index cannot disclose a private
chunk. An explicit position can be served while the sender's graph cursor lags.
Verification alone does not make private deduplicated bytes publishable. Actual
shared receipt is checkpointed in a bounded synced intent before CAS installation
and SQL indexing. Three process-crash boundaries, an actual failed SQLite receipt
write, corrupt recovery evidence and verified repair are covered. Repair retains
the corrupt file in quarantine. Legacy reads retain their whole-file contract;
legacy descriptors are explicitly refused by the bounded chunk-frame engine.

The scheduler reserves foreground, structural and background capacity globally,
per peer and per repository. Independent waiter cancellation/deadlines survive
joined installation; stale lease/drop tokens cannot release newer work. A full
provider's 64-waiter queue does not consume another provider's queue. Actual
range-to-transfer admission under six background jobs across three repositories
passed the 250 ms bound, and source backoff allows an alternate source immediately.
That test leaves the destination admission consumer behind: demand seeds only
its committed root and obtains service without waiting for a maintenance tick.
This is engine-level controlled evidence, not the required real peer/load campaign.
Verification buffers are weighted and bounded: blocked save producers retain
foreground read and structural traversal capacity. Explicit foreground preparation
runs before background alias repair, with a shared 32-candidate/1 MiB slice budget.

The engine checkpoint's Windows release checks passed 80 tests: `content_admission` 8,
`content_objects` 14, `content_provider` 12, `content_scheduler` 5 and
`contracts` 41. The separately invoked 1 GiB provider amplification test also
passed (1.95 s). Strict Clippy, formatting and diff checks are recorded in
`test-evidence/wave-6/object-engine-checks.json` with source/executable identities.
The former failing scaling regression is now a normal passing test, not ignored.
The full integration suite and cross-platform qualification have not been rerun.

## Typed object wire adapters (WP6.3, still in progress)

CCR-WP6.3-2 connects the immutable object engine to real loopback and authenticated
peer bulk handlers. The explicit receiver-driven `pull_one` exchange negotiates
object capability, obtains a route/run/generation-owned worker offer, proves and
reads one source frame, installs it with the durable receipt path, then ACKs the
source credit. Offers and frame credits cannot be released by stale tokens or
another authenticated session. Expired unacknowledged frames close their session
before returning byte credit. New frames have their own bounded inbound reserve,
dedicated content loopback sessions and a constant-work ACK path. Normal feature
activation remains unavailable.

Real-session tests now hydrate a manifest and three chunks with both workers'
semantic engine locks held. Other cases deny unnegotiated, bare-hash and forged
private-member reads, preserve newer offer/ACK ownership, reclaim credit on
session close and real timeout, and return real SQLite writer contention within
250 ms. An authenticated foreground frame also passes the 250 ms controlled
bound while two stalled save producers hold six background verification units.
Another real-session test holds eight legacy reads across two bulk sessions,
occupying both legacy session quotas and the old worker handler pool. The v1
frame and its ACK still finish within the controlled 250 ms bound.
This is source/frame service evidence, not the final continuously scheduled
cross-repository saturation campaign.

Integration exposed two gaps that were fixed: received verified pages still
needed expansion work, and source reads must carry foreground priority through
the provider buffer gate. Per-source/lane/kind keyset scans also prevent a
backed-off prefix from hiding all later objects; a regression confirms another
object gets a real transfer offer on the next slice. Repeated empty scans leave
SQLite's change count unchanged after their initial cursor. The earlier
engine-only evidence remains in its own report; current transport checks are
recorded in `test-evidence/wave-6/content-transport-checks.json`.

This checkpoint passed 98 unique focused checks: admission 8, objects 16,
provider 12, scheduler 5, real transport 9, contracts 41 and seam stubs 7.
Three selected legacy inbound checks also passed (server-wide limits across
session churn, role/route codes and shutdown), as did a separate 1 GiB
amplification invocation (2.13 s). Strict Clippy and formatting/diff checks
passed. The report distinguishes the 97-test invocation and the subsequent
9-test transport rerun after adding the legacy-saturation regression; overlapping
tests are counted once. These are package checks, not the full final suite.

## Continuous typed-content dispatch (WP6.3, in progress)

CCR-WP6.3-3 registers every outgoing bulk runner with a bounded daemon-wide
dispatcher. Four preparation threads, including two foreground probes, are
separate from four network threads; planner loopback sessions are separate from
frame/completion sessions. All admitted frames retain the common scheduler's
class, peer, repository and byte reservations. A slow network operation never
owns a preparation thread or the old two-repository turn gate. There are at most
four queued frames, 256 lane bindings and 512 working repository groups, with a
rotating deferred window. Source polling and worker/capability caches are bounded.

Real authenticated sessions and actual Store/CAS workers now exercise automatic
hydration through `objects::run_bulk_lane`. A three-repository test actually
stalls two background source reads for three seconds, then requires foreground
admission within 250 ms and its bytes before those reads finish. A complementary
case requires background bytes while a foreground source is stalled. These are
short controlled checks; the required 30-minute mixed-load run remains pending.

The first reconnect regression failed: an old source read held its transfer
slot until injected two-second worker I/O returned, even after closing its peer
session. The original one-second replacement gate was retained. A per-request
cancellation bridge now releases that source lease promptly while preserving
unrelated calls on the shared worker session. The exact regression then passed
in 0.30 s; the subsequent full transport run also passed. Outgoing capability,
read and completion calls observe cancellation too. Another test keeps the lane
registration alive to prevent socket-close cleanup from hiding the bug: lane
and daemon cancellation drain actual transfer ownership within 250 ms while the
source I/O is still stalled. Late registration Drop cannot remove a newer lane.

Preparation now advances an unaligned 1 MiB demand through its seventeenth
chunk instead of reverifying the first 16 until every slice's byte budget is
exhausted. The regression also removes a real cached leaf and root page, observes
their absence, invalidates derived availability, and refetches only the missing
object in each case. Required manifest pages participate in completion checks;
verified cached leaf adjacency alone is insufficient. Waiter windows rotate, and
lane filtering precedes the foreground candidate cap. V1 per-op body limits
are now 64 KiB for read/complete and zero for other v1 operations. Historical
operation limits remain unchanged.

The dispatcher view exposes bounded per-class admission/completion/failure/retry
counters and errors without starting content state on observation. Cursor-only
movement is not reported as verified progress. Full user-facing engine status
and bounded error-episode integration remain WP6.6.

Verification and source/executable identities for this checkpoint are retained
in `test-evidence/wave-6/continuous-dispatch-checks.json`. The initial reconnect
failure and its fix are recorded there; no timing gate was loosened.

The broader legacy lane check also exposed a separate real cleanup bug. The
600-object cross-source handoff regression failed twice in the serial lane
suite, although its isolated retry passed in 0.81 s. Bounded failure diagnostics
on the second run found 139 objects still leased to the stopped source's two
pull slots, with no exchange errors. A want could regrant an already-begun failed
object under the other slot; the duplicate check skipped it before backoff
handling, while settlement remembered only its originally planned slot. The
backoff check now precedes duplicate suppression, returning that regranted lease
under the slot that actually received it. The existing 40-second availability
and 15-second completion bounds are retained. This fixes that orphaned-lease
case; it does not provide generation fencing for a later lane run.

After that fix, the full serial legacy lane target passed 53 tests (18 long
qualifications ignored) in 88.91 s. The final focused content invocation passed
104 tests: admission 8, objects 17, provider 12, scheduler 5, transport 14,
contracts 41 and seam stubs 7. Session-core 59 and inbound 20 passed earlier in
this checkpoint, before the last legacy duplicate cleanup; their executable
stages are identified in the report. Strict Clippy and formatting/diff checks
passed. The final controlled samples measured foreground admission at 29.54 ms,
background availability under stalled foreground at 61.81 ms, lane/daemon cancel
drain at 57.84/27.74 ms, and replacement availability at 97.81 ms. Each sample is
one package-test observation, not a percentile or sustained-load result.

## Ordered retained pull ownership (WP6.3, in progress)

CCR-WP6.3-4 addresses CCR-WP5.INT-2 in the retained whole-file pull path.
Each lane uses generation-qualified local lease IDs. Both slots share a fence
ordered by a durable daemon incarnation and a monotonic lane-run number, while
retaining separate IDs for pipelining. A new run resumes immediately from the
durable spool offset. It neither inherits old cleanup authority nor waits for
the old lease's 60-second expiry. No peer byte format or whole-file identity
changes, and this does not activate typed metadata/content.

The catalog commits its next incarnation under its existing exclusive daemon
lock with SQLite synchronous=FULL before lanes start. A real child process exits
immediately after incarnation 2 commits; reopening advances to 3. Malformed and
exhausted values fail closed. Concurrent lane allocation is monotonic without
wall-clock dependence. The worker retains at most 16,384 scope fences and refuses
new scopes when full without evicting a fence; existing scopes still advance.

Worker tests hold a want after its SQL scan and a write after durable byte work,
then admit a newer run. Delayed old requests cannot reclaim, renew or release
the newer lease or alter its source backoff. Stale and unregistered-future
releases are harmless. Returned supersession is `IN_FLIGHT_ELSEWHERE` ownership
contention, avoiding a failure-breaker event and preserving the historical
cancellation code's classification. An actual authenticated bulk reconnect with
real Store/CAS workers obtains a 150,000-byte legacy file before the old source's
two-second read finishes; the first measured availability was 118.45 ms against
the unchanged one-second package bound.

The first complete package run passed 180 checks: catalog 21, real content
transport 15, contracts 41, generation/parser/crash checks 4, legacy lanes 53
and worker object tests 46. The 18 existing long lane qualifications stayed
ignored. The default 10,000-object worker test ran, with 40 writes and 28.6 s
measured transfer work; it is not counted as a mounted-build qualification.
This run preceded the final supersession error-class adjustment. The subsequent
affected checks and exact source/executable stages are recorded in
`test-evidence/wave-6/lease-generations-checks.json`; the unrelated expensive
10,000-object case is not repeated solely for that error-class change.
The final-source rerun passed 105 checks (transport 15, contracts 41,
generation/parser/crash 4 and worker objects 45, with that one already-passed
worker case filtered). The actual returned stale-call class is asserted not to
count as failure. Native legacy reconnect availability was 113.86 ms. Strict
Clippy and formatting/diff checks passed. These are package checks, not final
Wave 6 platform/load/build qualification.

CCR-WP6.3-5 below covers the next retained legacy cross-repository foreground
admission step. The retained acceptor push-offer ownership still needs audit,
followed by mounted range demand and recoverable private saves. The complete legacy
job/spool/lease budget audit also remains; the new fence cap alone does not
establish those bounds. Sustained fairness,
shutdown/reconnect load and large working-set latency still need qualification.
Catalog restore while adopting an existing worker also needs a WP6.6 fence;
restoring an older counter must not silently reuse its lease incarnation.
Metadata descriptor capability negotiation is required before descriptors leave fixture stores;
the object capability probe alone does not satisfy it.

Mounted adapters/write-session recovery, caching/journal, metadata capability
negotiation, deployment fencing and activation (WP6.3–6.6) and integration
qualification remain pending.
In particular, explicit root-pin inventory is not a complete retention audit and
does not authorize deleting any object.

## Reserved retained whole-file demand (WP6.3, in progress)

CCR-WP6.3-5 adds a foreground compatibility adapter to the fixed content
dispatcher. It observes actual Store demand independently of the folder
heartbeat and typed-content activation. Each preparation selects one demanded
identity through indexed SQL with zero busy timeout and a 50 ms reply deadline.
The existing background two-repository gate remains in effect; foreground work
uses the shared scheduler's reserved capacity and bounded network worker pool.
Transfers are capped at 256 KiB with 64 KiB reads/chunks and a charged one-MiB
job. Whole-file parts remain private until the native verifier installs them.

Unique ordered `c` frame IDs share the background lane's parent run generation.
They promote the demanded object without fencing unrelated background work.
Native worker tests resume from the durable offset, refuse an older active
frame's writes/wants, and prove that delayed frame/parent cleanup cannot release
or apply source backoff to its replacement. Existing native privacy tests now
exercise the new bounded source-read operation as well as `objects.read`.

The optional source reservation returns an exact single-use credit before
streaming. A receiver ACK during active streaming retains the source budget;
the active handler also holds an independent permit reference through session
close, expiry or shutdown. An authenticated native transport test covers
abandoned credit, stale ACK, duplicate use, active ACK and byte/credit receipts.
Unsupported old peers retain their historical pull path, without a claim that
they provide the new source reservation.

The actual three-repository regression stalls two nonempty background files
for two seconds under one daemon/peer/lane, then admits new metadata and demand
for the third without changing its published folder heartbeat. Its 1,048,599
bytes require five foreground frames. With typed activation disabled, admission
was 22.06 ms against the unchanged 250 ms bound, and verified availability was
219.43 ms against the one-second bound. The pre-disabled-fixture run was
59.48/248.61 ms. The first successful focused invocation passed 108 checks: transport 17,
contracts 41, parser/catalog crash 4 and native worker 46. Its one previously
passed 10,000-object case was filtered, not counted again. Further regression
results and exact source/executable stages are retained in
`test-evidence/wave-6/legacy-foreground-checks.json`. The final regression run
passed transport 17, retained legacy lanes 53 and session core 59. Across both
invocations, 220 distinct checks passed; the repeated transport tests are counted
once. The 18 existing long lane qualifications remain ignored. Strict Clippy,
formatting and diff checks passed. Source-set SHA-256:
`7f7b8c920be5f070dedfbd4e246d9213fc186045d9e1e525de5dda8caedb60e3`.

Test corrections are explicit: the original naive stall setup accidentally
spent faults on empty historical revisions; the corrected setup verifies shared
empty receipts first and asserts both real background files are still missing
and leased. That corrected pre-adapter case failed the 250 ms admission bound.
The larger transfer initially completed all five frames but its verifier asked
the provider for more than its one-MiB read limit; bounded verification reads
corrected that test. Neither failure was addressed by relaxing a timing bound.

This is short native package evidence for CCR-WP5.INT-1, not the final sustained
fairness/platform/build gate. Still pending: full retained legacy job/spool/lease
budget and acceptor push-generation audits, cancellation within legacy
first-serve hashing, large working-set/idle-poll overhead, mounted range demand,
recoverable private saves and WP6.4–6.6 integration/qualification. Ordinary
typed-content activation remains off.

## Pull-pool ownership across lane counts (WP6.3, in progress)

CCR-WP6.3-6 gives every real peer supervisor an ordered pool identity before
starting its lanes. Lanes and reconnects retain that identity; a replacement
supervisor receives a newer one. The local opaque pull ID appends the pool
counter, including foreground frame IDs. A delayed lane of an older context
cannot acquire a new pool merely because its run starts later. Production
contexts refuse unregistered pools; fixtures without a supervisor use the
documented lazy registration path.

Worker fences now order daemon incarnation, pool and lane run separately. This
closes the count-dependent scope gap: changing the bulk lane count, and later
returning to a previous count, cannot make an old layout current again. It also
handles old-layout requests for a shard not previously seen by the worker.
Canonical classic IDs and the previous qualified form remain recognizable;
after a newer family is claimed, they cannot release its leases or change its
source backoff. The historical opaque session behavior, including its loose
lane-name interpretation, remains for classic requests before a new family
fence is established. A classic opaque name resembling another peer's lane is
still opaque, as it was previously. New qualified requests retain strict peer
and lane validation.

Both worker family/run tables and the daemon context registry have explicit
16,384-entry bounds. Worker fences are never evicted for capacity. Existing
owners can advance while full. The daemon registry only reclaims dead weak
contexts, so a live old lane retains its original identity. Native tests cover
capacity, delayed higher-numbered old runs, count reversal, daemon restart,
stale writes/releases/backoff and malformed/bounded IDs. Expiry of an already
superseded lease no longer backs off a healthy replacement source.

The authenticated native reconnect test now explicitly establishes a verified
empty-history receipt before arming its read stall, then asserts the actual
nonempty file is still missing and leased. The same helper exercises a real
one-to-two-lane resize. The first resize sample restored 150,000 verified bytes
in 9.25 ms while the predecessor's two-second read was still in flight. The
tests retain the one-second limit, verify the newer worker fence after old
cleanup, and exercise the existing foreground-demand regression as well.

Final-source checks passed: authenticated transport 18, native worker 51,
generation/parser/catalog-crash 7 and contracts 41 (117 total), plus strict
Clippy and formatting/diff checks. The latest actual pool-resize availability
was 8.55 ms; unchanged-heartbeat foreground admission/availability was
21.87/214.98 ms for five frames with typed activation disabled. The preceding
broader invocation passed 198 checks, including retained lanes 53 and peer
supervisors 30. Across both stages there are 200 distinct passed checks; the
evidence explicitly separates the earlier lane/supervisor executables from the
final four targets. The 18 long lane qualifications remain ignored, and the
already-passed 10,000-object worker case was filtered. These do not constitute
the final platform/load/build gate.

`test-evidence/wave-6/pool-generations-checks.json` retains commands, raw outputs,
samples, source/executable hashes and stage limitations. It also records the
caught classic-lane compatibility regression and the failed concurrent Windows
link attempt; that attempt was retried only after both earlier tool processes
were observed terminal. Final source-set SHA-256:
`82483906af8734f7a0a8c9649480bbbd93b3d216a2cbbc0659b45afcc546eb1f`.

The pull pool's local `PeerContext` identity is not an authenticated remote
push-pool identity. CCR-WP6.3-7 below adds the separate inbound ownership
lifecycle. Full legacy budgets, hash cancellation, mounted reads,
recoverable saves, final saturation/build/platform qualification and WP6.6
compatibility/activation remain pending.

## Authenticated inbound push ownership (WP6.3, in progress)

CCR-WP6.3-7 replaces stable local inbound push lease IDs with authenticated
session, acceptor pool and per-offer frame identities. Sessions receive their
root before serving requests; old open sessions retain their original root.
Each new offer advances its frame, and the native worker keeps that frame floor
after object leases are released. Reconnect, count reversal, delayed writes,
late wants and exact cleanup cannot restore an older owner. The acceptor orders
pool-count changes when authenticated connections become admissible; the
unchanged legacy hello does not attest a persistent sender-side pool epoch.

Shared weak ownership slots coordinate offers for a worker/peer/lane. The slot
stays alive while an offer or handler can still finish. Publication and short
backoff/status mutations revalidate session/frame and pool, without holding
ownership locks across worker RPC, hashing, streaming or durable I/O. A late
reply cannot overwrite a newer offer, even after that newer offer was consumed.
Old reaper RPCs use exact IDs and no longer block replacement preparation.
There are at most 4,096 slots and 16,384 acceptor peer-pool identities; existing
identities keep working at capacity. Pruning removes expired offers/dead weak
slots and rechecks concurrent creation. Closed-session cleanup cannot remove a
replacement session's offer table entry.

Actual authenticated peer/loopback tests with real Store/CAS workers exercise
a two-second old release, a two-second delayed offer publication, and a bulk
pool resize while the old session remains open. The new owner pushes and
verifies 150,000 bytes with typed activation disabled. Initial same-count
admissions were 0.44/0.43 ms against the unchanged 250 ms bound. Native worker
tests cover frame/session/pool precedence, late writes/releases, the retained
frame floor and another source acquiring a predecessor's remaining object.

The first broader run caught an unknown-size handoff: a new frame's exclusion
did not physically remove its predecessor's old lease. Fresh wants now retire
excluded leases only when their owner is already superseded, without an extra
release-RPC wait, and preserve a current other source's lease. A native test
asserts both cases. Legacy lane fixtures retain exact qualified strings in
their lease tables and release logs; only historical peer/lane assertions
normalize their view. The delayed-reap regression now asserts prompt admission
and that the old release finds no ownership of the replacement.

This work does not establish the remaining full legacy job/byte/spool budget or
close-release helper fanout bounds. Those, hash cancellation, sustained fairness,
mounted range reads, recoverable private saves, caching/change journal,
capability/deployment fencing and final Windows/FUSE/build/load qualification
remain pending. Ordinary typed-content activation remains off.
