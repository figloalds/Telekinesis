# Wave 6 implementation record

2026-10-21. Goal: implement `WAVE_6_PLAN.md`. Work is sequential on the existing
checkout; no merge, push, deployment, live-store migration or production activation
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

The native worker also rechecks ownership at the source-backoff mutation after
an old corrupt-push verdict. A two-second pause after collecting that verdict
allows a replacement to install verified bytes first; the old result then
cannot recreate its memory retry. The replacement completed in 5.43 ms in the
final sample. Durable local-failure SQL projection and same-run foreground
error interleavings still need the full legacy audit.

The complete ownership invocation passed 319 checks. After adding that final
mutation guard and its native regression, the four affected targets passed
127 checks; there are 320 distinct checks across both source stages. Both
stages passed strict Clippy. The evidence retains separate manifests and
executables in `test-evidence/wave-6/push-generations-checks.json`, rather than
claiming the earlier library/lane/supervisor executables contain the last guard.
Final source-set SHA-256:
`a9a0154c02a2829eb2e6dd0d74138452f14ff2e058a22b1f1551466de501166b`.
The 20 ignored checks (18 long legacy lane scenarios and two library probes)
and one filtered, previously qualified 10,000-object worker case remain
explicitly recorded. This is not the final platform/load/build gate.

This work does not establish the remaining full legacy job/byte/spool budget or
close-release helper fanout bounds. Those, hash cancellation, sustained fairness,
mounted range reads, recoverable private saves, caching/change journal,
capability/deployment fencing and final Windows/FUSE/build/load qualification
remain pending. Ordinary typed-content activation remains off.

## Legacy read cancellation (WP6.3, in progress)

CCR-WP6.3-8 makes legacy first-serve SHA-256 verification interruptible between
64 KiB reads. Ordinary loopback object reads and the reserved foreground legacy
read borrow the request's deadline and cancellation/disconnect signal and also
observe worker shutdown. Foreground permit waits check the same request. The
checked Store range seam checks around bounded byte reads and rechecks the
committed shared reference/shareable triad after potentially long verification.
An interrupted hash does not become trusted, quarantine a good file or create
an object retry. The current filesystem call itself remains OS-bound.

Retained background push and inbound pull now pass their stop condition into
the ordered worker read stream. An empty result window wakes to check it, and
stopping the consumer cancels other pending calls. The existing read-window
threads remain bounded by their caller's configured window. Refusals that ran
nothing retain their historical retry, within the original deadline, with
interruptible backoff. Request cancellation preserves unrelated users of the
shared loopback connection. Dial cancellation and the complete retained legacy
thread/byte/spool budget remain unqualified.

The first source stage passed 174 checks: library 85, provider 12, authenticated
transport 23 and native worker 54. Its six actual worker cancellation scenarios
use a 3,145,741-byte untrusted file, ordinary/reserved reads, deadline expiry,
disconnect and force stop. Each entered the actual hash before cancellation;
the tests observed handler/permit return after the injected 250 ms I/O stall,
unchanged availability markers, no object retry/quarantine, and a subsequent
read still requiring verification. That stage preceded the background stream
bridge and sharing-withdrawal scenarios; final qualification is recorded below.

The final source passed 337 checks: library 85, provider 12, authenticated
transport 25, contracts 41, inbound 20, generations 10, retained lanes 53,
native worker 54, peer supervisors 30 and seams 7. All nine new native hash
scenarios passed, as did strict Clippy for all test targets and formatting/diff
checks. External background cancellation and consumer stop returned
in 30.30/35.55 ms while actual hash I/O was paused for 400 ms; the slow handler
was observed returning separately. The six ordinary/reserved deadline,
disconnect and shutdown cases drained in 250.39–252.95 ms with a 250 ms injected
storage stall. Sharing withdrawn during first-serve verification returned no
bytes. Cancellation remains cooperative across individual storage calls.

The unchanged-heartbeat legacy foreground scenario still admitted within
40.35 ms and made all five frames available within 237.30 ms while two other
repositories stalled for two seconds, with typed activation disabled. These
are single controlled samples under the retained bounds, not sustained fairness
or final performance qualification. The 21 ignored checks (18 long legacy lane
scenarios, two library probes and a large provider experiment) and one filtered,
previously qualified 10,000-object worker case remain outstanding.

`test-evidence/wave-6/legacy-hash-checks.json` retains command results, fault
configuration, native samples, source/executable manifests and raw development
failures. Its 364-file manifest includes src/tests/native/ui and Cargo/build
inputs, and therefore differs in coverage from earlier checkpoint
manifests. Source-set SHA-256:
`1324fe18e7a6d400b7e6a56e48d3c3c5290b9606a0d20cb7b3ebbcc0bfc35958`.

Full legacy budgets, bounded close-release work, durable local-failure error
projection, mounted range demand, recoverable saves, the change journal/cache,
compatibility/retention/activation and final platform/build/load gates remain
pending. Ordinary typed-content activation remains off.

## Bounded offer cleanup and closed-session authority (WP6.3, in progress)

CCR-WP6.3-9 replaces a thread per closed inbound session with two process-wide
release workers. Close only marks entries in the existing 4,096-offer table and
wakes the workers. There is no separate queue of copied release batches.
Oldest eligible offers are claimed first; two guarded client release attempts
may run at once, including explicit sweeps. Closed offers are removed when
claimed, silent live offers retain their historical released marker, and every
RPC carries the exact qualified lease ID. The guards survive RPC and unwind.
Workers retry missing startup capacity at most once per second on later offers
and contain an attempt's panic. Their fixed process lifetime is explicit.

The authenticated registration guard also retires its push session before
unregistering. Close and guarded offer/status publication use the same short
pool gate. This prevents a delayed want from publishing after close even when
the close initially saw no offer. Sibling sessions and pool identity remain
independent. An already-admitted data prefix may still become durable under
its original, unsuperseded lineage; it then releases only its own lease.
Closing authority does not discard that valid prefix or create source backoff.

Final source passed 329 checks: library 85, authenticated transport 28,
generations 11, native worker 54, retained lanes 53, inbound 20, peer supervisors
30, contracts 41 and seams 7. Strict Clippy compiled/linted all test targets;
formatting and diff checks passed. The 20 ignored cases (18 long legacy lane
qualifications and two library probes), the previously qualified filtered
10,000-object worker case and the unchanged provider target are not claimed
executed here.

Actual native evidence includes 22 authenticated want/close connections with
both release workers stalled for 1.5 seconds. The additional 20-connection burst
completed in 21.32 ms; peak want admission was 0.45 ms against the unchanged
250 ms bound. Active cleanup and the fixed thread count stayed at two, an
explicit sweep could not bypass the cap, a pending closed offer stayed leased,
and it drained after 1,488.97 ms. Reconnect then installed 150,000 verified bytes
without source backoff. A separate late-publication case closed before offer
publication and left no lease after 1,507.08 ms, without a replacement owner.
The prefix regression paused after admitting 65,536 bytes while no spool offset
existed, closed the session, observed exactly that durable offset and no lease,
then resumed and verified the complete 150,000-byte file. All used legacy
content with typed activation disabled. These are controlled samples, not the
final sustained-load gate.

`test-evidence/wave-6/offer-cleanup-checks.json` retains commands, native samples,
faults, raw outputs, source/executable identity and remaining qualifications.
Its 364-file source manifest matched before and after the final invocation:
`d4249bc4c27548ab0e5dce8d8b2f43b670e3e9e8ff1e456b8f40d02bbea60b5c`.

This change qualifies the release helper count and client attempt ownership,
not all retained legacy memory, job, connection or shutdown budgets. A timed-out
client attempt does not prove its remote handler already stopped. Large-backlog
tails, cleanup fairness across worker instances/repositories, durable failure
projection, the other legacy budgets, mounted saves/ranges, cache/journal,
compatibility/retention and final platform/build/load gates remain pending.
Ordinary typed-content activation remains off.

## Work ownership through error projection (WP6.3, in progress)

CCR-WP6.3-10 extends each legacy worker object lease with a separate work ticket.
The writer retains it through verdict handling and SQL/source retry projection,
after the ordinary lease can already be released. A new grant replaces the
ticket even if it reuses the same session ID or changes only the foreground
frame within one parent run. Finishing the replacement cannot revive the old
ticket. Release/exclusion/expiry retires the matching ticket. Weak object and
source-family registries have 65,536/16,384 entry limits and reclaim only dead
entries; active leases/writers retain their coordination state.

Memory retry mutation checks ticket and run under the worker state lock.
Local SQL retry projection takes source-family/object gates and rechecks
ownership, then releases the shared state lock before SQL. Claims use
nonblocking gate acquisition and explicit `WORKER_BUSY`/resource contention.
Hashing, copying and spool/verification I/O stay outside these gates. The
projection connection uses zero SQLite busy timeout. A verified available row
also wins over a collected local failure within one savepoint, protecting an
independent install and snapshot-upgrade contention. Direct Store failure
semantics and wire/schema shapes remain unchanged.

The broader source passed 336 checks: library 88, authenticated transport 29,
native worker 57, generations 11, retained lanes 53, inbound 20, peer supervisors
30, contracts 41 and seams 7. The implementation was then unchanged while two
stronger worker regressions were added; the final worker suite passed 59 checks.
There are 338 distinct checks across both stages. Strict Clippy for all test
targets and formatting/diff checks passed. The 20 ignored historical/probe
cases, filtered previously qualified 10,000-object case and unchanged provider
target are not claimed executed here.

The actual authenticated native peer/loopback case installed 150,000 verified
bytes in 11.58 ms while an old real spool-open failure paused for two seconds;
the old verdict left no SQL retry. Worker interleavings cover same session,
same-parent foreground frame, another source, independent verified install and
explicit cancellation. The stronger claim-only test admitted the same session
in 1.12 ms and kept its lease/error state intact while replacement bytes were
still absent, so the available-copy shortcut cannot mask a ticket failure.
During an already-prepared one-second SQL commit pause, WAL/read service took
0.97 ms and the shared worker/engine locks stayed free. A different source's
claim of the same object was deferred while another object's claim succeeded
in 1.05 ms. Those retained the 50 ms service bounds. Same-parent stale source
retry was also rejected after a 3.52 ms verified replacement. These are
controlled correctness/service samples, not a full saturation gate.

`test-evidence/wave-6/effect-ownership-checks.json` retains both 365-file source
and executable stages, commands, raw logs and fault/sample details. Broader
source SHA-256:
`94b6a419d841b0509500703ac34b444dbc162ff2b9f6cf0548eaaa2aecc51424`.
Final test-only follow-up SHA-256:
`a97dc84bde59600137d3103009b1e5eb89b3a25c1677ec388b0151d32c9a4447`.
Only `tests/objects_v3_worker.rs` changed between those stages; implementation
and other tests stayed byte-identical, and the CLI binary hash was unchanged.

This closes the examined worker failure-projection windows, not every legacy
effect. Historical reused `a`/`b` or opaque release IDs still lack an exact
per-round token after uncertain RPC timeout; daemon foreground task/status
mutations need their own audit. Full live-capacity/resource stress, legacy
job/byte/spool/connection budgets, long cleanup/fairness/shutdown, mounted COW
reads/recoverable saves, cache/journal, compatibility/retention and final
platform/build/load gates remain pending. Content activation remains off.

The capacity audit must also address the pre-existing legacy `grant` path's
`holders.clear()` at 65,536 entries: an existing-hash regrant at capacity can
forget other live leases' source provenance. The new effect registries do not
waive or qualify that behavior.

## Bounded lease ownership and exact download rounds (WP6.3, in progress)

CCR-WP6.3-11 repairs the legacy capacity gap identified above. An existing-hash
regrant no longer clears other hashes' source provenance. Both lease and holder
tables are bounded at 65,536 keys. Holder cleanup removes only orphaned or
mismatched entries; one bounded grant batch can reclaim up to 256 already-fenced
owners in one pressure scan, without source backoff. Live opaque/current owners
remain retained. A released lease's delayed writer still retains its effect
slot, so exhaustion continues refusing a new key until that proof drains.
Write admission preserves its contention/resource error per refused item,
without spool I/O or source/local failure projection.

Supporting workers now advertise a local, read-only legacy capability independent
of typed activation. A download round keeps one fresh nonce across its two a/b
IDs and all its wants/writes. Worker round floors are independent per slot,
retained after release, and bounded to 16,384 lane scopes. Advancing a cannot
retire b or foreground c work; foreground c can promote background work even
when its own frame counter is smaller. Kept leases and all settlement/exit
cleanup retain their exact acquired session string. This closes the examined
uncertain-release gap for negotiated round ownership.

The daemon caches negotiation by immutable worker instance, up to 16,384
instances per lane run. `UNSUPPORTED_OPERATION` retains the previous stable IDs
so older workers can continue short rounds; transient probe failures are retried
and version mismatch is `PEER_NEEDS_UPGRADE`. The unsupported-worker and
transient-failure cases were exercised with the explicit fake-worker harness.
Real W5 executable interoperation remains a separate qualification. Historical
or arbitrary opaque IDs on unsupported workers keep their previous ambiguity
after an uncertain release; the round-fence promise is conditional on negotiation.

Final Windows release verification passed **350 distinct checks** in one final
invocation: 91 library, 32 content transport, 41 contracts, 20 inbound, 12 lease
generation, 54 lane, 63 worker, 30 peer supervisor and 7 seam checks. Strict
Clippy passed for the library and all test targets; formatting and diff checks
passed. Two opt-in library probes and 18 long lane qualifications remain ignored;
the historical 10,000-object worker probe was filtered from the final invocation.
An earlier draft passed that probe, which does not waive the final retained gate.
The unchanged provider target was not rerun in this checkpoint.

Controlled native samples from that final source:

- Full-table grant tests retained all 65,536 owners, with a
  14.46 ms pressure scan, and reclaimed one fenced owner
  in 0.50 ms. A real Store-backed worker RPC
  test with seeded full in-memory lease/holder tables refused both want/write,
  created no spool and added neither source nor local retry. This is boundary
  evidence, not 65,536 concurrent native transfers or a full memory/load gate.
- A native release handler stalled 2,500 ms past its 2,000 ms client deadline.
  The next round acquired its lease in 0.35 ms;
  the delayed handler then rejected stale authority and could neither release
  the new lease nor add its 60-second hold. The replacement verified 150,000 bytes.
- An authenticated peer download verified 300,000 bytes in
  242.73 ms across 5 observed
  round IDs beneath one unchanged lane run, with a 60-second heartbeat and no
  source backoff. Native a/b floor, foreground promotion, retained-floor and
  full round-scope capacity regressions also passed through real worker dispatch.

The first broader attempt passed library/transport tests but the contract audit
caught an unregistered version error name and a unit-test measurement label.
The version error now uses the existing upgrade-required code; the measurement
label is explicitly listed as non-code. The final invocation reran all nine
targets after that correction. Development failures/results are retained and
are not added to the final distinct count.

A separate intermittent automatic-dispatch assertion was diagnosed rather than
accepted after a lucky retry: the manifest's CAS installation succeeded, then
its receipt-index write returned `SQLITE_BUSY`. Recovery could hydrate the file
without a successful manifest RPC callback. Waiting for that callback counter
could never succeed. The in-process RPC fixture lacks normal worker maintenance;
it now drives the same bounded receipt-recovery component while no frame runs.
The test requires complete byte verification, complete range indexing, verified
public receipt evidence for every job and actual structural/background network
admissions under the unchanged five-second limit. A new native case holds the
WAL writer after CAS installation, observes the rejected COMPLETE and proves
the retained receipt recovers. Production counters and receipt code are unchanged.

`test-evidence/wave-6/round-ownership-checks.json` retains raw outputs, the
365-file implementation/test/build manifest, executable hashes, commands,
controlled samples and the pre-correction source stage. Final source SHA-256
(sorted path/hash records):
`7923e8b41bc4175728ae1f3d048380936e8c0365eabc5462841f9297c93a116b`.

Full WP6.3 physical job/byte/spool/connection/remote-handler budgets and daemon
foreground-task/error/status mutations still require audit. Large cleanup
backlogs, cross-worker/repository fairness, real thread startup failures,
complete shutdown/cancellation and sustained 30-minute load/recovery remain
pending. Mounted COW reads/recoverable write sessions, cache/change journal,
compatibility/downgrade/deployment fences, retention/reclamation and the matched
platform/build/real-user-solution gates also remain pending. The full Wave 6
goal stays active and ordinary typed activation remains off.

Two concrete follow-ups remain in that audit: distinct round IDs can increase
settlement/exit release RPC fanout across the bounded kept set, requiring retired
ID pruning or bounded asynchronous cleanup; and the typed dispatcher's
`SQLITE_BUSY` failure statistics need contextual readiness/recovery treatment.

## Confirmed round cleanup and SQLite readiness (WP6.3, in progress)

CCR-WP6.3-12 addresses the two concrete follow-ups recorded above. The existing
16,384-worker-instance capability cache retains at most two confirmed round
floors per worker. Only a successfully decoded want reply, including an empty
reply, confirms its floor. Allocation, malformed/refused/timed-out/in-flight
calls do not. Delayed older replies cannot regress a confirmation.

Settlement and exit cleanup drop only IDs strictly below a confirmed matching
slot/peer/lane/incarnation/run/pool floor at the same worker instance. Current
and future IDs, other lineages/slots, foreground c and opaque IDs stay retained.
Surviving releases keep their exact acquired IDs. Worker/remote-replica
replacement cleanup retains the old endpoint and route, while a stable ID
actually reacquired on the same worker settles with its current round. Daemon
stopping still preserves worker adoption. Successful wants therefore bound the
producer's surviving round history to its two slots, instead of accumulating a
release session for each old round.

The typed dispatcher now treats zero-wait `SQLITE_BUSY` as contextual readiness:
retry after the existing 25 ms delay, increment retries and leave failure
statistics unchanged. The global/wire error table remains frozen. The forced
post-CAS WAL conflict test requires real receipt recovery, complete verified
bytes/range indexing/public receipt evidence, readiness retries and zero
dispatch failures. The automatic fixture also now waits for dispatcher startup
within its original deadline rather than unwrapping a not-yet-created view.

Verification passed **353 distinct Windows release checks**: 94 library, 32
content transport, 41 contracts, 20 inbound, 12 generation, 54 lane, 63 worker,
30 peer supervisor and 7 seam checks. Strict Clippy passed for all test targets;
formatting and diff checks passed. Two opt-in library probes and 18 long lane
qualifications remain ignored, and the historical 10,000-object worker probe
was filtered. Unchanged provider/content-object targets were not rerun here;
no retained gate is waived.

The native cleanup boundary seeded 4,096 real worker lease/holder records,
negotiated capabilities and advanced actual a/b worker floors through successful
empty loopback wants. The production grouping helper skipped 4,094 retired IDs
and kept two current hashes. An uncapped native fault counter observed exactly
two release RPCs, each stalled 100 ms; cleanup took 201.72 ms
against its 1,000 ms controlled bound, with no source backoff. This is a seeded
worker/protocol boundary, not 4,096 real producer rounds or a complete
shutdown/fairness/saturation gate. The ordinary lane lifecycle/resumption tests
also passed. The forced receipt conflict recovered 1
intent and verified 150,000 bytes with failure statistics
`[0, 0, 0]` and readiness retries `[0, 3, 9]`.

After the nine-target run, exactly one line inside the private library test
module changed: the hit counter became uncapped, so unexpected extra release
calls cannot hide after two hits. All 94 library cases and strict Clippy passed
again. All production bytes and other source files stayed identical; the CLI
and other test executable hashes were checked unchanged. Repeated library
checks count once. `test-evidence/wave-6/retired-rounds-checks.json` retains both
365-file source/executable stages, exact commands, raw outputs and development
failures. Broad source SHA-256:
`c3834f02b7b4108aaf2b22009edd804847a223be487946c2fd9c8bb8702c618c`.
Final test-only observation SHA-256:
`b5566e5fc79847694ad79d7564ab5e16631edaf61fae09a348760aa957a668e0`.

Large current batches still require paged releases. With 4,096 retained hashes
and 256-hash pages, two current sessions can require up to 17 existing attempts;
individual RPC deadlines do not prove a global shutdown-time bound. Complete
legacy physical job/byte/spool/connection/remote-handler budgets, asynchronous
cleanup/fairness, late foreground preparation, other daemon error/status effects
and error episode resolution still require audit. Real old-worker interoperation,
thread startup failure, sustained 30-minute load/recovery, mounted COW/durable
write sessions, cache/journal, compatibility/retention and final platform/build
gates also remain pending. The full Wave 6 goal stays active and ordinary
typed activation remains off.

## Foreground preparation and source-error ownership (WP6.3, in progress)

CCR-WP6.3-13 closes three foreground legacy gaps. A delayed native
`LEGACY_PREPARE` now checks its actual incoming deadline, cancellation or
disconnect, and worker shutdown before selection and grant, and after grant.
A failed final check removes only that exact session's grants. Parent/round
authority never rewinds; the diagnostic proving request retirement runs outside
worker/family guards. Ordinary legacy WANT and direct JSON dispatch do not gain
the full loopback request-context contract here.

Each foreground Task now captures an object/source-bound proof. Concurrent
sources share the epoch before the next verified local install, but cancelling
one attempt leaves other sources current. Proof validation and daemon backoff
mutation share the existing short lock. A daemon verified-install notification
retires all earlier incoming attempts for that object and clears source history.
Dropping a newer proof cannot resurrect an older one. The weak epoch table has
16,384 keys; pressure reclaims only dead entries, and live capacity refuses a
new root. Cancelled in-flight Tasks retain their slot until physical drop.
I/O, verification and cleanup RPCs stay outside this lock. Binding and daemon
cancellation use short-lived child tokens; their hooks cannot accumulate on
the long-lived daemon token.

The Task rechecks ownership before writes, after the peer reply and before
error/success projection. It retires its attempt before cleanup and selects a
source hold only while current and uncancelled; its scheduler reservation stays
held through the existing release attempt. This is not remote-handler retirement
acknowledgement. Local `Progressed` means a private prefix. Only local verified
`Completed`/`Skipped`, after stream and peer-receipt validation, can clear the
whole-file history. A successful partial foreground frame no longer claims a
whole-file install.

Verification passed **359 distinct Windows release checks** in one final
nine-target invocation: 97 library, 35 content transport, 41 contracts, 20
inbound, 12 generation, 54 lane, 63 worker, 30 peer supervisor and 7 seam checks.
Strict Clippy for all test targets, formatting and diff checks passed. Two
opt-in library probes and 18 long lane qualifications remain ignored; the
historical 10,000-object worker probe was filtered. Unchanged provider/content
object targets were not rerun, and no retained gate is waived. Earlier focused
runs count only as development evidence.

The final native evidence includes:

- Six actual loopback prepare cases: a 250 ms stall at server entry or grant,
  stopped by deadline, disconnect or worker shutdown. All retired within
  251.05 ms against the unchanged 1,000 ms controlled bound, left no old lease
  or source backoff, and preserved the replacement lease when the worker stayed
  running. Shutdown cases deliberately have no replacement lease.
- An old foreground source verdict stalled 1,500 ms, then cancellation and an
  independently connected replacement source verified 150,000 bytes in
  128.33 ms against its 1,000 ms controlled bound. The obsolete Task drained its
  scheduler reservation without adding daemon or worker backoff. The replacement
  uses the retained background lane; this proves ownership/availability, not
  that background traffic shares the common scheduler's physical budget.
- A 524,305-byte legacy file retained a private durable 262,144-byte prefix.
  After a real background source failure and a 2,500 ms foreground completion
  stall, the successful partial foreground receipt preserved source history and
  whole-file reads remained unavailable. This directly exercises the previous
  premature-history-clear path.

Three production-helper library regressions cover independent source
cancellation, install/replacement/no-resurrection and the full 16,384-key live
capacity boundary, including cancelled proofs retained by in-flight work. The
capacity test is not concurrent native transfer or a memory/saturation gate.
`test-evidence/wave-6/foreground-owner-checks.json` retains raw logs, commands,
all 365 source/build/test inputs, executable hashes and controlled samples.
The source was frozen before final lint/tests and every input hash checked
again afterward. Its SHA-256 is:
`10ecb3be2a3e3d70562c2d87b82bf5a40dd699daa0d45a28483b58c7307d3572`.

The foreground proof does not yet guard all background/upload/status writers.
Independent local installation still needs a verified-install observer; group
and catalog/source replacement cancellation, permanent c-frame retirement or
replay, reply-lost and release-WORKER_BUSY races, ordinary WANT late grants and
error-episode resolution remain in the audit. Current paged cleanup does not
prove a global shutdown bound. Complete physical legacy job/byte/spool/
connection/remote-handler budgets, common background admission, asynchronous
cleanup/fairness, actual thread startup failure and sustained 30-minute load/
recovery remain pending. Mounted COW reads/recoverable write sessions,
cache/journal, activation/migration/downgrade/deployment fences, retention and
matched Windows/FUSE/WSL build gates (with a real user solution when available)
also remain pending. The full Wave 6 goal stays active and ordinary typed
activation remains off.

## Dispatcher group lifetime and deferred ownership (WP6.3, in progress)

CCR-WP6.3-14 extends foreground ownership to dispatcher group/catalog lifetime
and typed work. Each Task has a weakly linked cancellation child of both its
group and lane binding. Either parent retires that Task without closing its
shared connection or cancelling the other parent. Linked children do not add
per-request hooks to long-lived tokens. The group itself is a dispatcher child.

A single nonblocking refresh builder validates live owners against the complete
folder snapshot and ready source/replica/lane set, including full worker
endpoint and local-device identity. It cancels changed groups before publishing
replacements and reuses unchanged groups. A weak registry retains at most 1,024
current owners, including work outside the 512-repository rotating page.
Page rotation alone cannot cancel that work. Dead/cancelled entries may leave
the registry; a full live registry defers new groups without evicting current
authority. The fixed preparation/network pools and bounded queue continue
owning cancelled physical Tasks while they drain. Selecting a group drops its
page snapshot before I/O.

Admission, completion and error projections now share a short group mutex;
group cancellation returning is a barrier for those mutations. No transport,
worker call, Task destruction or cleanup RPC holds it. Queue producers and
consumers reject cancelled groups, and an obsolete poll result cannot admit
new work. The foreground adapter's object proof consequently retires when
source/catalog/group authority changes, even if its lane stays connected.
Typed reads also check cancellation before failure projection or data install
and ACK any returned parseable frame. A typed Task keeps its scheduler
reservation through the exact-offer release attempt, and the offer keeps its
outgoing turn until that call returns. This fixes the previous release ordering;
it does not acknowledge physical retirement of every remote handler.

The new native catalog-withdrawal cases use real authenticated sessions and
workers. A legacy verdict is stalled for 1,000 ms, then its catalog disappears;
the group retires while its still-executing Task keeps its reservation until
unwind. It cannot record a failure, completion or source hold. Restoring the
catalog verifies 150,000 bytes through the original connection. A typed read
stalled for 1,500 ms likewise retires its destination Task/scheduler/local-worker
reservations and resumes on the original connection. Neither case cancels the
lane or closes its connection. The destination checks do not claim immediate
release of every stalled source handler/credit.

Seven new library regressions cover linked-parent independence, weak retention,
concurrent cancellation, deferred owners across page rotations, dead-owner GC,
the full 1,024-owner refusal boundary and the short projection barrier. Registry
capacity/rotation use the production helper; they are not a native saturation
test with 1,024 repositories. Native worker/device replacement and every
catalog change interleaving are not newly claimed by these two native cases.

Final verification passed **368 distinct Windows release checks** in one
nine-target invocation: 104 library, 37 transport, 41 contracts, 20 inbound,
12 generation, 54 lane, 63 worker, 30 peer supervisor and 7 seam checks. Strict
Clippy for all test targets, formatting and diff checks passed. Two opt-in
library probes and 18 long lane qualifications remain ignored; the historical
10,000-object worker probe was filtered. Unchanged provider/content-object
targets were not rerun. No retained gate is waived, and draft runs do not add
to the distinct count.

In the final native samples, legacy group withdrawal took 43.10 ms against
250 ms, retained the old Task's reservation until actual unwind, then verified
150,000 bytes in 67.99 ms after catalog restoration. Typed destination drain
took 118.86 ms against 250 ms, and replacement verified its 39 bytes in
86.11 ms against 1,000 ms. Both preserved the connection and recorded no
obsolete failure/completion; the legacy case also checked daemon and worker
source backoff stayed absent. These are controlled interleavings, not full
load/fairness or remote-handler retirement qualification.

`test-evidence/wave-6/group-lifetime-checks.json` retains final commands,
individual samples, raw outputs and executable hashes, plus earlier development
stages. All 365 source/build/test inputs were frozen before final lint/tests
and every hash verified afterward. Final source SHA-256:
`8c3c5e2422d62605e2d1d9e3af6fa5527afc26de6721a78d9e8e3ac4a62683da`.

Complete legacy physical budgets/common background admission, bounded
asynchronous cleanup/global shutdown, remaining background/upload/status error
writers, independent local-install observation, per-class error-episode
resolution, ordinary WANT late grants and c-frame/reply-lost/release-WORKER_BUSY
retirement remain outstanding. Executor fairness under complete saturation,
real thread startup failure, old-worker compatibility and sustained 30-minute
load/recovery still need qualification. The mounted-save, performance,
migration/activation/deployment/retention and platform gates above also remain
outstanding. The full Wave 6 goal stays active and ordinary typed activation
stays off.

## Common legacy download admission and staged ownership (WP6.3, in progress)

CCR-WP6.3-15 brings retained legacy incoming streams into the same scheduler
pool as joined object frames. Unjoined batches own a separate random token/
generation rather than an artificial single-object key. The existing eight
jobs/8 MiB, four per peer/repository and class reservations cover both types.
`usage` reports the aggregate; `joined_usage`/`stream_usage` distinguish them.
Each legacy pull requests at most 256 KiB with a conservative 1 MiB reservation.
Global refusal now precedes the local lane window, peer slot and plan mutation;
it returns readiness after 25 ms without a source error or new reader/writers.
The old wire operation, durable offsets and whole-file verifier stay intact.

The client reader, accepted buffered/pending pieces and actual worker write
batches retain independent proofs. Empty writes retain work even without
payload. The last sink proof remains held through its lane-credit hook, so
new global admission cannot race old window accounting. Cancellation or the
old runner's 500 ms exit wait does not return a still-running write's proof.
Dispatcher `started` now measures executor acceptance separately from queue
admission, under the same group projection barrier.

The first broad run exposed a real fairness regression: retaining an old
8 MiB key share with new 256 KiB requests let one wholly stalled source take
both peer background slots. The effective share now uses the capped payload;
a threshold notification wakes re-pull independently of writes/final replies.
The unchanged other-repository bound passed again in 0.83 seconds in its
focused check. A subsequent boundary check caught a rejected third request
temporarily entering the lane's reservation peak. Common admission now occurs
first, and sink proofs remain through credit publication. All failed stages
are retained rather than replaced by successful retries.

The historical test's minimum 8 MiB payload occupancy per lane is explicitly
superseded by Wave 6's common budget. It still completes the same 24/72 MiB
workload within 60 seconds, retains the <=16 MiB bound and requires two pulls
under link pressure. It now additionally requires an exercised >=256 KiB
payload and <=512 KiB actual outstanding payload/window in that fixture. The
focused corrected workload passed in 8.85 seconds. This allocation contract
change does not qualify sustained default-window throughput, and does not
waive ignored historical long qualifications.

Final native controlled samples fill six real source streams across six
repositories while a foreground reader receives actual executor capacity in
58.05 ms and verifies 43 bytes in 67.22 ms, against the unchanged 250 ms bound.
Observed peak common use is seven jobs/7 MiB. Empty and 16,000-byte transfers
stall their actual worker write for 750 ms after the stream ends; both retain
their reservation after cancellation and after the runner has joined, then
drain on physical client-work retirement. The original fixture incorrectly
treated runner join as write retirement; it now observes those separately.
These are client stream/staging/write-call reservations, not whole-process
RSS or remote-handler retirement acknowledgement.

Final verification passed **378 distinct Windows release checks** in one
ten-target invocation: 104 library, 8 scheduler, 39 transport, 41 contracts,
20 inbound, 12 generation, 54 lane, 63 worker, 30 peer supervisor and 7 seam
checks. Strict Clippy for all test targets, formatting and diff checks passed.
Two library probes and 18 long lane qualifications remain ignored; the
historical 10,000-object worker probe was filtered. Unchanged provider/content
object targets were not rerun. No ignored long qualification is waived; the
historical allocation-assertion change above is explicit.

Matched boundary probes ran the retained previous executable and the final
executable on this host with identical fixtures/configuration/faults, one
warm-up and five measured repetitions per case. All 24 executions passed and
are not added to the distinct count. Short-round 300,000-byte availability
median changed from 250.29 to 264.98 ms (+5.9%; ranges 228.08–251.35 and
240.64–314.52 ms). This case uses a 128 KiB lane window. Foreground admission
behind two other stalled repositories changed from 35.35 to 10.04 ms median;
1,048,599-byte availability changed from 249.36 to 207.56 ms. These individual
samples/ranges are controlled boundary evidence, not a p99 or a sustained
default-window bulk/native/W5/W6 mounted-build qualification.

`test-evidence/wave-6/legacy-stream-checks.json` retains all final commands,
source/executable identities, samples, raw logs, failed development stages and
matched repetitions. All 365 source/build/test hashes were frozen before the
final invocation and checked afterward. Final source SHA-256:
`c31e8d7d7a0b056da58346b4dc78cda44c3c85cbf1136fbda272a328916c4fce`.

Complete legacy upload/inbound/source/connection/thread/spool budgets and
remote-handler retirement remain pending. Cross-repository/source fairness
under every partial-stall interleaving, sustained default-window throughput,
actual thread startup failure, bounded asynchronous cleanup/global shutdown,
remaining daemon error/status effects, independent local-install observation,
ordinary WANT late grants and c-frame/reply-lost/release-WORKER_BUSY retirement
also remain pending. In particular, common admission pressure while earlier
sink writes are still retiring can end a download round and incur its 25 ms
readiness retry; this deserves throughput measurement before WP6.3 closure.
Mounted recoverable COW saves, cache/journal, compatibility/migration/retention
and the full platform/build/load gates remain outstanding. The full Wave 6
goal stays active; ordinary typed activation stays off.

## Same-round local retirement and measured download flow (WP6.3, in progress)

CCR-WP6.3-16 addresses the concrete pressure/retry follow-up above. A native
4 MiB transfer with one 250 ms worker-write stall reproduced eight actual
round nonces. After a stream ended, its pending local writer still held common
capacity; treating that as unrelated admission pressure unnecessarily finished
the round, regranted leases and incurred a 25 ms readiness delay.

The round now waits for its own buffered/pending/writing/retiring sink work
when no source stream is active. Without its own work able to return capacity,
it still yields the existing 25 ms retry to other scopes. Cancellation, errors
and the ten-second round cap stay in force. A successful admission clears the
previous refusal flag. A short atomic phase covers proof retirement after
ordinary byte/settlement publication; `CreditsReturned` wakes the consumer
after those proofs drop. The reader likewise drops its proof before publishing
`PullEnded`. The retirement guard resets its phase even on unwind, preserving
another live stream's reservation. No scheduler/sink lock covers I/O, hashing
or the controlled retirement stall. Budgets, payloads, hashes, offsets and wire
messages are unchanged.

Final native samples verify all 4 MiB in 408.38 ms with the 250 ms write stall,
and all 1,048,583 bytes in 354.75 ms with five 150 ms post-credit/pre-capacity
stalls. Both observe one round nonce, one lane run, a 60-second heartbeat and
no source backoff. Complete byte verification uses immutable provider ranges
of at most 1 MiB. The first draft fixture had mistakenly asked for one 4 MiB
range and hit the unchanged provider bound; the corrected pre-fix fixture
verified the bytes and then failed on the eight observed nonces. That fixture
failure and the genuine churn reproduction are retained separately.

The existing 72 MiB write-limited and 24 MiB link-limited workload now records
times while retaining all its completion, deadline, payload/window and pipeline
checks. The instrumented control's production bytes exactly match the prior
qualified `c31e...` source; only two integration-test files differ. Its measured
test file remains byte-identical in the final candidate, and its executable was
preserved before production edits. One warm-up plus five measured repetitions
per binary passed in all 12 process invocations:

- With a 3 ms fake-worker memory-spool write delay, 72 MiB median transfer time
  changed from 6,667.92 ms to 1,151.40 ms (82.7% lower); ranges were
  6,659.53–6,705.45 and 1,143.78–1,162.00 ms. Median throughput changed from
  10.80 to 62.53 MiB/s.
- With a 20 MiB/s, 2 ms latency link proxy, 24 MiB medians were 1,772.29 and
  1,776.77 ms (+0.25%); ranges were 1,763.94–1,781.43 and
  1,771.30–1,781.64 ms. Both kept two pulls active and at most 512 KiB of actual
  outstanding payload/window.

These are authenticated-session, fake-worker memory-spool measurements.
They do not qualify real-disk fsync, cold caches, native/W5/W6 mounted builds,
p99 or W6-BUILD/W6-LOAD. An initial complete control run overlapped a separate
native fixture compilation/test; its raw results are retained but excluded,
and the entire control series was repeated without another task compile/test
running. No individual sample was selectively replaced.

Final verification passed **381 distinct Windows release checks** in one
ten-target invocation: 105 library, 8 scheduler, 41 transport, 41 contracts,
20 inbound, 12 generation, 54 lane, 63 worker, 30 peer supervisor and 7 seam
checks. Strict Clippy for all test targets, formatting and diff checks passed.
Two library probes and 18 long lane qualifications remain ignored; the
historical 10,000-object worker probe was filtered. Unchanged provider/content
object targets were not rerun. No ignored historical qualification is waived;
timing repetitions and draft checks do not add to the distinct count.

`test-evidence/wave-6/stream-drain-checks.json` retains commands, individual
samples, raw outputs, development failures and both source/executable stages.
All 365 final source/build/test inputs were frozen before final lint/tests and
each hash rechecked afterward. Final source SHA-256:
`a1427a88ee1be9bbc4f3d2bcf488ba5dfa0d2160a37a878d1f4bf958bb5d11c7`.

Complete upload/inbound/source/connection/thread/spool budgets, remote-handler
retirement, retained round bookkeeping bounds, all partial-stall/fairness/load
interleavings, real disk/build performance, actual thread startup failure,
bounded asynchronous cleanup/global shutdown and remaining late effects still
need qualification. Ordinary WANT late grants, error/status episodes,
independent local-install observation and c-frame/reply-lost/release-WORKER_BUSY
retirement also remain pending. Mounted recoverable COW saves, cache/journal,
compatibility/migration/retention and final platform/build/load gates remain
outstanding. The full Wave 6 goal stays active; typed activation remains off.

## Bounded round metadata and empty-write completion (WP6.3, in progress)

CCR-WP6.3-17 bounds retained download bookkeeping independently of transfer
volume or the ten-second quantum. One round remembers at most 4,096 distinct
objects, covering its plan/deduplication/begun/failed/corrupt history and sink
object table. Wants limit their item count to remaining room, including
immediately reserving promoted demand entries. Returned overflow leases are
released by exact session without a source hold/error. Accepted work finishes
before the ordinary content yield starts another round; live work is not
evicted. Byte-plan estimates now saturate rather than overflow.

Completed pull records are removed when their single status report is emitted,
instead of accumulating every previous request in the round. At most eight
records may be live/pending; admission refuses a ninth before allocating its
resources. Peak diagnostics are per-round maxima, not total daemon metadata
or RSS. A production-helper test keeps six incomplete predecessors while
cycling 50,000 completed records through one temporary slot. Another fills
the 4,096-object boundary, checks overflow refusal/deduplication and exercises
u64::MAX byte estimates. These are bounded model tests, not native 50,000-file
saturation evidence.

This exposed an existing empty-payload status bug. The actual native worker
write was stalled for 600 ms, but the old code emitted successful
`ExchangeFinished` with zero installed objects as soon as the empty source
stream ended. Accepted empty writes now have independent internal receipts.
The collector waits for source completion, all bytes and those empty receipts;
settlement updates the object count in the same event vector before collection.
Rejected/probe-only frames do not create receipts. Final accounting closes
after sink finish joins/drains or discards its remaining client work. No peer
wire, canonical hash, lease or durable offset changes. The final real
SQLite/CAS/loopback test observed no premature completion and exactly one
installed-object completion, including after joining the runner.

The authenticated fake-worker boundary scenario completed 4,353 distinct
hash-verified objects in 167.27 ms, with all 4,353 completion counts preserved,
a 4,096-object per-round peak, one object-budget yield and two retained pull
records at peak. Its explicit 60-second test quantum isolates the object
boundary; production stays at ten seconds. This is a controlled memory-spool
fixture, not a native repository/build or full aggregate-memory qualification.

Matched flow probes preserved the previously qualified executable and reran
the same 72/24 MiB workloads with one warm-up plus five measured repetitions
per binary. All 12 process invocations passed, retaining the existing deadline,
two-pull and <=512 KiB payload/window checks. Removing only the newly added
metadata-boundary test function reconstructs the control test file SHA-256;
the measured workload/checks are unchanged.

- Write-limited 72 MiB median changed from 1,147.33 to 1,219.47 ms (+6.3%);
  ranges were 1,116.85–1,174.20 and 1,176.57–1,262.42 ms.
- Link-limited 24 MiB median changed from 1,784.41 to 1,800.60 ms (+0.9%);
  ranges were 1,774.63–1,789.04 and 1,778.85–1,969.31 ms.

These preserve the measured cost of the added bounds; they are authenticated
sessions with fake-worker memory spools/delays, not real-disk fsync, cold-cache,
p99 or mounted W6-BUILD/W6-LOAD evidence. No native/W5 regression gate is claimed
passed from this internal control comparison.

Final verification passed **385 distinct Windows release checks** in one
ten-target invocation: 107 library, 8 scheduler, 42 transport, 41 contracts,
20 inbound, 12 generation, 55 lane, 63 worker, 30 peer supervisor and 7 seam
checks. Strict Clippy for all test targets, formatting and diff checks passed.
Two library probes and 18 long lane qualifications remain ignored; the
historical 10,000-object worker probe was filtered. Unchanged provider/content
object targets were not rerun. No ignored gate is waived and timing/draft
repetitions do not add to the distinct count.

`test-evidence/wave-6/round-bookkeeping-checks.json` retains exact commands,
individual samples, raw outputs, the reproduced status failure, development
failures and source/executable identities. All 365 final source/build/test
inputs were frozen before final lint/tests and checked again afterward.
Final source SHA-256:
`4d9530d83976531a3928a73217bb6ffb3379d5848782753ebabb7d4b2b79fbd8`.

Aggregate discovery/kept-scope/connection/thread/spool limits and legacy
upload/inbound/source accounting remain pending, as do physical remote-handler
retirement, all partial-stall/fairness/load interleavings, real disk/build
performance, actual thread startup failure and bounded asynchronous cleanup/
global shutdown. Remaining late effects/error episodes, independent local
installation observation, ordinary WANT late grants and c-frame/reply-lost/
release-WORKER_BUSY retirement still need audit. Mounted recoverable COW saves,
cache/journal, compatibility/migration/retention and final platform/build/load
gates remain outstanding. The full Wave 6 goal stays active; activation is off.

## Request-owned ordinary WANT admission (WP6.3, in progress)

CCR-WP6.3-18 extends the real loopback request deadline and connection/CANCEL
predicate to ordinary `objects.want`. Previously only the foreground legacy
prepare used that context: a timed-out/disconnected ordinary WANT could still
reach its grant mutation after selection returned. Both the shared legacy
cancellation token and worker stopping flag are checked. JSON/direct dispatch,
which has no responder predicate, uses a 60-second deadline and observes worker
shutdown; no connection-disconnect guarantee is claimed for that surface.

The existing selection/grant checks now apply to ordinary WANT as well. The
post-grant rollback tracks only fresh grants from this request. Previously it
also retired leases already held by the same session, including the effect
tickets of earlier writers. Cancellation of a retry now preserves those leases,
expiry/provenance and tickets. The final check/rollback shares the grant lock;
a replacement cannot slip between them. The bounded grant page remains at most
256 objects. Store selection and stop diagnostics run outside the grant lock;
no per-request thread or retained cancellation hook was added. Applied ordered
run/pool/round floors and explicit exclusions remain monotonic.

One new library case uses actual SQLite/CAS state with two missing objects. It
reoffers a previously granted lease while granting the other, observes stop at
the post-grant check, and requires the previous lease/ticket to survive while
the new holder/effect disappears without source backoff. This is deterministic
admission testing, not arbitrary delivery-loss or saturation qualification.

The native loopback case now covers 24 internal scenarios: ordinary background
WANT with qualified a-round IDs and foreground legacy prepare with c-frame IDs;
entry/pre-grant stalls; deadline/disconnect/shutdown; and no replacement versus
an already admitted replacement. Each injects a 500 ms stall (200 ms request
timeout for the deadline case), keeps the one-second retirement ceiling and
checks no old lease/source backoff plus exact unchanged replacement ownership.
Final observed retirement was 499.45–501.40 ms. Its clock starts after stall entry and before
provoking the stop; the deadline case includes waiting for that deadline, and
shutdown includes stopping the server. These are conservative admission/drain
observations, not network p99 or full physical remote-handler retirement.

Development outputs are retained. The original implementation failed the
ordinary WANT request-stop diagnostic observation. That log did not directly
observe an orphan lease and is not described as doing so. Two expanded-fixture
attempts supplied WANT-only fields to strict LegacyPrepare, which rejected the
request before the grant stall; operation-specific headers fixed that fixture.
An initial abbreviated unit name with `--exact` ran zero tests; its later fully
qualified invocation and final full library run executed the case. None of
these draft/focused repetitions increases the distinct check count.

Final verification passed **386 distinct Windows release checks** in one
ten-target invocation: 108 library, 8 scheduler, 42 transport, 41 contracts,
20 inbound, 12 generation, 55 lane, 63 worker, 30 peer supervisor and 7 seam
checks. Strict Clippy across all test targets, formatting and diff checks
passed. Two library probes and 18 historical long lane cases remain ignored;
one historical 10,000-object worker probe was filtered. Unchanged provider/
content-object targets were not rerun; no ignored gate is waived. The existing
full lane flow workloads were retained, but no matched native/W5/mounted build
or long load/performance gate was run for this admission change.

`test-evidence/wave-6/want-cancellation-checks.json` records exact commands,
source/executable identities, final native samples and all development logs.
All 365 Cargo/build/src/tests/native/ui inputs were frozen before final lint
and the ten-target run and rechecked afterward. Relative to the previous
qualified input set, only `src/runtime/objects_rpc.rs`,
`src/runtime/server.rs` and `tests/content_transport.rs` changed.
Final source SHA-256:
`7ebc8975ee2495e38bb1be14aadc99aa1d48dede10b08f11d157cb0d75115ec3`.

Cancellation/reply loss after the final grant check, long store-scan cancellation
slices, c-frame replay and release-WORKER_BUSY retirement still need work;
this change does not provide reply delivery acknowledgement. Remaining late
error/status effects and independent local-install observation are also open.
Legacy upload/source/inbound aggregate jobs/bytes/connection/thread/spool limits,
aggregate discovery/kept scopes, bounded asynchronous cleanup/global shutdown
and full physical-handler/fairness/load qualification remain pending. Mounted
recoverable dirty-range COW sessions, cache/journal, compatibility/migration/
downgrade/deployment/retention and final platform/build/handoff gates remain
outstanding. The full Wave 6 goal stays active; activation remains off.

## Private COW write sessions and durable checkpoints (WP6.4, in progress)

CCR-WP6.4-1 adds `content::write::WriteJournal` and `WriteSession`, a private
provider-independent mutable layer over pinned immutable content. Save intent
captures the operation UUID, branch, workspace/entity/base generation, immutable
base descriptor and content/alive causal parents. It does not reconstruct those
parents from a newer view during recovery. This is a pending save episode;
the future metadata commit path must fence/retire it and allocate a fresh
operation/base for later writes after publishing a revision.

A 64 KiB dirty chunk stores its changed bytes plus an 8 KiB coverage bitmap.
Neither writes nor checkpoints read the base. Verified range reads fetch only
required uncovered base bytes through the provider. Logical shrink clips both
patch coverage and base visibility, so growth produces zeros without resurrecting
old bytes. No full-base staging file is created. The local overlay encoding is
not a canonical chunk/manifest or a peer wire type. A real LocalContent/CAS case
checks immutable base isolation, private edits, live pin lifetime and recovery;
an unavailable fake 1 GiB base exercises full/partial patches without hydration.

The separate `write-sessions-v1.sqlite` journal maintains distinct working and
saved maps. Ordinary mutations use NORMAL transactions; they do not acknowledge
a recoverable close. Explicit checkpoints verify pending payloads and atomically
commit the saved map and scoped intent with FULL durability. Later unflushed
writes/truncations cannot replace that durable selection. Resume discards those
uncheckpointed edits, retains the operation/parents and installs a fresh owner
inside its transaction. Old handles and delayed checkpoints are fenced; newer
checkpoints make old preparation selections fail stale. Cancel after FULL commit
returns an error while retaining the durable checkpoint.

The journal charges 73,728 bytes per distinct pending chunk, enforcing a shared
64 MiB ceiling before allocation and a limit of 64 dirty intent groups. Pressure
checkpoints synchronously before retrying. The real SQLite/WAL byte-pressure
case used distinct sparse eight-byte patches, not one deduplicated payload for
every index. Peak charge was 67,092,480 bytes under the 67,108,864-byte ceiling;
the group-pressure phase ended with three dirty groups under its 64-group limit.
These are private journal limits, not proof of the still-pending four-worker/
64-job canonical executor, total worker RSS or a bounded saved-disk inventory.

Records are bounded/checksummed, metadata proofs include operation/chunk scope,
and payloads are hash-verified. Oversized records are rejected before copying.
Private SQLite lock waits poll at 1 ms; VM progress hooks check cancellation and
deadline every 1,000 instructions and are removed by the lock guard. A real
recursive SQL trigger was interrupted at its 80 ms deadline (80.06 ms observed,
500 ms test ceiling), rolled back without replacing the old checkpoint, and
allowed a subsequent successful checkpoint. Blocking OS I/O remains OS-bound.
Providers run outside that journal lock; metadata engine locks must not span
these operations.

Repeated overwrites retained two payloads while the prior checkpoint was pinned
by the saved map, then one after the next checkpoint. In that 199-write fixture,
the database/WAL file lengths were 266,240 / 2,163,032 bytes; these are file-length
gauges, not device-write or fsync counters. A cursor-driven reclaimer examined
402 indexed payload keys in pages of at most 256, removed 400 orphan payloads
and preserved both saved and current data. Pending checkpoint enumeration is
also paged; consumers must reconcile again after changes and include those base
roots in retention. Complete runtime retention/startup cleanup is not wired yet.

Seventeen new tests include a 240-mutation seeded byte oracle, shrink/grow/holes,
stale and cancelled reads, owner replacement, late checkpoint fencing, corrupt
payload/oversized intent rejection, incompatible journal versions, cancellation
after durable commit, resource backpressure, native CAS and two real child-process
exits. Exit before checkpoint recovered the previous byte value 31; exit after
FULL checkpoint recovered 32. Both children exit 86 before destructors and are
covered through the parent test; they are not counted as extra successful tests.
This is process-exit/WAL recovery evidence, not a power-cut or mounted close claim.

The matched internal 4 KiB probe used one warm-up plus five measured repetitions
at each selected base size, with the same API and an unavailable fake provider.
The timer includes write and FULL private checkpoint, excluding setup/readback.
Each mode retained exactly one 73,728-byte overlay payload and read zero provider
bytes. Medians were 1.8330 ms for a 1 MiB selected base and 1.6473 ms
for 1 GiB; ranges were 1.6318–3.1347 and 1.5929–1.9081 ms. These are real
SQLite private-checkpoint measurements over fake descriptors, not real 1 GiB
hydration, canonical chunk/manifest amplification, native/W5/mounted builds,
p99 or full load qualification.

Final verification passed **168 distinct Windows release checks** in one
six-target invocation: 108 library, 8 admission, 6 content-format, 17 object,
12 provider and 17 write-session checks. Strict Clippy across all test targets,
formatting and diff checks passed. The four ignored items are two library
opt-in probes, the provider real-1-GiB canonical amplification qualification,
and the subprocess helper explicitly run by its parent. No qualification gate
is waived. Unchanged legacy integration targets were not rerun; the previous
386-check report belongs to its earlier source/dependency fingerprint.

The new rusqlite hooks feature uses the already cached 0.37 dependency offline;
Cargo.lock is unchanged. A first compile caught guard coercions, strict lint
caught `is_multiple_of` style, and a complete first 168-check run passed before
the audit added final cancellation checks after SQLite lookups/no-op returns.
Final strict lint and all six targets were rerun after those changes. Earlier
passes and focused repetitions do not increase the distinct count.

`test-evidence/wave-6/write-session-checks.json` retains exact commands, all
samples, child exit evidence, raw development/final outputs and executable
identities. All 367 Cargo/build/src/tests/native/ui inputs were frozen before
final lint/tests and checked afterward. Relative to the previous qualified
input set, only Cargo.toml, the content module registry and the new source/test
files changed. Final source SHA-256:
`7079c513a59a76828997e349626a535dfdeee90c7fa842c1410dc6dabab0e751`.

Canonical chunk/manifest preparation including resize, metadata/outbox generation
and causal/idempotency checks, proven recovery retirement, namespace/create/
delete/rename ordering and per-operation corrupt-work/backpressure policy remain
open. The four-save-worker executor, pending/unused-session cleanup, worker
startup/shutdown and full root-retention integration are also required. No
mounted callback uses this module yet: WinFsp cleanup/FlushFileBuffers and FUSE
FLUSH/FSYNC behavior and promises remain unchanged. ENOSPC/power-loss, complete
adapter recovery and all final platform/build/load/amplification gates still
need qualification. WP6.3 aggregate legacy budgets/late effects and WP6.5/6
cache/journal, compatibility/migration/activation/retention/handoff work also
remain outstanding. The full Wave 6 goal stays active; activation remains off.

## Canonical checkpoint preparation and complete-map integrity (WP6.4, in progress)

CCR-WP6.4-2 adds `WriteSession::prepare` and opaque `PreparedSave` pins. A selected
durable checkpoint can now produce canonical direct/chunk/manifest content.
The builder classifies indexed spans, reuses intact hashes from the captured
base or verified ancestor pages, and retains one page per tree level. Resize
crosses direct/manifest and root-depth boundaries without a whole-base copy;
only changed paths and boundary chunks are materialized. Zero-filled growth
uses a bounded shared zero DAG. Missing required structure returns its object
key for hydration; fully replaced output needs no old structure or data.

Needed legacy bytes are converted from the local whole-file object in one
sequential input stream with final SHA-256 validation. Conversion occurs before
builder buffer admission, with selected-owner checks during waits and reads.
One conversion pin is cached per live session/target; repeated preparation in
the fixture read 131,072 bytes (containing chunk plus duplicate verification),
compared with 2,162,821 bytes for the first 2,097,285-byte legacy conversion/
preparation. Parallel legacy preparations finished within their five-second
deadlines without nested buffer permits. This cache is not a persistent
conversion index; restart can require conversion again before metadata commit.

The six-size/four-stage matrix compares prepared descriptors with the independent
streaming importer, including shrink/grow/holes and root-shape transitions.
Native CAS tests preserve missing unrelated chunks, avoid fetching a fully
overwritten old chunk, and reject missing structure rather than staging the
file. Completely replaced output also prepares from unavailable chunked and
legacy bases. A logical 2^48-byte zero-filled version used one data object and
four manifest pages and supported a later verified partial edit. That is a
content/provider DAG test, not a 256-TiB mounted allocation. Delayed/cancelled
preparations retain recovery records and return no successful preparation.
Stale preparations waiting behind six unrelated background buffers retired in
5.062–12.186 ms, below the 250 ms bound, before those producers were released.

Auditing this path found and reproduced a flaw in the preceding private journal:
a deleted saved-map row could be interpreted as unchanged content and return a
successful preparation of the old bytes. Presence-only payload checks were
insufficient. v2 records bind complete sorted working/saved maps with SHA-256
and counts; scope seals and an inventory count/XOR also detect vanished intents.
Mutations verify existing maps before replacing their proofs. Reads/preparation
verify selected maps, and preparation checks again after building objects.
Missing metadata/payloads are corruption rather than holes. A valid saved
record/map remains recoverable when a newer buffered header is damaged; resume
fences old handles with a fresh UUID and discards only uncheckpointed work.

The private family file remains `write-sessions-v1.sqlite`, now at user_version 2.
Experimental v1 and future versions are refused without rewriting/discarding
them. The v1 refusal fixture preserves its old record bytes. No live migration,
metadata schema, canonical encoding or peer wire change is introduced here.
The normal runtime still does not construct/use this journal or preparation path.

Reclamation now reconciles scope/map integrity before deleting any payload,
processing at most 256 combined scope/map/payload records per call. Opaque
cursors bind the journal instance, SQLite data_version and total_changes;
foreign commits, local mutations and cross-instance cursors restart the audit.
A foreign deletion of an already-audited prefix was detected and all 300
payloads retained. Missing map or whole-intent cases also retain their bytes.
The 400-orphan fixture still completes in bounded pages while preserving both
saved and current references. Opportunistic replacement maintenance drives one
step, but the runtime must still drive remaining pages and qualify progress
under continuous mutation/load.

A second reproduced issue coupled optional cleanup failure to healthy writes:
another damaged scope made an unrelated file write fail after its mutation had
already committed. Maintenance now retains one bounded independent error episode
instead. The actual healthy write/checkpoint remains successful and recoverable;
explicit cleanup still returns its error and keeps bytes. Cancellation/deadline
does not create a failure episode. The episode clears only after a complete
healthy pass, demonstrated after restoring the damaged metadata. Worker status
wiring is still required; no mounted/user-visible recovery workflow is claimed.

The real prechunked 1 GiB pipeline probe imports a genuine uniform byte stream,
then copies/syncs its small deduplicated CAS graph into an isolated candidate
store. One warm-up plus five measured pairs compare the existing canonical-only
`LocalContent::overwrite` with write + FULL private checkpoint + preparation.
Every pair produced the identical descriptor, one new 64 KiB data object and
two manifest pages. The candidate read one 64 KiB base chunk and four structural
pages, with 236,064 canonical hash-accounted bytes. The two-boundary case created
exactly two data objects and left the base unchanged.

- Canonical-only control median: 5.6009 ms; range 5.5089–5.8276 ms.
- Journal/checkpoint/preparation median: 13.2294 ms; range 12.9319–17.6553 ms.
- Measured ratio: 2.362x. The additional recovery-intent FULL transaction
  and integrity validation are absent from the control; this cost is retained.

The input is uniform/deduplicated, not arbitrary 1 GiB user content. These
canonical file counters exclude private overlay/SQLite metadata hashing, WAL
page/device writes and actual device fsync counts. This is no native/W5/mounted
build or equivalent-durability regression pass, and no build/load/physical-I/O
gate is waived. The historical direct provider 1-GiB contained/cross-boundary
probe was separately executed and passed on the same final source.

Final verification passed **185 distinct Windows release checks**: one
six-target invocation with 108 library, 8 admission, 6 format, 17 object,
12 provider and 33 write-session checks (184 total), plus that explicitly
selected historical provider probe. Strict Clippy across all test targets,
formatting and diff checks passed. The main invocation reports four ignored
items: two library probes remain unexecuted; the provider probe was explicitly
run, and the subprocess helper is exercised twice by its parent (exit 86), not
counted as additional successful tests. Eighteen long lane and historical
10,000-object worker probes were not selected or waived; the old integration/
platform suite is not represented by these package checks.

`test-evidence/wave-6/canonical-prepare-checks.json` retains raw failures for
missing-map silent fallback and unrelated cleanup failure, intermediate
182/183-check runs, exact commands, all samples and source/executable identities.
Draft repetitions do not increase the distinct count. All 367 source/build/test
inputs were frozen before final lint/tests and rechecked afterward. Relative
to the preceding qualified input set, only `src/content/local.rs`,
`src/content/write.rs` and `tests/content_write.rs` changed.
Final source SHA-256:
`3fac865517f723ccbbe3ae901d9e7b7a2e1e0aececb5b6879d9dbcd216331c05`.

Canonical preparation now exists, but atomic entity/generation/causal/idempotency
metadata/outbox publication, receipt-owned retirement and safe operation/base
rollover while newer edits continue remain open. Namespace/create/delete/rename
ordering, the four-worker/64-job executor, independent journal read/write
coordination under slow fsync, startup/shutdown, full retention and maintenance
status integration still need implementation/qualification. No WinFsp cleanup/
FlushFileBuffers or FUSE FLUSH/FSYNC promise changed. Full adapter crash matrix,
ENOSPC/power-loss, device amplification, native/W5/mounted build/load and final
platform gates remain pending, alongside WP6.3 legacy budgets/late effects and
WP6.5/6 cache/change-journal, compatibility/migration/activation/retention/handoff.
The full Wave 6 goal stays active; activation remains off.


## Metadata save ownership and atomic publication receipts (WP6.4, in progress)

CCR-WP6.4-3 adds the metadata-owned part of canonical save publication in
`src/core/content_saves.rs`, initialized by `core.rs` and observed by the
existing transactional commit path. It remains restricted to isolated
`fixture:content_v1` stores. Normal mounted/runtime sessions do not call the
new APIs; activation stays off. No live store was opened/migrated for this work.

`begin_content_save` captures an existing live file's persistent workspace UUID,
branch, per-entity local generation, operation UUID owner, base descriptor,
content/alive parents and immutable event template. Device, clocks, applicable
basic-register update and the namespace frontier are retained rather than
reconstructed during recovery. Checkout epoch is read from durable config,
separately from the entity generation and stale in-memory Store fields.
The frontier is bounded at 256 IDs and each checksummed envelope at 64 KiB.
Limit failures roll back ownership/config/operation writes atomically.

Starting a newer operation fences old publication through both the local
generation and UUID owner. Deleting/recreating the counter at the same numeric
value cannot transfer an old operation's right. Ordinary local content, alive,
location and basic commits invalidate the owner in their event transaction;
foreign Store handles exercise rename, delete, replacement, permission changes
and legacy writes. A remote concurrent descriptor remains a causal competitor,
with both alternatives preserved by the independent derivation oracle.

`select_content_checkpoint` persists the complete selected checkpoint record.
Older/different equal sequences cannot replace it, and a newer selection fences
an old prepared callback. Publication checks the pin belongs to this native CAS,
then revalidates workspace/checkout/entity owner and selected sequence under
`BEGIN IMMEDIATE`. Event, admission metadata/outbox and checksummed receipt
commit together under FULL/WAL. Nested caller transactions and weaker SQLite
durability are rejected. No journal lock, file hashing/copying or hydration is
held in that transaction.

A missing persistence check was reproduced on exact source `b639c398...`:
a real SQLite trigger ignored the receipt UPDATE and the prototype returned
successful `SaveReceipt`. The retained failing test/log and full source manifest
are in the evidence. Exact affected-row and persisted selection/receipt checks,
plus event/root/outbox evidence validation, now reject ignored or discarded
writes. Both before-update IGNORE and after-update discard cases pass. Failures
roll back event/outbox/receipt and ownership, leaving the journal intact.

The operation UUID is the immutable event ID. Lost-reply replay returns the
matching original receipt before consulting obsolete local ownership, so it
cannot reapply an event after later checkout/deletion. A short WAL read snapshot
checks receipt evidence against the stored immutable event and root/outbox;
unknown operation returns absence. Serialized receipt bytes or absence are not
journal deletion authority. Recovery data remains retained in every fixture.

Four actual child process exit-86 boundaries cover before metadata, event
written, receipt written and committed. Fresh Store/journal ownership rebuilds
canonical content and commits/replays once. Real WAL writer acquisition polls
deadline/cancel with 1 ms busy retries and restores the prior timeout. A long
recursive SQLite trigger is interrupted at the request deadline and rolls back
all publication rows. A 500 ms injected metadata pause permits independent
journal mutation and saved-checkpoint read within 250 ms. This proves lock
independence, not isolation from actual device fsync latency or panic/unwind
cleanup. Native device fsync is not interruptible by the SQL VM hook.

The expanded first qualification exposed 24 unregistered write/publication
codes, including private-journal codes from the preceding package: 269 cases
passed and the global code-literal audit failed before the final three targets
ran, on exact source `672c0e8f...`. That result is preserved. The Wave 6 error
extension now classifies superseded work as Contention, stable capture/sequence
bounds as NeedsUserAction, unsupported private versions as Incompatible and
corrupt/lost publication or violated invariants as Fatal in their own save/
journal/connection scope. Historical classes remain unchanged. Contract tests
verify stale work does not count as failure and damaged saves do not auto-retry;
actual runtime status scoping is still required.

The publication cost probe uses one warm-up and five measured pairs in the
same build over identical 19-byte canonical descriptors. Control commits the
captured typed event plus admission outbox; candidate adds ownership/selection
validation and durable receipt/readback. Both use FULL/WAL and include the new
mutation observer. Capture, journal checkpoint, metadata selection and canonical
preparation are outside both timed regions; canonical logical counters remain
unchanged across candidate publication.

- Event/outbox control median: 1.4015 ms;
  range 1.3239–1.4707 ms.
- Publication with receipt median: 2.5643 ms;
  range 2.4290–2.8377 ms.
- Measured ratio: 1.830x, retained as component cost.

This is not an old-executable, native/W5/mounted regression or full-save build
comparison. Additional capture/selection recovery transactions are not free;
full-pipeline batching/latency, physical SQLite/WAL/device accounting and matched
build/load gates still need measurement. No gate was relaxed by these samples.

Final verification reports **303 passed cases**: one twelve-target
Windows release invocation (302 total) and the separately selected
historical provider 1-GiB contained/cross-boundary probe. Counts by target:
lib: 108, activation_inbox: 22, content_admission: 8, content_format: 6, content_objects: 17, content_provider: 12, content_saves: 23, content_write: 33, contracts: 41, derive_oracle: 15, replication_regressions: 9, startup_snapshots: 8. Historical targets retain their existing helper test cases; the new
publication helper runs through its parent at four exit boundaries and is not
counted again. Strict Clippy for all library/test targets, formatting and diff
checks passed. The raw ignored declarations are retained; library and long
oracle opt-ins, long lane/10,000-object worker and unselected integration/platform
qualifications were not waived or represented by this package run.

`test-evidence/wave-6/save-publication-checks.json` retains commands, source and
executable identities, raw failures/development stages, all final outputs and
individual samples. Draft repetitions do not increase the final count. All
369 Cargo/build/src/tests/native/ui input hashes were frozen before final
lint/qualification and rechecked afterward. Relative to the preceding qualified
source, five existing inputs changed and two were added: core registry/commit,
write-record validation visibility, Wave 6 error taxonomy/contract assertions,
and the new publication module/tests. Final source SHA-256:
`fa33bf277f83613b8496594e73586c1ccd8cdac22d715c8929dafe5a5539fcda`.

The metadata selection is a coordinator-owned authority, not an atomic observer
of the mutable private journal. Runtime integration must order checkpoint
selection, preparation, publication, resume, receipt adoption and rollover.
One operation publishes at most one checkpoint. Later buffered/durable edits
remain in the old journal; safe base/operation rollover and receipt-owned
retirement have no API yet. Capture currently covers existing live files,
not pending create/delete/rename recovery or mounted namespace semantics.

Next work remains the real session coordinator, panic/unwind ownership cleanup,
four-worker/64-job executor, save batching, off-lock slow metadata preparation,
independent journal read/write coordination under actual slow FULL fsync,
unused-operation/pending-root retention and scoped status integration. WinFsp/
FUSE close/flush/fsync, complete crash/ENOSPC/power-loss, total amplification,
two-peer/WSL/native/W5/mounted build/load and soak qualification remain open,
alongside WP6.3 budgets/late effects and WP6.5/6 change-journal/cache/compatibility/
migration/activation/retention/handoff. The complete Wave 6 goal stays active.


## Publication unwind and failed-connection recovery (WP6.4, in progress)

CCR-WP6.4-4 closes a reproduced ownership leak in the metadata publication
component. On exact 369-input source `ffa4275e...`, an injected panic before
metadata returned through the fixture's unwind handler while the real SQLite
connection still held its outer writer. The retained failing assertion is
`writer leaked after content.save.before_metadata`. The before manifest and log
are preserved; no later binary hash is attributed to that earlier source.

The wrapper now catches unwind only for cleanup: it clears the request's SQL
progress handler, undoes tentative publication/cache state, releases the writer
and resumes the original panic payload. Four panic boundaries verify no
before-commit event/outbox/receipt survives, while after-commit replay returns
the original receipt once. Cancelling the old request after cleanup no longer
interrupts a new independent recursive SQL query; busy timeout is restored and
a second connection can acquire the real writer. An actual SQLite commit hook
rejects COMMIT, exercising automatic transaction rollback and cached visibility
before a successful retry. This is not a power-loss/device-failure simulation.

Actual SQLite authorizer denial of ROLLBACK exercises both returned failure and
unwind after event writing. The failed Store is marked unusable in correctness
state outside disposable caches, its derived caches are cleared, and cleanup
attempts an empty in-memory replacement plus close of the real connection. A
new real writer and fresh Store/journal owner then recover and publish once.
Secondary replacement/close failures remain attached to rollback/original
failure; unwind logs the secondary episode and preserves the original payload.
Callers must discard the failed handle even if secondary cleanup fails.
`SyncStorePool` now discards marked returns despite autocommit on the empty
connection, and the next checkout obtains a healthy Store. It does not reconnect
and retry an ambiguous save automatically. Recovery checkpoints are not retired.

Required operation/capture/owner/workspace/epoch/event records that disappear,
have the wrong SQLite type, exceed the record cap or fail validation now carry
`CORRUPT_CONTENT_SAVE`/Fatal. They no longer surface as unknown retryable SQL
errors. Optional receipt lookup for an unknown operation still returns absence.
An actual SQLite authorization failure retains its typed rusqlite cause rather
than a corrupt-content label; real contention/interruption/deadline cases keep
their own outcomes. The quarantined-handle code is explicitly Fatal. No new
status retry loop or unrelated engine quarantine is introduced.

A private branch publishes bytes identical to another prepared descriptor while
retaining private/non-publishable admission, POSIX mode and creation time. Both
private recovery intents remain retained. Canonical encodings, the private v2
journal, metadata schema, peer messages and activation policy are unchanged;
only volatile Store ownership, error normalization and pool return behavior
change. The real mount/runtime does not yet use this save component.

Final verification passed **292 cases** in one eight-target Windows release
invocation: lib: 108, activation_inbox: 22, content_admission: 8, content_saves: 30, content_transport: 42, content_write: 33, contracts: 41, startup_snapshots: 8. The new publication target reports 30 successes and one
ignored helper; its parent invokes the helper at four real exit-86 boundaries.
The write helper is likewise covered through its parent. Two library opt-ins
remain ignored. Strict Clippy on all library/test targets, format and diff checks
passed. The earlier 303-case source/probe/oracle result remains a separate report;
unselected provider, oracle, replication, long lane/worker and platform runs are
not attributed to this new source or waived. Draft repetitions add no successes.

The same-build publication probe retains one warm-up plus five paired samples
over identical 19-byte descriptors, with unchanged logical canonical counters.
Control event/outbox median is 1.5755 ms (range
1.3219–1.7016); candidate with receipt
median is 2.1393 ms (range
1.9866–2.6958), ratio
1.358x. Both use FULL/WAL. Capture/checkpoint/selection/canonical
preparation are excluded; this is component cost, not a full-save/native/W5/
mounted build regression pass, physical device accounting or a relaxed gate.

`test-evidence/wave-6/save-recovery-checks.json` retains the commands, outputs,
faults, before/final source identities, current executable hashes and all
individual samples. All 369 Cargo/build/src/tests/native/ui inputs were frozen
before final strict lint/qualification and rechecked afterward. Exactly five
existing inputs changed relative to the preceding qualified source: core Store,
publication module, error taxonomy, pool return and publication tests. No input
was added/removed. Final source SHA-256:
`df0bc121936d337b6d8b2f445c37b8cca236560164ecb90bd9f0416cb7ed553c`.

The next required step remains the actual session/metadata coordinator and safe
base/operation rollover with receipt-owned retirement while later writes survive.
Current v2 saved-map scanning and reclamation/rebase behavior still need fixed
local-work/amplification qualification after large and overlapping saves; a
single-patch/19-byte publication fixture is not evidence for that workload.
Pending create/delete/rename recovery, four-worker/64-job execution/batching,
off-lock slow metadata preparation, real slow-fsync journal coordination,
pending-root/unused-operation retention and scoped status remain open. Empty
replacement allocation/OS close failure, actual ENOSPC/power loss, global
shutdown bounds and full WinFsp/FUSE/two-peer/WSL native/W5/W6 build/load/soak
gates remain unqualified, alongside WP6.3 budgets/late effects and WP6.5/6
cache/change-journal/compatibility/migration/activation/retention/handoff. The
complete Wave 6 goal stays active and activation remains off.


## Exact receipt history and metadata continuation (WP6.4, in progress)

CCR-WP6.4-5 adds the metadata half of a save-generation handoff. The immutable
`content_save_history` archive retains bounded checksummed capture, selected
checkpoint and receipt records, indexed by operation/checkpoint and uniquely by
operation/generation. Publication commits and verifies the archive together with
event, root metadata/admission outbox and current receipt in FULL/WAL. Ignored or
discarded history cannot acknowledge a save. No committed event is rewritten.

Exact `content_checkpoint_receipt` lookup and old committed retries now survive
later generations without replacing the latest capture or selection. The old
operation-only lookup describes the current generation; its absence must not be
used as historical receipt absence or deletion authority. Missing parent history
proved by a successor is corruption. Pre-archive current receipts can be backfilled
only after validating their actual immutable event/root/outbox evidence.

`continue_content_save` archives/verifies the committed parent, claims the next
local entity generation and records one immutable successor capture/mapping in a
FULL/WAL transaction. The operation UUID stays stable for future journal adoption
without renaming all patches. Initial event UUIDs remain unchanged; successor IDs
hash a versioned domain, operation/generation and predecessor checkpoint digest.
One generation publishes one checkpoint. Its byte base is the actual committed
descriptor and its content/alive parents are the committed parent's event, even
when remote competing content is currently visible. The new frontier/clock/basic
template is captured once; lost-response replay does not recapture newer state.

Local rename/delete/basic/new-operation and checkout changes fence creation.
Existing handoffs can be retrieved as immutable recovery evidence after later
generations, validated against current or archived child captures through indexed
lookups. Retrieval is not permission for stale publication. Record persistence,
ownership, successor mapping and parent history are checked before acknowledgement.
Uncommitted parent is Contention; lost continuation persistence is scoped Fatal.

The opaque, non-deserializable `SaveContinuation` binds previous checkpoint,
next intent, canonical native store path and a retained base pin. It is not byte
availability proof, journal deletion authority or a completed mutable rollover.
The v2 private journal remains unchanged and cannot adopt that next intent yet.
Tests deliberately retain the original journal and use separate fixture journals
for subsequent metadata generations. The original later checkpoint/bytes remain
readable and are not discarded by an older receipt or continuation.

Ten new parent cases cover exact/multi-generation receipt replay, latest-selection
isolation, later original-journal work retained, remote content alternatives and
the causal oracle, local/checkout fences, discarded writes, corrupt/missing
history/mappings and initial capture byte-codec compatibility. Initial capture
serialization omits the optional predecessor field, preserving its previous bytes
and checksum; this is not an actual old-executable upgrade/downgrade rehearsal.
Four additional real child exit-86 boundaries cover archive writing and before/
written/committed continuation, alongside four existing publication boundaries.
Continuation unwind and real authorizer-denied rollback preserve the committed
parent and recover through a fresh Store. Helper invocations are not extra passes.

Final verification passed **227 reported cases** in one six-target Windows
release invocation: lib: 108, activation_inbox: 22, content_admission: 8, content_saves: 40, contracts: 41, startup_snapshots: 8. The publication target has 40 successes and one
ignored helper, exercised by its parents at eight process-exit boundaries. Two
library opt-ins remain ignored. Strict Clippy across all library/test targets,
format and diff checks passed. Earlier source totals and unselected provider,
write, transport, oracle, replication, long lane/worker/platform qualifications
are not attributed to this source or waived. Draft repetition adds no successes.

The publication probe retains one warm-up and five measured same-build pairs on
identical 19-byte descriptors. Control event/outbox median is
1.4044 ms (range 1.3191–1.6699);
candidate with receipt/history median is 2.7496 ms (range
2.4869–3.3152), ratio
1.958x. Both use FULL/WAL; capture/checkpoint/selection, canonical
preparation and continuation are outside timing. Logical canonical counters stay
unchanged. These are component costs, not native/W5/mounted or full-save build
regression, physical-device amplification or a performance gate adjustment.

`test-evidence/wave-6/save-continuation-checks.json` retains commands, outputs,
development stages, samples and current source/executable identities. All 369
Cargo/build/src/tests/native/ui inputs were frozen before final strict lint/
qualification and rechecked afterward. Only publication module, error extension
and publication tests changed; no input was added/removed. Final source SHA-256:
`f472823b02ae98f201affcfe76e83c2f6dceb1d782cce8b3d87ff0225b67b8bd`.

The next receiver is a correctness/performance requirement, not a small base
pointer replacement. Current v2 saved maps contain cumulative patches against
the original base, and map verification scans those rows. To adopt the canonical
parent while retaining later edits, it must identify later patch generations,
atomically fence old handles and preserve truncate/grow zero semantics without
rekeying/copying all historical patches under one broad transaction. Physical
retirement/reclamation must be bounded and retain unknown reachability and
in-flight readers; an opaque metadata handoff alone does not permit deletion.

A concrete code-inspection counterexample for receiver design: start with a
10-byte base, checkpoint an append with data through byte 100, then truncate to
50 and grow to 100 after that checkpoint. Current `base_visible_len` stays 10
for both states. Switching to the 100-byte canonical parent and simply ignoring
its old patches can resurrect bytes 50..100. Durable truncation-after-checkpoint
watermarks or an equivalent immutable map difference are needed, not just the
original low-water mark. This is an implementation constraint identified from
the current code, not a new passing workload or a frozen private-v3 format.

The full task still requires actual same-journal handoff/rollover and receipt-owned
retirement, crash recovery of each cross-database boundary, fixed local work after
large/overlapping saves, namespace/create/delete/rename recovery, four-worker/
64-job scheduling/batching, off-lock slow metadata preparation, slow-fsync journal
coordination, retention/status and all WinFsp/FUSE/two-peer/WSL/native/W5/W6 build/
load/amplification/soak gates. WP6.3 budgets/late effects and WP6.5/6 cache/change
journal/compatibility/migration/activation/retention/handoff remain open. The
complete Wave 6 objective stays active; no live store/mount was activated.


### WP6.4 immutable overlay index foundation (CCR-WP6.4-6)

The pending journal receiver now has `content::overlay_map`: a separate immutable
SQLite sparse index with authenticated patch and truncation-sequence roots.
The active private v2 journal, metadata continuation implementation, runtime and
peer/canonical formats are unchanged. This is a prerequisite for bounded adoption,
not same-journal rollover or a mounted save implementation. No live data was
migrated and activation remains off.

Sixteen-way radix nodes are bounded to 1 KiB, scoped to operation UUID/kind/level/
prefix and hashed with a private versioned domain. Branch summaries include leaf
count, maximum mutation sequence and minimum cut length; demand reads verify
hash/body/shape/stats and never treat missing or malformed storage as a hole.
Oversized and wrongly typed bodies are rejected before BLOB materialization.
Patch point paths have eight branch levels; cut sequences use all 64 bits.
An independent Python binary/root vector and actual FULL/WAL reopen retain the
format contract. Returned roots must be sealed/persisted by the caller's intent
transaction and pinned for lifetime, independently of byte availability.

Sequence floors hide published entries without copying old leaves. Physical
`entries()` includes hidden leaves. Relative truncation minima over
`(published_sequence, target_sequence]` retain shrink/grow zero semantics; the
test byte oracle distinguishes an earlier Saved snapshot from later Working
cuts. Subtree truncation and sorted streaming bulk construction avoid preserving
point-update history for every seed entry. No collection is performed.

Walk/prune cursors bound memory by depth/fanout and poison partial failed work.
Pruning stages immutable parents in pages and yields a replacement root only on
completion; budget-one tests commit each staging transaction and retain the old
root. Restart after page rollback/commit failure, failed install or cancellation.
The embedding collector still needs its own root-set/owner fence and reader pins.
Each page permits 1..256 node read/install units; each install has an additional
bounded verification SELECT, separately counted by `node_checks`. The unit
budget does not describe all SQL, payload, WAL or device operations.

An actual BEFORE INSERT RAISE(IGNORE) reproduced a false successful nonexistent
root on source `c567bf9015fca9b67627bada645647fa43087d56b892e47c8412ed21887fbe6b`.
All 371 frozen inputs were checked unchanged before fixing it. Each install now
compares exact persisted bytes before success. Final cases cover ignored, deleted
and altered writes, plus ignored insertion with an exact preexisting node. Actual
SQL failures retain their cause. Caller-owned transactions are mandatory; the
new missing-transaction code is Fatal within that save scope. FULL persistence,
SQLite progress/busy policy and atomic intent publication belong to the caller.

Final Windows release qualification passed **237 cases** in one invocation:
lib 108, content_saves 40, content_write 33, contracts 41 and overlay_map 15.
Four ignored cases are two library opt-ins and two child helpers exercised by
their parents; child exits and draft repetitions add no reported successes.
Strict Clippy across all library/test targets, format and diff checks passed.
The map cases include a 320-step BTreeMap mutation/truncation oracle, immutable
forks, scope/version/corruption fences, mixed-generation pruning of both kinds
against an independent sequence filter, cancellation poisoning and u64-tail cuts.

The first expanded attempt failed only because the existing literal classifier
read binary magic `TKOM` as an error code. Its frozen source, four invoked binary
hashes, 221 successes/one failure and raw output are retained independently.
The exact magic was added to the existing non-code list beside `TKFC`/`TKFM`;
no error class or gate was relaxed. Final results above replace that attempt.

At both 5,000 and 50,000 seeded entries, a point read uses nine traversal reads;
an update uses nine reads/nine installs/nine readbacks and 7,491 encoded bytes.
Pruning old generations while retaining one newer patch uses nine reads/eight
installs/eight readbacks and 7,416 bytes in five budget-four pages. Bulk seeds
retain 5,340/53,339 nodes respectively. Each fixture's FULL/WAL seed commits
before timing. One warm-up and five measurements per size time only component
work, with transaction begin/commit/fsync and payload work excluded. Median
read/update/prune milliseconds are 0.0254/0.0644/0.0614
at 5,000 and 0.0258/0.1737/0.1779 at 50,000. Raw samples/ranges
remain in evidence. Fixed logical node counts do not establish constant SQLite
B-tree/cache/WAL/device cost or latency, or pass a native/W5/mounted build,
full-save, scale/amplification or physical-I/O gate.

`test-evidence/wave-6/overlay-map-checks.json` retains commands, outputs, samples,
before-fix failures, source manifests and final executable hashes. All 371
Cargo/build/src/tests/native/ui inputs were frozen before final lint/tests and
rechecked afterward. Added: map module/tests. Changed: content exports, error
registry and classifier's exact binary-magic exception. Final source SHA-256:
`c0c64cac62f4205dd3fb41a966bfce58bbe559a8ce4105a95101b82cf63ae87c`.

Next work remains the actual private-journal receiver: sealed durable map roots,
sequence/floor/cut state, stable operation UUID with a new owner/base pin, exact
checkpoint history and retry markers, staging/revalidation of later Saved and
Working work and cross-database recovery. The old v2 maps still scan all rows;
this new module does not remove that cost until integration. Receipt-owned
retirement, root registry/reader retention, bounded physical reclamation, slow
fsync coordination, namespace intents, save executor/batching and runtime/status
integration remain pending. The full Wave 6 goal, including WP6.3 remaining
budgets/late effects, WP6.5 cache/change journal, WP6.6 compatibility/migration/
activation/retention/handoff and every platform/build/load/soak gate, stays active.


### WP6.4 actual immutable-root journal integration (CCR-WP6.4-7)

`WriteJournal` now uses the immutable sparse maps, replacing the mutable
`changes`/`saved_chunks` indexes and complete Saved-map verification scans.
Working has cumulative patch/cut roots and a separate pending-patch root; Saved
has independent immutable roots in its checksummed bounded checkpoint. Root
bodies are verified before summaries are used, descendants on demand. Canonical
preparation uses authenticated range-existence queries to skip untouched spans.
The map's new range query is checked against an independent sequence-filtered
BTreeMap at boundary and full-u32 key ranges, including missing-root failures.

Ordinary writes path-copy their Working/pending paths and verify payload storage.
Shrinking drops complete subtrees, clips the last partial patch and records a
sequence/length cut. A clipped Saved boundary consumes pending quota. Growing
cannot restore old masked bytes or increase base visibility. Saved snapshots
retain their old bytes through newer Working writes/truncations; resume assigns
a fresh owner from Saved even if the newer buffered header is damaged.

FULL/WAL checkpoint verifies only the bounded pending patches and payloads
against Working, then commits Saved root references and clears pending state.
Global dirty-group/pending-byte accounting uses an indexed dirty-session query;
64 groups/64 MiB and 1 MiB producer limits remain. NORMAL ordinary buffering is
unchanged. The private connection mutex still spans FULL checkpoint work;
independent reads under actual slow fsync and the executor/batching are pending.

Private `user_version` is 3 at the unchanged journal path/application ID.
Experimental v1/v2 and future/foreign schemas are refused before mutation;
tests retain their raw records. Mandatory private root serialization changes the
checkpoint codec. No live migration, old-executable downgrade/fencing rehearsal
or production activation occurred. Historical experimental checkpoint metadata
also needs an explicit compatibility policy before activation. Canonical bytes,
peer wires, metadata event identities and SaveIntent capture codec stay unchanged.

Complete Working/Saved state, flags/count, seal, inventory and payloads are read
back before acknowledgement. Actual ignored header/payload writes and altered
Saved/inventory writes roll back without false success; the prior checkpoint
and newer healthy Working bytes remain readable. Missing/malformed required SQL
record values become scoped corruption, retaining actual SQL I/O/authorization/
interruption causes. Existing deadline/cancel/progress-hook/owner/crash tests pass.

Payload collection audits inventory and all live Working/Saved/pending/cut roots
in pages, read-back marking payloads in SQLite with a fresh pass UUID. Deletion
begins only after the entire audit verifies. Own/foreign commits or another
journal instance restart it. Missing roots/intents and ignored marks cannot
authorize deletion. Combined node/scope validation plus payload examinations
stay <=256 per page; additional mark/readback SQL is not hidden in that unit.
Maintenance failures remain independently visible and do not fail unrelated
already committed healthy writes. One mark row per hash is overwritten by later
passes and removed with collected payloads. Historical immutable nodes are still
retained, and continuous root changes can prevent a long audit finishing; bounded
page work does not qualify history storage, reclamation progress or long soak.

Final Windows release verification passed **243 cases** in one five-target
invocation: lib 109, content_saves 40, content_write 37, contracts 41, overlay_map
16. Four ignored cases are two library opt-ins and two child helpers invoked by
their parents; helpers and development repeats add no successes. Four new write
cases cover header/inventory false acknowledgements, ignored payload, ignored GC
marks and explicit v2 refusal. Existing byte/oracle, sparse/grow/shrink, independent
Saved recovery, native preparation, bounded collection, foreign-commit/instance,
deadline/cancel, child process exit and metadata receipt/history suites reran.
Strict Clippy across all library/test targets, format and diff checks passed.

A new library probe measures the actual journal after committed 5,000/50,000-leaf
Saved roots. Each size bulk-seeds one authenticated sparse root and one shared
73,728-byte masked payload before timing; this excludes initial edit-history
cost and is not a dense full-file workload. One warm-up and five measurements
read one byte, change it and FULL-checkpoint, including transaction/fsync/payload
verification and one bounded maintenance page. SQLite total_changes deltas are
256 rows at both sizes, below the 400-row bound. That counter excludes reads and
is not physical write-byte amplification. Median read milliseconds:
0.2309 / 0.1787; write+FULL-checkpoint medians:
3.4512 / 4.4790.
Measured save ranges are 3.1276–9.7323
and 4.0927–20.5917 ms.
All samples and standalone index samples remain in evidence. These are component
results, not mounted/native/W5/W6 build, complete save-pipeline, scale, physical
device/WAL amplification or slow-fsync coordination release-gate evidence.

`test-evidence/wave-6/root-journal-checks.json` retains commands, outputs, source/
binary hashes and samples. All 371 Cargo/build/src/tests/native/ui inputs were
frozen before final strict lint/qualification and rechecked afterward. Changed
since the previous stage: journal, map, journal/map tests and the classifier's
exact measurement-label exception. No input was added/removed. Source SHA-256:
`a6206b64d4b373e9096c68d7f3fcca8f3ef6c3e64a2655e91469b304548c589a`.

Next remains actual same-journal SaveContinuation adoption: retain exact prior
checkpoint evidence, stage later Saved/Working/cut roots in bounded pages,
revalidate concurrent work, atomically replace owner/intent/base pin, persist
lost-reply markers and recover each cross-database boundary. The root sequence
floor is not advanced yet. Receipt-owned retirement, root registry/read lifetimes,
node reclamation and collection progress remain required. Namespace intents,
runtime adapters/status, save workers/batching and every original platform/
build/load/soak gate stay open, along with remaining WP6.3/6.5/6.6 work. The full
Wave 6 goal remains active and activation stays off.


### WP6.4 same-journal continuation adoption (CCR-WP6.4-8)

The private receiver now implements `WriteSession::adopt_continuation` on the
same operation UUID/journal, with an actual new owner and native base pin. It
checks the opaque metadata handoff, canonical store/provider identity and exact
private checkpoint history. Working/Saved roots are staged in <=64 node-unit
pages with the mutex released between commits. Their relative cut minima and
publication floor preserve independent later Saved and buffered Working views.
The final FULL/WAL transaction revalidates the complete captured state, stores
a retained predecessor and atomically switches owner/intent/roots. A concurrent
mutation/checkpoint/resume returns stale; no staged result replaces later work.

The new session preserves pending ordinary bytes/quota and invalidates old
handles/preparations. A marker binds old owner, exact predecessor and archive;
a committed lost-response retry verifies them and returns the same current new
owner without another transition/archive, preserving that owner's later edits.
Resume clears the marker and assigns a fresh owner, so old retries cannot steal
it. Later continuations likewise fence older handoffs. A clean rebased Saved
baseline supports resume but is excluded from pending/public checkpoint output
and rejected by metadata selection, avoiding a synthetic no-op event. Metadata
live generation/view/workspace checks still govern actual publication.

`checkpoint_records` contains indexed checksummed identities, not delete/read
authority for superseded selections. `retained_overlays` stores independently
bounded Working/Saved records, scoped/sealed by archive ID and operation. Its
seal joins inventory; collector audits current sessions and archives before any
private payload deletion. Missing/altered/whole-row-lost archives or discarded
writes fail closed. Actual canonical-byte deletion does not authorize reclaiming
the predecessor payload. No receipt-owned deletion is performed. Native old-base
CAS root registration, in-flight lifetimes and automatic recovery using retained
predecessors still need runtime integration; private payload retention alone is
not a complete native base lifetime/availability or recovery guarantee.

Transaction cleanup also closes a false-replay hazard: an actual denied rollback
could otherwise leave tentative marker/header changes visible to the next caller.
The guard clears the request hook, marks a non-cache failed-connection flag,
retries rollback and attempts replacement/physical close. Future requests fail
`WRITE_JOURNAL_CONNECTION_FAILED` even after successful replacement; discard and
reopen before recovery. A library case tests actual authorizer-denied rollback
for returned failure and unwind after the tentative owner write, verifies no
replay acknowledgement and reopens the original Saved checkpoint. OS close,
allocation failure, ENOSPC and power loss remain separate platform gates.

Private `user_version` is 4 for the publication flag/index, history, archives and
marker. V1/V2/V3, future and foreign schemas are refused without rewriting their
recovery records. V3 root-era schema/raw-byte refusal is covered explicitly;
this is not migration or an old-executable upgrade/downgrade rehearsal. Canonical
content/wires, SaveIntent capture codec and event identity algorithms stay fixed.

Final Windows release qualification passed **255 cases** in one five-target
invocation: lib 111, content_saves 49, content_write 38, contracts 41, overlay_map
16. Four ignored cases are two library opt-ins and two child helpers invoked by
their parents; repeated drafts/helpers add no successes. Nine added save parents
cover a real same-journal event chain/no-op baseline, independent Saved/Working
shrink/grow, six returned-failure/lost-response boundaries, concurrent staging,
canonical loss/archive retention, missing history/discarded retention, wrong
store/provider, six real child exit-86 adoption boundaries, and cancellation/SQL
deadline. Existing metadata-only fixture generations remain labeled as such.
Strict all-library/test Clippy, format and diff checks passed on the frozen source.

A new library probe uses genuine metadata handoffs after 5,000/50,000 published
patches with both a later Saved edit and another buffered edit. One warm-up and
five measurements use separate real disk/FULL-WAL fixtures. Setup bulk-seeds a
complete patch root and an independently built equivalent zero DAG; its initial
PreparedSave is a trusted private fixture over that root/pin, not the actual
full 50,000-patch preparation pipeline. Initial edits, preparation and metadata
handoff are outside adoption timing. Later Saved native preparation and byte
checks run afterward. Timed receiver work includes preflight, staging, header/
archive/history verification and final FULL fsync. It uses <=18 staging reads,
<=16 installs/readbacks and <=32 changed SQLite rows at both sizes, leaving one
Saved patch; old recovery roots remain retained. Staging counters exclude
preflight/header queries and total_changes is not read/statement/physical byte
accounting. Median adoption milliseconds: 5.0086 / 1.5934;
ranges 1.1904–6.4795 / 1.1967–1.8736.
Raw samples remain in evidence. This does not pass full-save, mounted/native/W5/
W6 build, physical/WAL/device amplification, long storage or soak gates.

The first unbound compile caught generic ContentProvider lacking an identity
method; identity is checked using its returned pin outside the journal mutex.
An unfrozen development test tool session ended with code 1 and only Compiling
logged, without Rust diagnostics or test results; cargo/rustc were absent when
checked afterward. Cause is unproven. Final compilation used one Cargo job;
no successes or source/binary qualification are attributed to that aborted run.

`test-evidence/wave-6/adoption-checks.json` retains commands, raw outputs,
development attempts, current source/executable identities and component samples.
All 371 Cargo/build/src/tests/native/ui inputs were frozen before strict lint/
qualification and checked unchanged afterward. Only journal, metadata selection,
error registry and save/write/classifier tests changed; no input added/removed.
Source SHA-256:
`5ddb4ed0b71bc92beb1477956cb0a79fe485036bdc883174a4698b09c78fcce5`.

The full Wave 6 objective remains active. Receipt-owned logical/physical
retirement, checkpoint/node/archive reclamation and collection progress are not
implemented; historical storage grows conservatively. Native root registration/
read lifetimes and automatic retained recovery, namespace intents, four-worker/
64-job scheduling/batching, actual slow-fsync independent access, checkout/
shutdown, runtime adapters/status and unused captures still need work. All
original platform/build/load/outage/soak/physical amplification and remaining
WP6.3/6.5/6.6 compatibility/migration/activation/retention/handoff gates remain
required. Activation stays off and no failed gate is waived.

The six new hard-exit cases use the published baseline. Later Saved/Working
truncate/grow views pass live adoption and postcommit reopen, but the next crash
qualification must repeat every private boundary with that overlapping state;
baseline exits are not evidence for the full overlapping-save crash matrix.


### WP6.4 overlapping-save process-exit matrix (CCR-WP6.4-8 qualification)

The six private adoption boundaries now run against both the published baseline
and distinct later Saved/Working states: twelve real child process exits with
code 86. Production code and formats are unchanged; only publication test/helper
source changed. The exact older parent is passed separately to the child so
recovery cannot accidentally continue the newest unpublished checkpoint.

The overlap fixture publishes 100 bytes at sequence 2, shrinks to 50 and grows
to 100, then FULL-checkpoints Saved at sequence 4. The child resumes under a new
owner and writes ordinary buffered bytes at 75, producing Working/leaf sequence
6 with valid length 89. Each precommit exit retains the original intent/floor
and no archive; the committed exit has next intent/publication floor and exactly
one retained archive. Parent inspection before resume verifies actual persisted
Working sequence, base visibility, authenticated leaf/payload SHA-256, buffered
bytes and masks for the zero gap. Product collection audits typed scope/header
seals, inventory and all current/retained roots before recovery changes state.

Fresh resume intentionally selects promised Saved and discards ordinary Working;
observing buffered bytes in SQLite is not a new checkpoint promise. Precommit
cases retry adoption; committed cases resume the rebased Saved and reject the
old handoff. Every overlap case recovers Saved sequence 4 with visibility 50,
reproduces the 100-byte precrash Saved view, builds native content equal to byte
import and publishes/replays exactly one next event/admission (two admissions
including the older parent). Exact old receipt and derived-state oracle remain
valid, and archive count stays one. Baseline cases remain clean without a no-op
publication. These are actual process exits/reopens, not power loss or ENOSPC.

Final same-source Windows release qualification passed **255 reported cases**:
lib 111, content_saves 49, content_write 38, contracts 41 and overlay_map 16.
The enlarged crash matrix remains one parent case; neither its twelve helper
invocations nor six added overlap executions inflate the reported total. Two
library opt-ins and two parent-invoked helpers remain ignored. Strict Clippy
across all library/test targets, format and diff checks passed. All five suites
reran; earlier totals are not added or retroactively relabeled.

`test-evidence/wave-6/overlap-crash-checks.json` preserves commands, twelve matrix
records, raw outputs and executable/source identities. All 371 Cargo/build/src/
tests/native/ui inputs were frozen before lint/qualification and rechecked
unchanged. The only source delta is `tests/content_saves.rs`; no input was added
or removed. Source SHA-256:
`b880ddbeacc978c4341b9f5415e9f81f974bcdd6985293998585a2ed232580b0`.

This closes the previously documented baseline-only hard-exit evidence gap for
these six adoption boundaries. Native base/root registration and in-flight
lifetimes, automatic retained recovery, receipt-owned retirement, bounded
checkpoint/node/archive reclamation and collection progress still need work.
Namespace intents, executor/batching, actual slow-fsync independent access,
checkout/shutdown/status and runtime adapters remain incomplete. The complete
Wave 6 objective, all remaining WP6.3/6.5/6.6 work and original platform/native/
W5/W6 build/load/outage/soak/physical amplification/compatibility gates stay
active. No live store was activated and no failed gate was waived.


### WP6.4 private root source and immutable retained reads (CCR-WP6.4-9)

The journal now enumerates recovery base FileRevisions from current sessions and
retained predecessors in pages of 1..64 records, validating typed headers/scope
seals and inventory count/XOR. Unsaved sessions are included by the source query.
Cursors bind instance/SQLite own and foreign change tokens; a completion fence is
available only after the whole source audit and can be revalidated. Partial or
stale results are not complete reachability. No complete-root vector/permanent
pin is eagerly loaded at startup. Any journal write/maintenance can conservatively
invalidate a pass; continuous-write progress is not qualified.

Opaque source roots can acquire a same-store native base pin outside the journal
mutex and revalidate afterward. These pins protect lifetime, not availability.
The source is private recovery data only: metadata/history/conflict/snapshot and
explicit/in-flight roots plus a native deletion barrier still need reconciliation
before collection. No native deletion or whole-store reachability authority is
returned, and no automatic runtime registration is implied.

`open_retained_checkpoint` exposes immutable archived Saved bytes independently
of the active owner. It checks archive/seal/root identity, reserves one of at most
64 read tickets per journal under its mutex, pins the provider base outside the
mutex and rechecks. Failed pin/acquisition and reader drop return the ticket.
The reader owns journal/provider/base lifetimes after old sessions close and the
journal reopens. Future retirement must inspect these tickets atomically under
the journal mutex; no archive retirement is introduced here. Shared range code
applies the same valid-mask/base-visibility rules to live and retained readers.
Base byte I/O is outside the private mutex, with archive/context checks at each
chunk and after completion; error output must be discarded.

`restore_native` explicitly reconstructs and pins retained Saved content using
native COW/zero spans and normal verified installation. It yields content, not
PreparedSave or metadata ownership/publication authority. A needed legacy base
converts once per reader/target before builder buffer acquisition; full masking
avoids hydration of an absent legacy base. Cached conversion works after removal
of the legacy source file. Necessary unavailable old-base bytes/structure still
fail; automatic live fallback and native reconciliation remain future work.

Final Windows release qualification passed **277 reported cases** in seven
selected targets: lib 112, content_provider 12, content_retention 9, content_saves
49, content_write 38, contracts 41, overlay_map 16. Strict Clippy across all
library/test targets, format and diff checks passed. Five ignored cases are two
library opt-ins, the provider 1GiB opt-in and two parent-invoked process helpers;
helper/scenario/draft invocations are not additional successes. Earlier source
and unselected platform/performance results are not attributed to this source.

Nine new integration parents use actual two-generation metadata publications/
adoptions and disk-backed SQLite/native CAS. They verify bounded roots and native
pins/reopen; cursor/fence staleness and whole-scope inventory loss; cross-instance
limits/cancellation; base pin and Saved bytes after all live sessions close;
actual canonical-manifest deletion followed by reconstruction without a metadata
event; slow base read alongside current write/FULL checkpoint; cancelled base I/O
releasing pin/ticket; reader quota/failed pin cleanup; and archive corruption.
The slow-I/O case bounds the independent current operation with a 300ms deadline;
it does not establish independent reads under slow private FULL fsync. A new
library parent uses trusted private archive fixtures for legacy cached conversion
and full replacement without the missing base, compared to native byte import.
Existing provider, live journal, byte/oracle, publication/crash and map suites
reran after extracting shared overlay application. No schema/canonical/wire/
event/capture encoding changes; private user_version remains 4.

`test-evidence/wave-6/retained-reader-checks.json` retains exact commands, outputs,
source/executable hashes and development diagnostics. All 373 Cargo/build/src/
tests/native/ui inputs were frozen before final lint/qualification and checked
unchanged afterward. Added: private retention module and integration target;
changed: journal exports/reader registry/shared range helper. No input removed.
Source SHA-256:
`8db8182fa56f84c58e1241ae0d364ed7928949f4f9f07b3acf7e68cc933038b0`.

The full Wave 6 goal remains active. Runtime native-root registration across all
sources, deletion/in-flight barriers, automatic retained fallback, receipt-owned
logical/physical retirement, node/checkpoint/archive reclamation and collection
progress/storage bounds are still incomplete. Namespace intents, save executor/
batching, slow-fsync independent journal access, checkout/shutdown/status and
mount adapters still need work. WP6.3 residual budgets/late effects, WP6.5 caches/
change journal, WP6.6 compatibility/isolated-store/old-executable migration/
downgrade/activation/retention/handoff, and every original platform/build/load/
outage/soak/physical amplification gate remain required. Activation stays off.

### WP6.4 bounded orphan private node collection (CCR-WP6.4-10)

Private maintenance now reclaims superseded immutable map nodes in addition to
unreferenced payloads. Bounded walks report verified node hashes; the collector
marks all current Working/pending/Saved and retained predecessor patch/cut roots
at floor zero, verifies mark readbacks and completes the existing sealed scope
inventory count/XOR audit before sweeping. An epoch only authorizes deletion
after that complete audit. Superseded checkpoint identity records remain; they
do not grant arbitrary historical private byte reads. Archived roots remain
protected even when canonical bytes are unavailable.

Audit, payload and node candidate work share 256 units per page. The existing
removed/examined fields remain payload counts; new node counters are separate.
Mark writes/readbacks are extra SQL operations inside a traversal unit, not a
256-statement or physical-I/O bound. Existing instance/SQLite own/foreign change
tokens restart stale passes. Each adoption reserves a bounded journal-local RAII
staging lease under the journal mutex before any NORMAL staging commit. While
one exists, node sweep defers with a continuation, protecting not-yet-installed
roots through final FULL commit, failure or unwind. Up to four stages are
admitted; the runtime four-worker/64-job save executor is still unimplemented.

Node marks use connection-local FILE-backed TEMP storage keyed by node hash.
Reopen loses marks/cursors and re-audits; committed roots protect their nodes,
while abandoned staging nodes are orphans. Main/temp SQLite caches are configured
1536/512 KiB, preserving the former aggregate 2048 KiB configuration. This does
not establish an RSS or disk bound. No persistent schema/canonical/wire/event/
capture codec changes; private user_version stays 4. SQL row deletion does not
promise file shrinking, freelist reuse or sustained storage progress.

Final Windows release qualification passed **281 reported cases** in seven
selected targets: lib 113, content_provider 12, content_retention 9, content_saves
50, content_write 39, contracts 41, overlay_map 17. Strict Clippy across all
library/test targets, format and diff checks passed. Five ignored cases remain:
two library opt-ins, the provider 1GiB opt-in and two parent-invoked process
helpers. Child scenarios, draft runs and prior-source totals are not extra
successes.

Four new parents cover exact verified walk-node enumeration with small pages;
200 hot-chunk rewrites ending with exactly nine live nodes and correct Saved
bytes after reopen; real metadata adoption paused after staging commit while
collection defers, followed by valid Working/Saved bytes after commit/collection;
and dropped TEMP marks refusing deletion before a successful retry removes a
malformed unreferenced node, also checking cache configuration. Existing retained
reader reopen coverage now completes GC with its ticket/base pin held, then
reads archived bytes after live handles/store close. Implicit maintenance already
removed obsolete hot-chunk nodes, so its explicit final pass removes zero and
examines the nine live nodes; this is not a physical storage/amplification claim.

The 5K/50K root-journal probe allows at most 600 changed SQL rows rather than 400
because each audited node now adds a TEMP mark DML. This source measured 512 rows
per point write/FULL checkpoint at both scales, including its bounded maintenance
page. Counts include TEMP writes and exclude readbacks/physical I/O. Component
timings remain samples, not mounted build/latency gates or a waiver of any gate.

`test-evidence/wave-6/node-gc-checks.json` retains exact commands, outputs,
source/executable hashes and intermediate diagnostics. All 373 Cargo/build/src/
tests/native/ui inputs were frozen before final lint/qualification and checked
unchanged afterward. Six existing inputs changed: overlay map, write journal,
retention/save/write/map tests. No input added or removed. Source SHA-256:
`2824e3f495542b856d7b476ebdc8a5180fd85727e7e5f11de1970d4f4290cbba`.

The full Wave 6 goal remains active. Receipt-owned archive/checkpoint retirement,
native root registration across all sources/deletion barriers and actual native
GC/reader races, automatic live fallback, continuous-mutation collection progress
and long-run DB/temp/WAL/device storage bounds remain open. Namespace intents,
runtime/mount integration, executor/batching, slow-FULL-fsync independent journal
access, checkout/shutdown/status, residual WP6.3/6.5/6.6 work and every original
platform/build/load/outage/soak/compatibility gate remain required. Activation
stays off; no live workspace was migrated.

### WP6.4 runtime content-save coordinator (CCR-WP6.4-11)

Added a runtime component connecting private checkpoints, canonical preparation,
metadata selection/publication and view refresh, exact continuation and private
adoption. A stable SaveFile replaces its session only after successful adoption;
writes/truncation/checkpoints/adoption serialize per file, while canonical
preparation releases both file and metadata locks. New canonical content stays
pinned across publication until its continuation base pin is acquired/adopted.
Range reads capture the session without retaining the file mutex through base
I/O; stale output still must be discarded.

The executor has four fixed workers and at most 64 outstanding operations,
including active jobs and producers checkpointing before queue insertion.
Operation-key exclusion and UUID-owned RAII permits prevent duplicate jobs and
stale releases; saturation has no synchronous fallback. One same-store journal
Arc is required for all files; independent same-path or foreign journals cannot
multiply the private dirty-byte budget. A weak per-engine registration survives
cancelled owners while remaining workers/reservations finish, preventing a
replacement pool on that engine from multiplying blocked threads.

Enqueue preserves a newer Saved while an older committed parent needs adoption.
A bounded exact operation/generation metadata history lookup validates the
capture/intent/receipt and finds that parent after lost reply/reopen. It grants
no publication/deletion authority. Unpublished stale work can retry the latest
Saved. An adoption that committed before its reply can replay its old owner
marker. Explicit flush repeats through the carried work until clean or deadline;
it never waits for a peer. Enqueue alone is not a mounted close/fsync boundary:
lost-reply recovery may first adopt committed state whose carried Working still
needs checkpoint/publication. Adapter integration must enforce the full fence.

Operation cancellation links to pool stop; cancelling only a ticket wait leaves
the accepted operation running. Engine acquisition polls against context.
Errors/caught job panics return permits and retain recovery data; the pool does
not collect a completed-job history. Shutdown closes admission and caps its drain
wait at the earlier caller deadline or five seconds. Uninterruptible OS/provider
I/O and blocking cancellation hooks are not killed; timed-out workers stay
registered. Global lifecycle deadlines are not qualified by this local wait API.

The component is explicit and headless: no startup, RPC, WinFsp or FUSE callback
constructs it. Mounted engines and engines with legacy sessions/handles are
refused. This intermediate fence must be replaced by actual provider-backed
adapter integration; it does not close that requirement. Existing typed-content
fixture gating and legacy adapter behavior remain. No persistent schema,
canonical, wire, event or capture encoding change; private user_version stays 4.

Final Windows release qualification passed **301 reported cases** in nine
selected targets: lib 113, content_executor 11, content_provider 12,
content_retention 9, content_saves 50, content_write 39, contracts 41,
overlay_map 17 and pending_saves 9. Strict Clippy across all library/test targets,
format and diff checks passed. Six ignored cases are two library opt-ins, the
provider 1GiB opt-in and three parent-invoked child helpers. The preliminary
eleven-case executor run, child scenarios and earlier-source totals are not
additional reported passes.

Eleven new parents use the actual journal/metadata/native CAS: two-generation
flush with writes during preparation; lost replies at four publication/adoption
points; unpublished superseded retry; reopen with a committed parent and newer
Saved; caught preparation panic/worker reuse; four active plus 60 queued jobs,
saturation rejection, independent metadata, short shutdown timeout then
cancelled-job recovery retention; independent cancelled ticket wait; engine
acquisition timeout/permit cleanup; foreign/independent journal refusal; mounted
callback guard; and four real child process exits with fresh recovery, byte
checks, one publication and the derivation oracle. The saturation case leaves
64 Saved checkpoints, not canonical publications. Its 4000ms blocking fault
tests a bounded timeout and eventual drain, not arbitrary OS/provider shutdown.

`test-evidence/wave-6/executor-checks.json` retains exact commands, outputs,
source/executable hashes and development diagnostics. All 375 Cargo/build/src/
tests/native/ui inputs were frozen before the final strict lint/nine-target
qualification and checked unchanged afterward. Added runtime module/integration
target; changed journal accessors, metadata exact-generation history lookup,
Engine registration and runtime module registry. No input removed. Source SHA-256:
`9458e85f6d4ac7eb87be1e37be6c36606a4f1ef9e2d3765c48b029937bb0d783`.

Three known live draft compilations were deliberately stopped for reviewed pin
lifetime, journal-budget/API and test-fixture corrections. They were not
observation timeouts, test failures or passing qualification. Their raw logs
remain separate; final evidence uses only the later frozen source/run.

The full Wave 6 goal remains active. Namespace/create/delete/rename/atomic
replacement/open-handle recovery, startup reconciliation/autonomous retries,
Engine quiet/checkout/shutdown/status wiring, mounted reads/callback durability,
compatible checkpoint/metadata batching and slow-fsync independent private reads
remain required. Native root registration/deletion barriers/actual GC races,
receipt-owned retirement and sustained storage progress remain open, along with
all remaining WP6.3/6.5/6.6 work and the complete original platform, build/load,
outage, soak, physical amplification and compatibility gates. Activation stays
off and no live workspace was migrated.

### WP6.4 compatible private checkpoint groups (CCR-WP6.4-12)

The private journal now checkpoints up to 64 distinct same-instance owners in
one FULL transaction. It validates all requested owners/headers/roots before
advancing only their dirty sessions, preserving result order and the existing
pending-payload verification. Unrequested Working stays buffered. A failure
acknowledges none; a lost reply can leave the whole Saved group committed.
Single-file and quota checkpoint paths share this implementation.

The runtime exposes enqueue_batch and a bounded flush_files slice. It reserves
the whole group against the existing 64-outstanding budget before allocating
sorted file/guard/job vectors, locks files in UUID order, checkpoints once,
resolves committed older parents and queues independent publication/adoption on
the same four workers. Returned UUID/ticket pairs use that order. Duplicate,
oversized, foreign or earlier-pending groups cannot partially queue. Error after
checkpointing retains Saved for retry. Flush slices drain earlier pipelines and
repeat compatible groups through commit/adoption until clean or deadline. No
writer thread, inline fallback or enlarged budget was added.

Process-local checkpoint_work counters record successful logical FULL checkpoint
COMMIT calls and dirty sessions advanced, including empty/read-only transactions
if a COMMIT is issued. Empty input opens none. These counters reset on reopen and
exclude ordinary writes, adoption, metadata and canonical installation. They
are not physical fsync/WAL/device measurements or durable authority. A group of
64 private checkpoints is not one fsync for 64 complete saves; metadata remains
per revision. Headless/unintegrated callback and isolated-fixture gates remain.

Final Windows release qualification passed **307 reported cases** in nine
selected targets: lib 113, content_executor 14, content_provider 12,
content_retention 9, content_saves 50, content_write 42, contracts 41,
overlay_map 17 and pending_saves 9. Strict Clippy across library/all test targets,
format and diff checks passed. Seven ignored cases remain: two library opt-ins,
the provider 1GiB opt-in and four parent-invoked child helpers. The preliminary
executor14/write42 run, child scenarios and prior-source totals are not extra
reported successes.

Six new parents cover 64 real runtime files sharing one private checkpoint before
64 independent publications, byte checks and the derivation oracle; pre-commit
failure retaining Working without partial Saved/queue/slot updates and successful
retry; duplicate/oversized/foreign/pending groups refusing partial insertion;
both prior Saved surviving a failed group while newer Working stays visible,
followed by ordered retry of only requested owners; stale/foreign/duplicate
private owner refusal; and two real process exits before/after FULL commit with
fresh reopens of four files showing all-old or all-new Saved. The new ignored
helper runs only through that parent. This is process-exit recovery, not power
loss, device fsync counting or a mounted/build/performance gate.

`test-evidence/wave-6/checkpoint-batch-checks.json` retains commands, outputs,
source/executable hashes and development diagnostics. All 375 Cargo/build/src/
tests/native/ui inputs were frozen before final strict lint/nine-target release
qualification and checked unchanged afterward. Four existing inputs changed:
private journal, runtime coordinator and their write/executor tests; none added
or removed. Source SHA-256:
`14e70bde45deaaccd1016591608e17a2f94e072a958f8d85d55155384555b9e9`.

An initial new write-test compile lacked local fault imports; fixed before its
behavior run. The allocation-admission order was corrected while that draft was
compiling, so it is unbound intermediate evidence; final source was frozen and
all nine suites reran. No draft source/binary/pass attribution substitutes for
the final run.

The complete Wave 6 goal stays active. Automatic coalescing of independent close
producers, compatible metadata batching, slow-fsync independent private readers,
complete mounted close/fsync recovery fences, namespace/open-handle recovery,
startup retry/enumeration and Engine lifecycle/status integration remain open.
Native retention/deletion barriers/actual GC-reader races, receipt-owned record
retirement and sustained storage progress still need qualification. All other
WP6.3/6.5/6.6 work and original platform/build/load/outage/soak/physical storage/
compatibility gates remain required. No schema/canonical/wire/event changes;
private user_version remains 4, activation stays off, no live workspace migrated.

### WP6.4 descriptor-backed mounted local reads (CCR-WP6.4-13)

WinFsp and FUSE now select immutable typed descriptors for native reads. Entry
size remains the descriptor's validated length even when the manifest or an
unrelated chunk is absent. The adapters acquire a provider pin before
acknowledging open, do provider construction/pinning/range I/O outside Engine,
then revalidate the store, view, handle and selected descriptor. A bad provider
pin is refused, failed provisional opens are released, and concurrent losing
pins are dropped outside the metadata mutex. Lease release must be bounded and
perform no disk/network I/O. Providers own any asynchronous cleanup separately.

The session retains its pin until close or clean revision refresh. In-flight
reads retain their own immutable selection, so a concurrent revision change
cannot mix versions; old output fails STALE_VIEW and is discarded. A refresh to
the same descriptor reuses its binding. Clean non-writer sessions now refresh
the complete Entry metadata as well as their bytes, correcting stale handle
length/timestamps/revision. Dirty and writable bases retain their existing
rules; an unlinked open file retains its bytes.

Every provider read is at most 1 MiB. The public range API permits short reads
at that bound; WinFsp fills its borrowed destination through bounded calls to
the actual requested length/EOF. This avoids false EOF at the provider buffer
boundary. FUSE allocates only after typed selection within its existing 1 MiB
request/reply cap; open uses the existing bounded threaded dispatcher. Ready
verified ranges do not need other chunks, while missing/corrupt demanded data
fails rather than returning zeros or empty content. Native writable open,
snapshot, resize and direct staged read remain explicit guards pending COW
write integration. Existing WinFsp disabled caching and FUSE direct I/O remain.

Adapter contexts poll Engine acquisition with a deadline and link to the
registered object-worker stop token. Cancellation hooks now run outside the
inflight-objects mutex while preserving the post-cancellation state snapshot.
These changes do not forcibly interrupt arbitrary provider/OS blocking I/O or
qualify whole-runtime shutdown. Default LocalContent is constructed lazily
off-lock; a programmatic one-time provider seam permits independent providers
before any descriptor session. Neither path activates typed content in user
workspaces.

Final release qualification passed **359 reported cases on Windows** and
**366 on Linux/WSL**, in eleven selected targets on each: library 113/120,
content_mount_reads 10, content_transport 42, content_executor 14,
content_provider 12, content_retention 9, content_saves 50, content_write 42,
contracts 41, overlay_map 17 and pending_saves 9. Strict Clippy across the
library/all test targets, formatting and diff checks passed. Both platforms
used Rust/Clippy 1.96.0; WSL uses a named toolchain and keeps its default 1.88.
The per-platform executions are not 725 distinct cases. Eight ignored cases
remain in each main run: two library opt-ins, the provider 1GiB opt-in, four
parent-invoked crash helpers and the kernel mount opt-in.

The ten new ordinary parents exercise descriptor length/pins and ready ranges
with a truly absent unrelated chunk, missing manifests/corrupt demanded bytes,
empty/EOF and write guards, bounded provider calls with a larger WinFsp ABI
destination, slow pin/read with independent metadata, wrong pin refusal,
concurrent refresh and stale output, same-reference pin reuse/full metadata
refresh, legacy zero-wait and generation fences, Engine-lock deadlines and
mid-read cancellation/lifetime. Each platform also passed one explicit ignored
parent using a real kernel mount in a child process. These are two separate
platform results, with no extra child success counted. Both real mounts verify
a 257-byte ready range of a 196608-byte descriptor with its third chunk absent,
a complete 2097163-byte file, missing-demand and native-write refusal, then
refresh the same OS handle to 11 new bytes with updated size/timestamp. Pins
release after close, health stays clear and graceful detach succeeds. Only
temporary isolated fixture stores were mounted.

`test-evidence/wave-6/mounted-read-checks.json` retains commands, raw outputs,
source and all eleven executable hashes per platform, distinct supplemental
mount results and development diagnostics. All 377 Cargo/build/src/tests/native/
ui inputs were frozen before final lint/tests and checked unchanged afterward.
Two inputs were added (the runtime reader and integration target), eleven
existing inputs changed and none removed. Source SHA-256:
`c270ea38e1ee759e98913b4db3d4d2da435b9b6869bbf03667d1e2ec5df58892`.
A local ignored ZIP of the exact frozen inputs was read back and verified
against every member hash; its identity is recorded with the raw evidence.

Linux 1.96 strict lint exposed six existing Unix diagnostics: two nested
conditionals and four signal function-pointer casts. Corrected without warning
suppression. Cross-platform regression work also fixed a test observation race:
legacy bytes could be available before the fifth frame's completion counter
advanced. The test now observes that acknowledgement before cancellation under
the same one-second bound, preserving admission/availability timing. The legacy
retained-save writer test could finish its 32MiB save before collecting its
minimum 20 samples (17 free of 18). Its existing byte-store fault point now
delays 400ms, with an asserted hit and RAII reset; minimum samples and free ratio
remain unchanged, and writer-before-store would still fail. Preliminary failed
lint/test logs and one Windows link attempt against a still-running earlier
test binary remain separate. Final suites ran after source refreeze; no draft,
repeated run, helper invocation or earlier-source total is an extra success.

The full Wave 6 goal remains active. Foreground missing-range demand/lease
scheduling and availability-index repair are not yet connected to these
callbacks. Native COW mounted writes, durable namespace/open-handle recovery,
complete close/fsync/startup/quiet/checkout/shutdown/status integration,
automatic close-producer coalescing, metadata batching and slow-fsync independent
private readers remain required. Complete native root registration/deletion
barriers/actual GC races, automatic live retained fallback, receipt-owned
retirement and sustained storage progress remain open. All residual WP6.3,
WP6.5 and WP6.6 work and original platform/two-peer/ENOSPC/power-loss/OS-close/
allocation/build/load/outage/soak/physical amplification/compatibility gates
remain required. No schema/canonical/wire/event changes, private user_version 4,
no live activation or migration, and no gate waived.

### WP6.4 mounted missing-range demand and availability repair (CCR-WP6.4-14)

Native adapter reads now register missing ranges on the existing foreground
content scheduler. Ready local reads keep the direct provider path. The waiter
captures the exact session content event/entity reference, branch and checkout
generation; it revalidates them throughout Engine/SQL waits and at completion.
A newer event with identical bytes still stales an older waiter. Only the same
native provider owner and a currently granted shared reference can start network
demand. Private observations can clear stale local availability but never create
a remote waiter. Independent providers and typed legacy references retain their
own unavailable behavior; the ordinary legacy string hydration path is unchanged.

Demand uses the stable local workspace UUID already used by save intents. Its
existing config row is allocated lazily on first missing-range demand, off
Engine, and reused after reopen and by later saves. Read generation describes
the captured workspace view, with the immutable reference selecting the content
event; it confers no save/publication ownership. No schema/canonical/wire/event/
capture changes or new activation path were introduced.

A wait owns one <=1 MiB range lease until verification or failure. Actual
missing/corrupt object observations invalidate only that key's verified index
bit, preserving receipts and permissions. A new missing key or a contradicted
verified index rearms the lease, so an earlier satisfied index cannot hide
required repair. Joined installation ownership remains in the object engine;
independent callers retain independent deadlines/cancellation and pins.

Native waits charge a process-wide limit of 64 before opening secondary handles
or allocating demand state, plus the adapter's existing waiter slots. Ready
ranges need no slot. The new nowait pool checkout applies zero SQLite busy
timeout before view refresh, including for new connections, restores the prior
setting on return and keeps at most eight idle handles. SQL busy retries poll
the actual request context/selection; SQLite/provider work stays off Engine.
Public range contexts weak-link worker stop without cancelling caller tokens,
and adapter fences are checked during Engine and SQL acquisition. Cooperative
waiting is bounded; arbitrary blocking provider/OS I/O/fsync is not forcibly
interrupted. No new worker thread, retry daemon or inline transfer fallback.

Regression qualification uncovered a preexisting legacy authority race: global
stream reservation could fail, then its own credit could return before the
pending-work check. Seeing no pending work after that return prematurely ended
an unfinished round and changed its nonce. The sink now increments a local
credit epoch after dropping returned proofs and before clearing its retiring
count. Progress across refusal/pending makes the runner retry capacity before
yielding; no progress still yields to other scopes. Empty retirement does not
advance the epoch, and unwind releases proofs/count. This is readiness evidence,
not a durable generation or new authority; budgets and round caps stay intact.

Final qualification passed **369 reported cases on Windows** and **376 on
Linux/WSL**, in eleven targets per platform: library 113/120,
content_mount_reads 18, content_transport 44, content_executor 14,
content_provider 12, content_retention 9, content_saves 50, content_write 42,
contracts 41, overlay_map 17 and pending_saves 9. Strict Clippy for library/all
test targets, formatting and diff checks passed. Rust/Clippy 1.96.0 is used on
both platforms. Nine default ignores remain per platform: two library opt-ins,
the provider 1GiB opt-in, four parent-invoked crash helpers and two kernel mount
opt-ins. Platform repetitions are not 745 distinct cases.

Eight new ordinary reader parents cover exact waiter scope/stale verified-index
repair plus independent metadata and stable UUID reuse after reopen/save;
independent cancellation of two waiters; same-byte new-event and grant-withdrawal
fences; a held SQLite writer during first UUID allocation under a 35ms deadline,
with prior timeout restored; adapter slot saturation while a ready range still
reads, then stall/drain; private missing content without remote demand or UUID
allocation; worker stop without cancelling the caller token; and 64 pending
native readers, extra refusal and capacity/pin/idle-connection drain. A new
real-peer adapter parent reads 257 bytes of a three-chunk revision while the
source itself lacks the unrelated third chunk. The existing two-other-repository
saturation test now triggers demand through the actual adapter read API, keeping
the 250ms admission bound and requiring the result before stalled background
completion. One new legacy parent forces two 600ms retirements and an 850ms
post-refusal/pre-pending stall; data completes within the existing 5s bound under
one original nonce. The existing unwind unit also verifies zero/one credit-epoch
increments without adding another reported case.

Two explicit ignored parents passed separately on each platform, with one real
child apiece: the prior local mounted read/refresh test and a new real WinFsp/FUSE
mount fetching a missing range through an authenticated peer. The application
reads the correct 257 bytes and known 196608-byte length while the source's third
chunk remains absent. These four platform/parent executions are supplemental,
not added child successes or evidence of the full mounted build/load matrix.
Only isolated temporary fixture stores and peers were used.

`test-evidence/wave-6/mounted-demand-checks.json` retains commands, outputs,
per-platform source/executable identities and separate supplemental mount
results. All 377 Cargo/build/src/tests/native/ui inputs were frozen before final
qualification and rechecked afterward; nine existing inputs changed, none added
or removed. Source SHA-256:
`5489abad90d74c8f979d30a57c23bf6fc27d9e3eb08ce14787f6e9421b887b4f`.
The local ignored source ZIP is read back against every member hash.

Cargo selecting opt-ins rebuilt two Linux test paths. Final evidence therefore
uses a direct sequential rerun of all eleven actual release test images after
the opt-ins, without compilation, with SHA-256 checks before/after each run and
at completion. Supplemental images also remain unchanged since their own log
completion. The first verifier encountered a vanished WSL /tmp build cache at
the next invocation; no missing image was attributed. Linux was rebuilt and
requalified in persistent `/home/figloalds/.cache/tkfs-wave6-release`, then its
images directly verified. Original Cargo runs and verifier repetitions remain
raw evidence, not additional counted successes. Earlier mutable Windows guards
and the Linux two-nonce failure remain visible in the diagnostic record; no
assertion, timing bound, resource limit or original release gate was waived.

The complete Wave 6 goal remains active. Native mounted COW writes and complete
close/fsync/namespace/open-handle/startup/retry/lifecycle/status integration,
automatic close-producer coalescing, compatible metadata batching and slow-fsync
independent private readers remain required. Full root registration/deletion
barriers/actual GC races/live retained fallback/receipt-owned retirement and
sustained storage progress remain open. All residual WP6.3, WP6.5 and WP6.6 work
and original platform/two-peer/build/load/outage/soak/ENOSPC/power-loss/OS-close/
allocation/physical amplification/compatibility/activation/handoff gates remain
required. Isolated native demand tests do not close that complete release matrix.

### WP6.4 save metadata outside Engine (CCR-WP6.4-15)

The existing headless `SaveExecutor` held Engine while selecting checkpoints,
publishing a prepared revision and obtaining continuation metadata. That included
SQLite writer waits and FULL commit/fsync. These save-specific operations now
use zero-timeout secondary connections outside Engine. Short Engine acquisitions
check root, active intent branch, checkout generation, stop and switching; the
existing observation publication follows durable commit and rechecks that view.
The coordinator also refuses a quarantined primary and a caller's open primary
transaction rather than escaping it through a secondary connection.

Fenced receipt observation now explicitly advances `EngineVitals.store_revision`
without SQL so the status cell notices a secondary publication. The primary's
`total_changes` sees only its own writes. Review found this before final freeze;
the blocked-writer parent additionally asserts the revision advances. Retry may
advance it again: it is a freshness signal, not a durable event count.

When registered, the worker's connection pool is cloned by sharing its Arc and
eight-idle limit. Standalone headless callers use their own pool. Existing four
save workers and 64 outstanding operations remain; no additional threads or
larger queues. SQL writer acquisition polls the job deadline/cancellation, and
the secondary restores its previous busy timeout on return. A connection with
an open transaction or failed rollback is quarantined/closed, never reused.
Panic on a secondary cannot poison Engine. Private Saved and pending state stay
recoverable through failure, receipt replay and final continuation/adoption.

Six new executor parents exercise a real publication transaction stalled for
1000ms while Engine readonly metadata, another private write and another enqueue
remain live; an external SQLite writer with a 200ms job deadline followed by
successful retry; publication panic without Engine poison; SQLite authorizer
denial of rollback on an actual seeded worker-pool connection, quarantine and
fresh retry; old committed-branch receipt refusal after checkout; and refusal
of a caller primary transaction with Saved retained until retry after rollback.
Existing checkpoint-group64, bounded queue/shutdown, lost replies, newer writes,
exact journal ownership and real process-exit recovery tests remain relevant.

Final qualification passed **301 reported cases on Windows** and **308 on
Linux/WSL**, in eight targets per platform: library 113/120, content_executor
20, content_retention 9, content_saves 50, content_write 42, contracts 41,
overlay_map 17 and pending_saves 9. Six default ignores remain on each platform.
Strict Clippy for library/all test targets, formatting and diff checks passed
with Rust/Clippy 1.96.0. These are scoped save regressions; mounted read/transport
and kernel opt-ins were not rerun in this stage. Platform repetitions, crash
children, preliminary runs and direct verification do not add distinct cases.

`test-evidence/wave-6/save-metadata-checks.json` records commands, outputs,
source/executable identities and limitations. All 377 Cargo/build/src/tests/
native/ui inputs were frozen before final qualification and rechecked afterward;
four inputs changed from the mounted-demand stage, none added/removed. Source
SHA-256:
`9213252c24184e6d9b67f15f4a0d62fdb37637de7aafaf389800fb2b3ddc321d`.
Every archived source ZIP member was read back against its manifest. After all
compilation, each platform's eight actual executable images were hashed and
directly run sequentially without compilation, with matching hashes before/
after each run and at completion. Reported totals use that verified execution
once per case.

Initial compile diagnostics, the pre-status-invalidation snapshot and incomplete
inline WSL exit capture remain diagnostic records. The final saved Bash runner
captures lint/test exit codes explicitly through a login shell; an initial
non-login invocation lacked Cargo on PATH. No source assertion or release gate
was weakened for those harness corrections, and repetitions add no successes.

This is a concurrency prerequisite, not mounted COW write activation. The
mounted/legacy-session guard remains enforced. Other legacy primary metadata
mutations may themselves still hold Engine while waiting for SQLite; short
observation metadata reads also stay under Engine. The proof covers independent
readonly metadata and other private writes, not concurrent SQLite writers or
mounted notification capture (which is None in this headless path).

Native COW write/read/truncate/close/fsync callbacks, durable namespace and
startup/retry/open-handle/lifecycle/status integration remain incomplete.
Automatic close grouping, compatible metadata batching, private-reader
independence during FULL fsync, complete root/retention/deletion/GC/storage
progress, aggregate runtime budgets/shutdown/soak, WP6.5/6.6 and all original
Wave 6 release gates remain required. No schema/canonical/wire/event change,
private user_version 4, no live activation/migration or gate waived.

### WP6.4 native existing-file COW adapter integration (CCR-WP6.4-16)

An explicitly installed native workspace now attaches existing descriptor files
to the private `WriteJournal`/`SaveExecutor` pipeline. Installation requires the
existing isolated fixture flag before mounting/opening; no production path
automatically activates repositories. Existing-file readers and writers share
one stable `SaveFile`. Metadata capture compares branch, checkout generation,
descriptor and content/alive parents inside the real SQLite writer transaction
before superseding ownership, then final installation revalidates the handle.
Pinning, private creation and all byte work remain outside Engine. Same-entity
preparations join one key; at most 64 distinct preparations are accepted.

Actual WinFsp/FUSE callbacks route native reads, writes, atomic append,
constrained writes, truncate and allocation semantics to the private file.
The per-file gate orders mutable reads, writes and flushes outside Engine,
including large borrowed requests processed in bounded pieces. Cached length
tracks private commit even after a lost buffered-write reply. Explicit fsync
and native close/cleanup, including burst close, wait for FULL Saved, local
metadata publication and continuation/adoption; no peer wait. Native failures
retain dirty/Saved data and use existing mount reporting, including WinFsp void
cleanup. FUSE release now runs on the bounded request worker pool, and native
O_TRUNC/size setattr performs preparation and byte work off Engine.

Private buffered/checkpointed epochs prevent an under-Engine retain from
claiming newer native data was checkpointed. Force-detach persist/release/lost
and forced worker stop checkpoint native files outside Engine first. On explicit
installation/reopen, bounded Saved enumeration restores up to 64 active-branch
existing-file operations as visible dirty/nonquiet sessions before handles open.
Uncommitted ordinary Working is still not a promise. Committed earlier receipts
replay without another metadata event. Inactive, deleted or competing operations
remain recovery-required with their private data retained.

Eleven ordinary native parents cover explicit activation refusal; joined two
writers/reader and one publication; full overwrite without an absent base chunk;
shrink/grow holes and cached length after a lost write reply; 128 atomic appends;
a started 350ms write ordering before flush while Engine remains available;
same-byte newer-event rejection before owner creation; a 100ms metadata writer
deadline with Saved visible after reopen and retry; the actual force-detach
retain phase stalled 350ms before journal commit while Engine stays acquirable;
real exits86 before preparation and after native publication with exactly one
replayed event; and a 1000ms native publication stall with another native file's
private write/read plus readonly metadata completing within 300ms.

The first final-source Linux direct-image regression failed the existing
same-round legacy native-write test. Its retained trace identified a local
`WORKER_BUSY: object effect commit in progress`, followed by round retirement
after 3.5 MiB and a second round for the remaining 512 KiB. The source was the
write admission try-lock overlapping the same-family `objects.want` effect
snapshot. Experimental progress-observer/counter changes did not fix it and
were removed; their diagnostic sources and failures remain archived.
Write admission now waits outside Engine/lease state/object gates within the
existing shared two-second order deadline, checks cancellation/stop and stale
run ownership, then revalidates through `claim_run` under the acquired gate.
Twenty isolated repetitions of the originally failing case passed after this
fix (diagnostic repetitions, not additional cases). A new controlled actual
want/write test forces a 350ms family-gate hold and requires the complete 4 MiB,
one sampled live lease nonce and no source backoff. A new unit checks
deadline/stop refusal without claiming or changing work. The final Windows
direct-image sample records two overall download-job invocations and one live
nonce. The report initially rejected a stricter count-one check; inspection
confirmed that counter includes empty anti-entropy invocations. The sample does
not attribute its second invocation or prove zero redundant rounds/bytes. The
unfiltered count and limitation are retained, with no Rust assertion/source
change or rerun to erase the sample. No release gate or assertion was relaxed.

Final evidence: [native-write-checks.json](../../../test-evidence/wave-6/native-write-checks.json).
All 379 Cargo/build/src/tests/native/ui inputs were frozen as
`69686ba60c111130de930885358585f18fd640141811af6e004acb2f4e833ed1`.
Strict release Clippy with `-D warnings`, formatting and diff checks passed.
Twelve selected targets passed **388 Windows / 395 Linux** main cases, with
11 default ignores on each platform; these are separate platform counts.
Actual executable hashes were checked before/after direct execution of all
twelve images and again after the additional lifecycle build. Four focused
legacy stop/detach cases passed on each platform, without changing those twelve
images; these are not complete lifecycle suites or extra native-kernel tests.
Each platform also passed one explicit isolated real-kernel parent (WinFsp or
FUSE): write 7 bytes, truncate to 70,001, append 4, fsync, verify 70,005 bytes,
close/reopen equal and verify the metadata oracle. Child successes and repeated
Cargo/direct runs are not additional distinct cases. With native publication
held for 1,000ms, the other native file's private I/O plus readonly metadata
took 0.879ms Windows / 0.753ms Linux in the final samples (300ms bound).
Every member of the exact source archive was read back and hashed. Prior failed
and rejected diagnostic sources/logs remain separate; no user mount, migration
or production activation occurred.

Durable native creates/names/delete/rename/atomic replacement and retained
open-handle identity remain incomplete. Unsupported open-handle namespace paths
are refused, and void-cleanup delete semantics are not qualified. Native COW
base reads/preparation do not yet register authenticated remote demand; missing
bytes retain an unavailable result and Saved data. Generic provider mounted
COW, long-lived writer metadata/time rebasing, autonomous startup/namespace
recovery, complete production lifecycle/status and panic/accept-error buffered
exit handling remain open. Automatic close coalescing/metadata batching, private
readers during journal FULL fsync, complete native retention/GC/storage progress,
aggregate runtime budgets/shutdown/soak, WP6.5/6.6 and every original release
gate remain required. No persistent schema/canonical/wire/event change, private
user_version 4, live activation/migration or gate waived.

### WP6.4 authenticated missing-base hydration (CCR-WP6.4-17)

Native COW reads and canonical preparation can now register authenticated
remote demand for an unavailable immutable base. A process-local optional
session hook uses the actual SaveIntent event/entity/workspace/descriptor,
native provider identity, current grant generation and checkout/file ownership
fences. Secondary SQL and byte verification run outside Engine. Adoption keeps
the hook with the new intent; explicit Saved recovery attaches it to the new
worker. The generic provider API and durable records are unchanged.

Missing builder pages carry a local one-byte path position. For structural
absence, rebuild releases its buffers/journal guards before waiting, then revalidates Saved and
retries. Structural demands use their reserved class and exclude data chunks;
partial base reads separately demand the required bytes. Full overwrites do
not hydrate old data, and unchanged chunks can remain absent. Native mutable
reads reserve adapter slots before the file gate; missing-base waiters share
the existing global 64 limit. Every wait checks deadlines, cancellation,
worker/fence state and reference authorization, including same-byte regrant.

Five new transport parents cover a needed range with another private chunk
already changed, partial flush with an unrelated source chunk absent, full
overwrite with old bytes absent at both peers, Saved reopen/retry with one
publication, and deadline/withdraw/regrant/stop/cancel preserving Saved/private
bytes while releasing demand. A callback test proves capacity is reserved
before waiting on a started write; an object-engine test proves structural
requests use their own class without fetching the data chunk, then foreground
data work proceeds after release. An explicit isolated real-kernel parent
writes seven bytes, hydrates only the needed base for fsync, verifies/reopens
the first 64 KiB and preserves an unavailable unrelated chunk.

The first full-overwrite diagnostic failed because standard error-chain
traversal did not recover an anyhow context payload. Extraction now uses
anyhow's context-aware downcast; the initial four parents then passed. A test
accessor visibility error was fixed using the existing public Cow variant,
without broadening the production API. Early builds/runs were diagnostic;
final qualification uses a frozen source set and exact executable hashes.

The first complete attempt froze 380 inputs as
`cffe482c0337b4f749c0a79a328f9e07caedcea414c34dd6fc30192ed808909a`.
Strict Clippy passed on both platforms. Windows passed all 13 selected targets,
three explicit isolated real-kernel parents and direct verification of its
13 executable hashes. Linux's transport suite failed the existing
`native_capacity_return_between_refusal_and_pending_snapshot_keeps_round_authority`:
two live nonces, six successful pulls, 1,048,583 bytes written. All five new COW
parents passed in that same run. This failure prevents complete stage
qualification; no assertion was relaxed or failing sample discarded. The exact
380 inputs and raw attempt outputs were archived/read back in
`target/wave6/cow-base-attempt-cffe-before-credit-trace.zip`
(SHA256 `ae046f36b6b47838e5de797a100d8babcbdffe89f890090d16162903e1dbac26`).
At that point, error propagation, local capacity retirement and same-family
readiness were still under diagnosis. Windows's frozen-source process completed before any
diagnostic source change.

Thirty isolated diagnostic repetitions of the capacity-return case passed,
but the instrumented full transport sequence failed the existing pending-write
case. Its round exited with `local_error=WORKER_BUSY: object effect commit in
progress` and reason `failed`, after 3,932,160 bytes, followed by a 262,144-byte
round. This identifies want readiness independently of the capacity hypothesis;
no additional StreamLease/credit-observer change was made. The exact diagnostic
source/log snapshot is `target/wave6/cow-base-credit-diagnostic-snapshot.zip`
(SHA256 `d31c28c8ee953483ab3f8653120fcc6c959e83a1aaa61a8116515318d10231ad`).
All 380 original inputs were restored exactly before implementing the fix.
Both want phases now share the cooperative family-gate helper and one existing
two-second deadline, checking caller cancellation while waiting and revalidating
ownership under each acquired gate. The existing post-grant cancellation
rollback remains. A new unit drives both actual want phases through wait,
success and cancellation without new grants. A diagnostic grant/shutdown case
then exposed the worker-stop fallback bypassing the caller's cancellation
reason/observation; the helper now invokes that caller check before its generic
worker stop result. The existing 24-scenario offer-retirement parent passed
afterward, without assertion or timing changes.

Final mechanism evidence: [cow-base-checks.json](../../../test-evidence/wave-6/cow-base-checks.json).
All 380 inputs were frozen as
`6a3f096f991e2a9898f1e1a7256068913041a73e75b20a6ea993e8bd85843140`.
Strict release Clippy (`-D warnings`), formatting and diff checks passed on
the recorded Rust 1.96.0 toolchains. Thirteen selected targets reported
**413 Windows / 420 Linux** main passes, with 12 default ignores/platform.
The preexisting nonignored `receipt_crash_worker` driver accounts for one
ordinary early-return pass/platform, not independent recovery evidence; its
actual crash parent is reported separately in the same suite. Three explicit
isolated real-kernel parents passed per platform: authenticated partial COW
preparation/fsync/reopen, authenticated readonly range hydration, and existing
native write/truncate/fsync/reopen. Child successes and repeated Cargo/direct
runs are not additional cases. All 13 executable hashes matched before/after
direct execution and the kernel runs, and all source-archive members were
read back and hashed. Failed attempts/diagnostics remain retained.
This qualifies the interim authenticated mechanism and regression fix, not
the unfinished no-peer fsync/builder-independence contracts described below.

Generic-provider canonical rebuilding, legacy conversion hydration and fetching
missing reused chunks of a new root before peers learn its new authorized
reference remain unqualified. Native namespace/startup/lifecycle completion,
batching, complete retention/GC/storage progress, aggregate budgets/shutdown/
soak, WP6.5/6.6 and all original release gates remain open. No activation,
migration, live CAS deletion or schema/canonical/wire/event change occurred.

Contract audit found a required next step: this interim integration can fetch
base bytes/pages during fsync, while the authoritative plan requires fsync
without a peer. Necessary hydration must move before mutation acknowledgement,
and the fsync coordinator must remain local-only. Also, structural absence
releases builder buffers before waiting, but partial-data reads from inside
rebuild still hold those buffers; that guarantee is not complete. These are
implementation gaps, not a reinterpretation of the plan as merely avoiding
replication acknowledgement. Authentication/preparation evidence does not
close either contract. This preflight/local-fsync change takes priority over
further native namespace work.

The next implementation should preflight only the mutation's required base
bytes/manifest paths against a stable Working selection, without hashing or
installing a canonical result and without forcing a FULL checkpoint for each
ordinary write. A preflight visitor must return/release its buffers before
remote recovery, then revalidate owner/sequence. Native write/resize should
finish that preflight before acknowledgement. Canonical flush/fsync must use
only verified local inputs and retain/report unavailable data immediately;
disconnecting the peer after a successful mutation must not prevent fsync.
Full overwrites must continue avoiding old chunk bytes. Rework the interim
tests around that required boundary rather than preserving network-in-fsync
as a compatibility promise. The original complete Wave 6 scope stays intact.

### CCR-WP6.4-18 — pre-mutation base preflight and strictly local fsync (2026-10-21)

The native coordinator now preflights prospective write/resize against an exact
Working selection before changing private bytes. A read-only canonical-shape
visitor verifies required existing pages/old byte gaps, skips fully covered old
chunks and new zero ranges, and constructs no new content hashes, CAS objects
or FULL checkpoints. Input verification still hashes existing objects. Missing
native inputs return from the visitor before authenticated BaseWait: all three
background buffer permits, cursor/scratch and journal guards are released;
file state is also unlocked. A concurrent private mutation invalidates the
selection before incoming bytes can apply. Local preflight and bounded write
pieces check request cancellation and the mount's atomic fence.

Recovered/headlessly modified sessions include existing dirty paths in their
next native preflight. Partial failures retain that process-local marker; full
native success clears it. Canonical prepare and checkpoint reads now use only
local content and cannot invoke BaseWait. Fsync first retains FULL Saved, then
prepares/publishes locally. Missing required local base after an acknowledged
write returns unavailability immediately, retains Saved/private bytes and can
retry after local restoration, with zero demand-hook calls. Legacy conversion
stays local; a fully private replacement can resize/prepare without the absent
original object. No durable/schema/wire/event format changed.

Reworked authenticated cases disconnect the peer after write/resize and before
fsync, including the isolated real WinFsp/FUSE write/sync/reopen parent. Three
new transport parents cover missing-local-base fsync, stale preflight preserving
concurrent bytes, and shrink/grow zeroes. A new 1 GiB deduplicated-tree/4 KiB
edit parent bounds preflight to 128 KiB existing data reads and at most eight
manifest page reads, with no CAS install or FULL checkpoint; fixture seeding
is excluded. A new legacy unit also checks local preflight cancellation.

The first missing-local-base diagnostic used an unsupported peer `hello` probe
after its fsync assertions; changed to the existing authenticated capabilities
probe. The new legacy unit initially failed compilation because `checkpoint`
returns an option; corrected its unwrapping. Draft Linux then passed all eight
ordinary COW parents and the legacy unit. These diagnostics are not additional
unique passes or full qualification. No assertion, timing gate or scope relaxed.

Final evidence: [local-fsync-checks.json](../../../test-evidence/wave-6/local-fsync-checks.json).
Qualification used 380 frozen source inputs:
`24b26c2ccd572c15b34fbb7f543a927912aecb4880b4970c21d4e464ba6cbfb9`.
Strict release Clippy (`-D warnings`), formatting and diff checks passed.
Thirteen selected targets reported **418 Windows / 425 Linux** main passes,
with 12 default ignores/platform. The nonignored `receipt_crash_worker` driver
accounts for one early-return pass/platform, not independent recovery proof;
its actual crash parent is included in the same suite. Three explicit isolated
kernel parents passed per platform: new offline COW write/fsync/reopen,
authenticated readonly range, and native write/truncate/fsync/reopen. Child
successes and repeated Cargo/direct executions are not additional cases.

All 13 actual executable images per platform matched their recorded hashes
before/after direct execution and the kernel runs. All 380 exact source-archive
members were read back and hashed. Archive:
`target/wave6/local-fsync-source-final.zip`, SHA256
`3b51fb821297d558e50b79f1547cdae3c3239eef1800c47b0347c6829b8a5f74`.
Final direct Windows/Linux samples respectively: missing-local-base fsync
failed in **2.9952 / 6.178277 ms**, zero demand-hook calls, Saved/private bytes
retained and local-restoration retry successful (300 ms bound). Independent
canonical import while preflight waited completed in **1.6669 / 4.184768 ms**,
with file state/journal available and preflight buffer permits released
(300 ms bound). The 1 GiB/4 KiB edit read **131,072 bytes and six pages** on
both platforms, installed zero CAS objects and forced no FULL checkpoint;
preflight took **4.896 / 4.703179 ms**, excluding fixture seeding. No timing
target was relaxed. These mechanism results do not replace full build/load,
RSS, storage-amplification or release qualification.

This corrects CCR17's network-in-fsync and partial-data buffer-wait gaps; the
earlier evidence is preserved as history. Structural demand after dispatcher
idle/saturation still has unqualified admission latency (existing backoff can
reach 640 ms against the original 250 ms gate). Generic-provider rebuilding,
remote legacy conversion and serving missing reused chunks before peers learn
a newly published root's authorized reference remain open. Native namespace,
startup/lifecycle, batching, complete retention/GC/storage progress, aggregate
budgets/shutdown/soak, WP6.5/6.6 and every original release gate remain required.
No production activation, user mount, migration or live CAS deletion occurred.

### CCR-WP6.4-19 — structural demand after idle behind stalled background (2026-10-21)

The CCR18 admission gap was reproduced without changing the timing policy.
An authenticated transport parent waits for seven actual empty structural
preparations of its own repository, removes its verified local root manifest,
and starts a COW write while two other actual background source reads are
stalled for 4,000 ms. Per-group diagnostic snapshots expose capped empty-poll
counts and remaining scheduled delays; neither a sleep nor another group's
counter stands in for the tested idle phase. The new snapshot is bounded by
the existing group page and performs no SQL or provider I/O.

Baseline: **639 ms** remained after seven empty preparations; the new demand
was not admitted by **250.36645 ms**, missing the original **250 ms** gate.
The write eventually completed at **693.925347 ms**; page repair, private bytes
and subsequent local fsync succeeded. Both stalled background completion
counts remained zero. No assertion was relaxed. Exact 380 source inputs
(`e7a8c416a7b79a344bc86b2e601e621a18ff32118ebeb60dd0608665ae009b34`)
and raw failure outputs were archived/read back in
`target/wave6/structural-idle-before-fix.zip`, SHA256
`9680c96ec556a0346af0d198b6da1b83e3039237c55cea2aa606bd70a73e7720`.
That archive's index used Windows pathlib ordering for its aggregate source
hash. Readback of the unchanged exact members with the final qualification's
UTF-8 string ordering yields canonical source identity
`9fc85d9db557f49b08369a07932ce9d006b00a08629a768debb9e689870c8fd0`;
both identities and algorithms are retained in
`target/wave6/structural-idle-baseline-identity.json`. The original archive is
unchanged.

Empty structural preparations now poll again after **50 ms**, independently
of foreground's existing 10 ms and background's unchanged exponential delay
up to 640 ms. Existing workers, class/byte/queue admission, authorization,
source selection and cancellation remain. This adds no notification dependency
and does not claim a global latency guarantee for arbitrary repository counts.
The focused corrected Linux case passed at **50.714661 ms** admission and
**85.921549 ms** write completion, with background completion counts still zero.
This preliminary case is not an additional unique pass or full qualification.

Final evidence: [structural-idle-checks.json](../../../test-evidence/wave-6/structural-idle-checks.json).
Qualification used 380 frozen inputs:
`cb68b61144bdc9edb3cde9d7183d2604b4b00ba126943b08f08adba06897f435`.
Strict release Clippy (`-D warnings`), formatting and diff checks passed.
Thirteen selected targets reported **419 Windows / 426 Linux** main passes,
with 12 default ignores/platform. One `receipt_crash_worker` early-return
driver/platform is included in those harness counts, not independent recovery
proof; its actual crash parent is in the same suite. Three explicit isolated
real-kernel parents passed per platform. Repeated Cargo/direct runs and child
successes are not additional cases. All 13 actual executable hashes/platform
matched before/after direct execution and kernels, and every exact member of
the 380-input source archive was read back/hashed.

Final direct Windows/Linux admission was **94.2927 / 51.516882 ms**, below
the unchanged **250 ms** gate, after seven actual empty preparations. Remaining
scheduled delay snapshots were 48/49 ms. Both background completion counters
remained zero while their actual source reads were stalled for 4,000 ms.
Write completion was **171.2483 / 100.979839 ms**; repaired page, private bytes
and local fsync passed. Archive `target/wave6/structural-idle-source-final.zip`,
SHA256 `7aca750c7d9be9c87d7375f74f5b4b602d580de81ad44985b82d8f51e6c31bb9`.
This closes the explicit new-COW structural repair after idle/two-stalled-peer
scenario, not aggregate fairness/CPU/RPC cost, all repository/source counts,
full outage/load/30-minute soak or any other original Wave 6 gate. Production
activation stays off.

Next implementation priority is native namespace/lifetime completion. Audit:
`Engine::open_with_attributes(create=...)` still calls the legacy creator;
new files do not acquire native descriptors through `NativeWrites::install`.
Also, ordinary local `basic`/`location` changes deliberately fence publication
ownership in `core/content_saves.rs`. A long-lived native writer needs an
explicit validated metadata/time continuation rebase, not removal of those
fences or silent parenting of competing remote changes. Preserve the existing
stale-publication tests, stable open-file identity and crash-safe receipt/journal
handoff while adding create/rename/delete/replacement semantics. Namespace
metadata and canonical work must stay outside Engine waits/fsync. Immediate
durable namespace commits and eventual compatible batching need separate
correctness/performance evidence; successful content fsync alone does not
qualify namespace recovery or atomic replacement.

### CCR-WP6.4-20 — owned basic-info metadata and capture rebase (2026-10-21)

Core now offers `apply_content_save_basic` for an exact registered SaveIntent.
Under one FULL WAL writer, a private proof exempts only its exact local
basic-info event from ordinary ownership invalidation. Workspace, branch/view,
generation/owner and unselected/uncommitted capture are checked again inside
the event savepoint. Ordinary metadata continues fencing stale saves. Content
and alive parents, descriptor and private operation remain exact; no provider,
canonical or private-journal byte work occurs in this Core operation.

The basic event, bounded/checksummed request/event receipt and updated capture
plus successor lookup commit atomically. A mutation UUID supports exact
lost-reply replay after later publications; changing the request with that UUID
is rejected. The metadata event cannot reuse the future byte event's identity.
An optional capture policy binds explicit write/change-time retention to one
private sequence. Older selection is rejected, later bytes expire retention,
exact repeated selection preserves clocks, and superseding selection changes
template/selection together before publication. Published events never change.

Ten new ordinary parents cover bytes/parents stability, timestamp sequence
ordering, superseding selection, selected-capture refusal, ordinary metadata
fencing, successor/lost-reply replay, event-ID reuse, a preserved remote content
competitor and actual process exits before/during/after metadata commit. Eight
initial focused Linux parents passed. Integrity audit then added two stronger
parents; the first attempt reported one pass/two failures: ignored receipt and
valid-but-reverted selected template incorrectly acknowledged success. Exact
381 inputs/source
`1c02b5e0320a3e8f6101b160bac4bb8fc2d650b8031baaf9ceccfd24d7643cf5`
and raw failure outputs were archived/read back in
`target/wave6/owned-basic-before-integrity-fix.zip`, SHA256
`f82da0707f5d52619fb1b66e14809748baea60bfd92530499b4c991ad8f5efd4`.
Exact receipt/current-capture/successor/selected-template readback now rejects
ignored/deleted/reverted writes and rolls back the whole mutation. All ten
focused Linux parents then passed, including all four silent-write scenarios
and the three actual process exits. No assertion or gate was relaxed.

The initial `f6bcdeaa…` freeze was superseded before any final qualifier launched.
First frozen qualification used 381 source inputs:
`96db7eff4b96db65c7c84b8fecae04f858350b2a47711acbeb6855b919bf6198`.
Both strict Clippy runs passed, but this attempt failed Linux's
`classify_covers_every_code_literal_in_src`: 40 contract cases passed and seven
new codes lacked classification. The 60-case save suite and preceding targets
passed; later targets/kernel/direct phases did not finish. The Windows build
was deliberately stopped after verifying its process identity/creation time,
because it shared that deterministic gate failure. It was not restarted due
to an observation timeout. Exact 381 inputs and raw outputs are retained/read
back in `target/wave6/owned-basic-classification-attempt-96db.zip`, SHA256
`e247842cdb0929038aeaadcdef9a2e59cce6b8d226d8008972ef256e34faf57d`.

The seven codes now have explicit Wave 6 classifications and assertions in the
existing scope contract. Save-in-progress is contention without failure-count
escalation; receipt/continuation/owner/UUID integrity violations and invalid
internal timestamp policy are fatal only in the pending mutation/save scope.
Recovery bytes stay retained; historical classifications are unchanged.
The corrected Linux runs passed all 41 contracts and 60 saves before refreezing.
Fresh final qualification is in progress against 381 inputs:
`6f40fb3668ccb903ca7b96c24f762089fe629daaa614ed562e463a113a9b2c89`.
Selected suites, exact executable/source verification and archive readback
must complete before recording this stage as qualified. Its existing kernel
parents remain regression checks, not proof of native basic-info integration,
which is not wired.

This adds `content_save_metadata_changes` and an optional private metadata
capture field. New code reads old captures; older binaries using strict unknown
field rejection cannot read captures containing the new policy. WP6.6 still
requires its actual compatibility fence/migration/downgrade rehearsal.
Private write-journal version 4 and canonical/wire/event encodings are unchanged.
Receipt lifecycle/retirement and storage amplification remain part of the full
retention audit; a metadata receipt is not deletion authority.

Native integration must preserve metadata-only operation when base objects are
unavailable. The strict unselected-capture primitive above is not sufficient
for a failed, already-selected native save. A checkpoint-bound owned metadata
operation must supersede/rebase that selection without editing a published
event or forcing canonical publication. Before acknowledging metadata tied to
private sequence N, retain that sequence durably so recovery cannot strand a
policy beyond its newest recoverable Saved. A private FULL checkpoint can do
this without publishing unrelated bytes. Keep it outside Engine.

Windows timestamp-disable/reset flags must follow the handle that actually
wrote each private version. Persist their relevant write/change-time mask with
Working/Saved at the existing NORMAL/FULL boundaries, not a new FULL per
ordinary write; restore it for Saved recovery. Explicit timestamp retention
expires on a later byte mutation, while per-handle disabled updates apply to
that handle's later writes. Failed preflight without changed bytes must not
clear retention. These native callback/lifetime rules, create/rename/delete/
replacement, batching, complete retention/GC, aggregate budgets, WP6.5/6.6 and
all original full release gates remain required. Activation remains off.
