# Wave 6 contracts

2026-10-20. CCR-WP6.0-1. This freezes the new representation and ownership
contracts before their consumers. It does not activate repository features or
claim qualification. Runtime remains std/polling. Existing event strings retain
their original whole-file SHA-256 meaning.

## Representation v1

All integers below are unsigned little endian. Parsers reject trailing bytes,
unknown versions, algorithms and tags. Digests are 32 raw bytes in binary;
JSON uses exactly 64 lowercase hexadecimal characters.

`FileContentRef` is exactly 48 bytes: `TKFC`, envelope version byte `1`,
kind byte (0 legacy, 1 direct, 2 manifest), representation version byte
(0 legacy, 1 direct/manifest), algorithm byte (1 SHA-256), u64 total length,
32-byte digest. The descriptor is a separate typed value, never a string
reinterpreted as a legacy object hash. Legacy length is at most i64::MAX,
matching existing metadata; new-format maximum is 2^48 bytes (256 TiB).

New files of length 0..65536 are direct. The empty representation is a direct
descriptor of length zero and SHA-256(empty). Old empty legacy references
remain valid. Larger files use aligned 65536-byte chunks; the final chunk
contains precisely the remaining bytes, without padding. Chunks and direct
objects use SHA-256(raw bytes), permitting physical reuse. Sparse extension
is represented by actual authenticated zero-filled chunks in v1; there is
no hole tag and an unavailable chunk is never interpreted as zero.

Manifest pages contain `TKFM`, version byte `1`, level byte, u64 total length,
u16 entry count, then ordered entries: kind byte (0 chunk, 1 page),
u64 child length, 32-byte digest. Offsets are the prefix sum of lengths.
The identity is SHA-256(`tkfs:manifest:v1` + NUL + encoded page), not the
SHA-256 of the represented file. The page CAS namespace is distinct from
the raw-byte namespace.

A leaf has level 0 and chunk children. Level n has page children at exactly
level n-1. Fanout is 1..256; the encoded-node limit is 65536 bytes
(this format's maximum actual page is 10512 bytes). Child capacity at level n
is 65536 * 256^n. Every child except the last fills that capacity; the last
has the exact positive remaining length. The root uses the smallest level
covering the file. A rightmost internal page may have one child, preserving
uniform height. Root height is at most four for this maximum file size;
the absolute decoder depth bound is eight, with overflowing capacity rejected.
Parent length, expected child level, digest and coverage are checked on every
edge before bytes can be exposed. Construction appends full pages from left
to right; edits replace only touched chunks and their ancestor pages.

## Provider and mutable workspace boundary

Workspace ID, entity ID and monotonically increasing entity generation identify
a mutable session. A pending operation has a separate UUID, retained across
replay. They are never host paths or content hashes. A revision identifies an
immutable `FileContentRef`; local provider locators are process-local objects.

The provider pins an immutable reference and returns a lifetime-owned handle.
Range requests use checked offset/length and a caller-owned buffer no larger
than 1 MiB. They carry an absolute deadline and an independent cancellation
token. Availability reports available, missing or corrupt, never an implicit
hole. Reads verify the full containing data chunk before copying any of it;
legacy reads may verify the entire legacy object. A failed read may modify the
caller's buffer but returns no successful byte count. Providers must check
cancellation/deadline before each bounded I/O; blocking storage calls themselves
remain OS-bound. No success is returned after cancellation/deadline expiry.

An immutable snapshot-root handle owns its selected workspace generation and
content pins; cloning it preserves the same selected view. Existing disposable
startup snapshots are not these handles. Until persistent tree roots exist,
the interface is internal and does not expose a workspace-clone command.

A write session retains its base pin, dirty aligned chunk ranges, intended
length, entity generation and pending-operation UUID. A full overwrite does
not hydrate the old chunk; partial overwrites fetch only untouched base bytes.
Namespace intents retain parent/name, entity, branch and operation ordering.
Preparation, chunk hashing and durable checkpoint I/O occur outside the engine
lock. Commit revalidates the captured generation and retires the recovery record
only in/after the durable metadata commit. A failed or stale completion cannot
discard a newer record or publish over a newer revision.

## Adapter durability map

The current native shim's `Cleanup` and `Close` callbacks return void and
discard Rust status. Therefore successful Windows `CloseHandle` alone cannot
promise that a fallible checkpoint succeeded. Wave 6 must checkpoint during
cleanup before queuing finalization, retain and report failures, and document
this API limitation. `Flush` is fallible: `FlushFileBuffers` waits for installed
content plus the corresponding FULL SQLite commit. No peer is required.
Delete-on-close and rename must settle prior saves before committing namespace
changes; a later finalizer cannot resurrect a deleted entity.

On FUSE, `FLUSH` (opcode 25, close) checkpoints bytes and save/namespace intent
durably before returning success; it may defer canonical chunk publication.
`FSYNC` (20) commits the revision before success. `RELEASE` (18) is resource
release and must retain/report failures because applications do not receive
its errors reliably. Windows `Close` (Rust op 10) is likewise resource release.
Ordinary writes stay buffered. Repeated flush and recovery replay are
idempotent. Checkout/shutdown account for checkpointed, uncommitted operations.
No adapter callback claims a stronger promise than it can return.

## Engine coordination and authorization

Metadata transactions append root admission changes atomically with accepted
events: durable cursor, repository, branch, reference/event identity, authorized
root, generation and grant/withdraw. Metadata activation does not await roots
or chunks. Objects persist their own consumed cursor in the same transaction
as their idempotent admission effects. Notifications are hints. Recovery
reconciles current roots, retained histories and pending saves incrementally.

Admission is per reference and generation, never per hash alone. A root admits
only itself until verified edges admit descendants. Serving rechecks the
requester's current reference authorization before each frame; the committed
withdrawal is the local revocation linearization point. Already delivered bytes
cannot be recalled and offline revocation has no globally instantaneous point.
Retained-history pins do not grant disclosure. Bare-hash requests are rejected.

Demand contains repository/reference/revision, byte range, priority, waiter UUID,
deadline/cancellation and eligible peer scope. Installation can join by content
identity, but cancellation and grants remain waiter/reference-specific. Lease
release requires matching owner generation; an old run cannot release a newer
lease. Structural pages use a reserved object slot, never metadata payloads.

Workspace journal records carry workspace, epoch, sequence, generation and
affected entries/entities/directories. Cursor replay returns bounded ordered
changes or explicit rescan-required after overflow, epoch replacement or lost
continuity. A foreign SQLite commit invalidates cache freshness; no cache token
may be advanced past a concurrent metadata change without publishing its view.

## Default budgets

| Resource | Default and overload behavior |
|---|---|
| Queued dirty bytes | 64 MiB per worker; checkpoint synchronously under backpressure |
| Save workers / queued jobs | 4 / 64 per worker; caller performs bounded preparation when full |
| Verification | 8 buffers of 64 KiB per worker; structural reserve one additional 64 KiB |
| Provider read request | 1 MiB; larger requests must be split |
| Manifest expansion | 256 edges and 64 KiB input per slice, depth <=8 |
| SQLite transaction owner queue | 128 jobs; reserve 16 for foreground/metadata completion |
| Foreground demand | 64 independent waiters per worker; bounded readiness error when full |
| Object transfer jobs | 8 global, <=4 per peer and <=4 per repository |
| Reserved transfer capacity | 1 structural, 1 foreground, 1 background; borrowing revocable at frame boundary |
| Transfer buffer | <=1 MiB per active job; accounting charged before allocation |
| Change journal | 65536 records per workspace; oldest eviction implies rescan for old cursor |
| Cancellation / shutdown | poll <=50 ms between bounded work; 5 s cooperative drain, retain unfinished saves |

These are contracts for implementation and measurement, not assertions that
all current Wave 5 queues already enforce them. No engine-wide mutex spans
hashing, copying, fsync, network waits or demand waits. Namespace locks use
ordered workspace/parent/name keys; cycle checks and checkout additionally use
generation revalidation. SQLite writer wait, transaction execution, fsync and
checkpoint time must be reported separately.

## Compatibility, rollout and ownership

Activation stays unavailable until WP6.6 safety and compatibility evidence
passes. Both IPC and peer discovery must negotiate content-v1 before sending
events using it. Unsupported repositories stop at upgrade-required without
acknowledging unsupported events, classifying them as corrupt, or blocking
supported repositories. Historical events are never rewritten.

Wave 5 source has no global database-version rejection. Test its actual release
executable against an isolated disposable store with a future marker. If it
opens it, activation requires a distinct store path and launcher version fence;
an in-place marker is insufficient. The isolated new store uses a distinct
metadata filename unavailable to the old launcher; migration/export preserves
the original backup and causal identities. A new launcher must reject an old
executable for that store. Rollback exports post-activation work then selects
the separate backup, never downgrades an activated database in place.

GC retains current, historical, conflict, recovery, snapshot and read pins.
Unknown reachability retains bytes. A generation barrier serializes acquiring
pins with deletion; deletion cannot pass a reader's acquired pin.

Single integration ownership: `src/model/content.rs`, `src/content/`,
module registries, wire/schema/activation changes and this document belong to
the integrator. WP6.1 owns content representation/provider tests; subsequent
packages consume those frozen seams. Existing core/runtime seams transfer in
the order specified by WAVE_6_PLAN; changes to these contracts require a named
CCR in the integration notes. No delegation is implied.

## CCR-WP6.2-1: metadata representation and durable admission

The content register retains its legacy string encoding. A v1 register value
is the JSON object form of `FileContentRef`; decoding validates its fields.
The projected `Entry.content` remains an optional whole-file hash string.
`Entry.content_ref` is a distinct optional descriptor, omitted from old JSON
when absent. Exactly one represents a content register value. Derivation,
incremental successors, publication and restore copy the register's typed value
without opening a provider. Old event IDs and payloads are unchanged.

Admission changes also carry `shared` and `publishable`. The latter records
explicit local authorship/publication. An incoming reference does not make a
private local deduplicated copy sendable. Remote byte-receipt evidence and
verified descendant edges are additional object-engine requirements. A local
review alone does not create publication evidence.

`content_root_metadata` retains one root per event/entity reference, including
historical and withdrawn references. Reference identity is SHA-256 of canonical
JSON `["tkfs:content-reference:v1", event_id, entity_id]`. Each reference starts
at generation 1; changes of authorization increment it. Grant/withdraw outbox
rows are unique by reference/generation and have an independent monotonic
SQLite cursor. Their descriptor encoding is the frozen 48-byte binary value.
Root metadata and its initial outbox row are inserted inside the event's
transaction. Consumers commit their cursor and root effects together, in
bounded batches of at most 256 roots. No transaction expands descendants.

Revocation takes effect at the metadata authorization commit, even while an
object consumer is behind. A later generation supersedes earlier effects;
replaying an older consumer cannot restore a withdrawn grant. Lost replies to
grant/withdraw operations are idempotent for the expected generation. Historical
retention is distinct from current network permission.

Until WP6.6 provides the complete activation/deployment fence, typed metadata
is accepted only by isolated test fixtures explicitly setting
`config('fixture:content_v1')='1'`. There is no activation CLI. Normal stores
refuse such incoming events before staging with `PEER_NEEDS_UPGRADE`. Already
staged unsupported descriptors remain unacknowledged in an incompatible retry
episode, without a corruption/quarantine verdict.

Metadata preparation captures its branch journal generation. After acquiring
SQLite's writer and before changing projection rows, commit revalidates that
generation. A stale preparation returns `STALE_METADATA_PREPARATION`, retaining
the event for retry. View publication compares the projection counter before
and after commit and reads the data-version token before the final counter.
This permits adopting overlapping object bookkeeping without masking a
concurrent metadata commit. Unknown counters drop the cached view.

Wave 6 extends error classification through `model::errors::WAVE6_CODES`,
preserving the historical §8 rows. Cancellation and stale lease/reference
ownership are contention/readiness outcomes. Deadlines, corrupt received bytes
and stale metadata preparation are scoped transient outcomes. Unsupported
representation/conversion is incompatible. Invalid descriptors, manifest
coverage/arithmetic and admission invariants are fatal only in the affected
object/reference scope; their handling must preserve unrelated work.

## CCR-WP6.3-1: shared graph, receipt evidence and owned admission

The object index is a reusable immutable page DAG. `content_manifest_edges`
stores each page's canonical ordered children once. `content_object_jobs`
stores verification/expansion/retry state once per typed `(kind,digest)`.
Neither table grants network permission. New root admission is constant work;
one expansion reads one page outside SQL and writes at most 256 children in a
short transaction. A new revision reuses already-expanded pages without replaying
their descendants or updating every occurrence of an identical chunk.

Jobs retain a disposable root-relative witness of at most eight ancestors.
Page aliases bind a shared page to another root/reference/generation/offset.
After revocation, work can rebase its witness through an unchanged ancestor;
it proves the resulting path against current root metadata. It examines at
most 256 aliases per resolution, persisting a cursor across slices. Deferred
stale witnesses do not occupy an unbounded filtered SQL join. Retention still
keeps the immutable bytes and historical roots; aliases do not authorize GC.

Range lookup follows canonical positions through indexed page-child lookups,
bounded by requested range and depth. The sender can also accept an explicit
position, authenticate it against immutable provider pages, and serve while its
derived graph cursor lags. A forged derived membership row cannot substitute a
private chunk. Both position and key are checked, followed by content hashing.
Current shared reference authorization and generation are rechecked before
returning each frame. The eventual peer adapter must additionally authenticate
the requester's current repository/branch access. It is not connected yet.
Legacy whole-file reads retain their provider and streaming transport contract;
they never enter a bounded v1 chunk job or become corrupt merely for exceeding
64 KiB. The new frame API explicitly refuses legacy descriptors.

Physical verification, permission and publication provenance are separate.
An incoming shared reference does not promote a private-only CAS hit. Serving
requires current shared admission plus explicit local publication or durable
evidence of actual verified shared receipt for that typed object. Receipt
evidence alone never authorizes an arbitrary hash or a withdrawn reference.
Receiving verifies the admitted frame, syncs a bounded checksummed intent,
durably installs CAS bytes, commits the receipt/index, then retires the intent.
Replay verifies the actual installed bytes and generation before indexing.
Lost derived-index writes therefore do not require retransferring installed
bytes. Stale intents cannot mutate a newer grant. Corrupt bytes are denied;
verified repair preserves the old corrupt file in quarantine.

The shared scheduler owns eight transfer jobs globally, four per peer and four
per repository, charging at most 1 MiB per job before admission. Each scope
retains a slot for each absent priority class (foreground, structural,
background); current implementation does not lend those reserved slots. A
transfer has a random owner plus monotonic generation, and only that token can
release or complete its work. Waiter cancellation and deadlines are independent
of joined installation. Each provider/worker has 64 waiters and a 2 MiB demand
queue, within global bounds of 4096 waiters and 64 MiB. Provider identity scopes
in-process stores even if test repositories share the same repository ID.

Local verification has eight regular 64 KiB units plus one structural reserve.
Imports/overwrites charge three regular units, range reads two, raw-object
verification one, and page verification the structural reserve. Background
regular work leaves two units for foreground; foreground work leaves three for
background. No permit mutex spans I/O. Transfer preparation verifies at most
32 candidates/1 MiB per slice and expands at most one page. Explicit range
admission runs before background alias repair. Admission latency is measured
at first actual local service or owned/joined transfer, not queue insertion.
Demand can seed its one committed current root before the durable consumer
catches up; it does not wait for maintenance or replay unrelated admissions.

These are engine seams and package tests. The bulk exchange and continuous
dispatcher below now consume them; mounted adapters, the carried legacy lane
issues and sustained load qualification remain outstanding.

## CCR-WP6.3-2: bounded bulk and loopback content frames

`model::content_wire` defines a distinct `objects.content-v1.*` namespace.
`capabilities` negotiates version 1, the 65,536-byte frame bound and repository
activation. It never activates a repository. Normal repositories still report
inactive; only explicitly enabled fixture stores accept these object operations.
Peer negotiation is bound to authenticated session, repository and worker
instance, expires after 60 seconds and must precede `read`. Metadata descriptor
capability negotiation remains a separate WP6.6 requirement.

Worker `prepare` returns one root-relative offer for one lane and priority class.
`complete` and `release` name its random owner/monotonic generation token, exact
route and requesting run UUID. An expired or consumed offer cannot release or
complete a newer one. The pending map is bounded by the existing scheduler,
with a 15-second offer lifetime, maintenance/prepare reaping and stop cleanup.
Content calls use secondary connections with zero SQLite busy wait; returning a
pooled connection restores its previous timeout. No engine lock spans I/O.

Peer `read` carries the committed reference and typed object position, never a
bare-hash authorization. Its route is constructed from the authenticated peer,
managed/sync-enabled repository and replica binding. It uses immutable provider
membership proof plus current shared reference/publication checks, even when
the sender's derived graph has not caught up. A source frame is at most 64 KiB,
including a zero-byte direct object. Manifests remain on this bulk surface.

Each daemon shares one content scheduler across inbound and outbound work,
reserving 1 MiB before reading each frame to cover its bounded buffers and
copies, within eight jobs/eight MiB globally and four jobs per peer/repository.
A sent frame keeps this credit until an exact session/scope/token `ack`.
Expiry closes that session before returning credit, so a reply still queued
on a stalled socket cannot silently return its byte budget. Session close
reclaims only that session's credits; stale acknowledgments leave newer tokens
alone. The credit reaper polls at 50 ms and is owned by the daemon context.

Outbound work has at most four turns, retains one slot for each absent
priority class, and allows one turn per peer/class. This leaves service for
inbound reads and avoids symmetric outgoing work consuming both per-peer slots
of one class. V1 inbound jobs have a separate eight-job server reserve and four
slots per session, independent of legacy object/metadata exchanges. ACKs do
constant bounded map work on the authenticated handler, without waiting for a
worker or one of those exchange slots. Content uses dedicated loopback sessions,
so old bulk streams cannot occupy all of its handler threads.
The real-session regression fills eight legacy worker reads on two peer sessions;
the v1 frame and ACK still complete within its controlled 250 ms bound.

Foreground priority is carried through the worker/provider boundary. Frame
proof and byte verification use one foreground 64 KiB unit each, sequentially;
background pages retain the structural reserve. Actual authenticated peer reads
complete under the controlled test while two stalled import producers hold six
background units. Already-cached ancestor verification alone no longer records
a demanded data transfer as admitted.

Transfer discovery uses per-source/lane/kind keyset cursors, inspecting at most
16 jobs of each selected kind. A backed-off prefix cannot hide later objects.
Verified but unexpanded pages remain structural work. Cursor state is capped
at 4096 entries, and unchanged idle scans do not write SQLite/WAL. Existing
64 KiB content encoding, legacy object transport and historical identities are
unchanged by these derived-index and transport additions.

`content_transport::pull_one` exercises the complete receiver-driven frame path,
with session identity, lane, route and single-class checks. The continuous
dispatcher below uses the same preparation/frame phases. Mounted demand remains
unconnected. Existing legacy pull/push session IDs and their carried stale-run
issue have not been changed by this CCR.

## CCR-WP6.3-3: continuous dispatch and cancellation

Every outgoing bulk runner registers its authenticated connection with one
daemon-owned dispatcher. Four preparation threads (two foreground, one
structural, one background) are separate from four frame threads. There are no
per-repository threads. Planner loopback sessions are separate from content
data/completion sessions. A stalled remote read cannot occupy a preparation
thread or the old two-repository bulk turn gate.

The dispatcher retains at most 256 lane bindings, 512 repository groups and
four queued frames; admission charges the existing daemon-wide lease/byte
budget before queuing or remote I/O. More repository groups rotate through the
bounded working set, with their deferred count exposed. Worker connection
caches retain at most 64 clients each; source capability cache holds at most
4096 entries. Source polling examines at most 32 candidates, rotating the window
when necessary. Idle work backs off without rewriting SQLite cursor state;
cursor movement alone is not advertised as verified content progress.

Local preparation uses one logical lane to discover a typed object, then selects
the live authenticated physical lane that owns its digest. This avoids a prefix
of work for other lanes consuming the foreground candidate budget. Waiter work
rotates in bounded windows; eligible-source polling does not clone descriptors.
Unaligned 1 MiB demands can cover 17 chunks. Already verified objects do not
reconsume the preparation verification budget on every slice. Completion checks
include required manifest pages as well as leaves. An actual provider observation
of missing/corrupt bytes invalidates only derived availability; it neither changes
permission nor publishes bytes. Range service and sender proofs still verify the
actual immutable bytes.

Each lane binding has a random identity. Replacement cancels/closes the old
binding and creates fresh group scheduling state if its source set changed.
An old registration's Drop cannot remove its replacement. Peer and worker calls
can abandon an individual request via a nonblocking cancellation predicate,
checked at most every 20 ms during queue/reply waits. Ordinary session calls
retain their existing behavior. The source bridge propagates cancelled peer
requests to the shared worker session without closing unrelated exchanges.
Outbound reads, negotiation and installation also observe binding/daemon
cancellation. Exact offer release and known frame ACK have bounded cleanup
deadlines. A completion already executing still relies on generation validation
and durable receipt recovery; cancellation is not a rollback of committed bytes.

Controlled real-session tests demonstrate foreground admission within 250 ms
while two other repositories' background reads are actually stalled, background
progress while a foreground source is stalled, replacement within one second
during an old two-second read, and cancellation draining transfer ownership
within 250 ms without waiting for the stalled source I/O. These are short package
checks, not continuous-load or final platform qualification. Dispatch counters
separate admitted/completed/failed/retried work by priority; observing the view
does not start a content engine. User-visible status integration remains WP6.6.

CCR-WP6.3-5 below extends this dispatcher to explicit retained whole-file demand.
CCR-WP6.3-4 addresses retained pull-run ownership. Mounted demand, continuous
fairness/load qualification and the remaining WP6.3 integration work are pending.

## CCR-WP6.3-4: ordered ownership of retained whole-file pulls

Local pull lease IDs retain their classic per-peer/lane/slot base and append
`~w6.<incarnation>.<run>`, with each integer spelled as exactly 16 lowercase
hexadecimal digits. The complete ID remains within the existing 128-byte local
session bound. Both pull slots share one ordered run fence and keep distinct
lease IDs for pipelining. Whole-file identities and peer object messages are
unchanged. This is a local worker ownership extension, not content activation.

Every real daemon startup increments `settings.objects_incarnation` in its
locked catalog, using a short SQLite FULL-synchronous transaction before any
lane starts. Each lane creation allocates a monotonic in-process run number.
The pair orders independently of wall time. A startup crash after the commit
may skip an incarnation; it cannot reuse it. Malformed or exhausted counters
prevent startup rather than silently resetting. Manually constructed test
contexts have a process-wide ephemeral zero incarnation and touch no host files.

The worker retains the latest generation per owned pull scope. A newer run can
take over either older slot at its durable spool offset immediately, without
waiting for lease expiry or the older runner's cleanup. Wants revalidate their
generation after the store scan; writes check before byte work and again before
changing lease state. A stale write already installing verified immutable bytes
cannot renew/release the newer lease or apply source/local backoff. Its result is
ownership contention. Stale or unregistered-future releases change neither the
new lease nor its source backoff. The route's peer and want's lane must match
the encoded identity. Superseded calls use `IN_FLIGHT_ELSEWHERE` readiness
classification, preserving the historical cancellation code's classification.

Generation fences are capped at 16,384 per worker incarnation and are not evicted
while delayed requests may exist. At capacity, a new scope is refused as busy;
existing scopes can continue and advance. This preserves stale-request rejection
instead of making forgotten old generations admissible. A worker restart starts
a new in-memory fence set after old request handlers are gone. An adopted worker
must implement the matching generation extension; normal launcher version
checks still apply. Complete migration/downgrade rehearsal remains WP6.6.

This addresses the carried pull-run cleanup scope of CCR-WP5.INT-2. Legacy
cross-repository foreground admission (CCR-WP5.INT-1), the retained acceptor push
offer lifecycle, mounted demand and final sustained-load qualification still
require their own evidence; none is inferred from a pull generation test.

## CCR-WP6.3-5: reserved foreground admission for retained whole-file demand

The fixed content dispatcher also polls explicit legacy `object_requests`,
independent of the published folder heartbeat and typed-content activation.
Legacy background replication retains its existing two-repository gate. A
foreground adapter uses the shared content scheduler's foreground reservation,
one MiB of charged job capacity and the existing bounded network worker pool.
Preparation selects one demanded hash by indexed identity lookup; it does not
rebuild the background current/history want list. The local SQL connection has
zero busy timeout and a preparation call has a 50 ms reply deadline.

Each transfer sends at most 256 KiB through reads/chunks of at most 64 KiB.
Legacy bytes retain their whole-file SHA-256 identity: parts go to the existing
private spool, and only whole-file verification installs and registers bytes.
Neither a partial spool nor a claimed hash becomes a published chunk identity.
Unknown legacy lengths use the existing empty size-discovery part.

Local `c<16 lowercase hexadecimal digits>` frame slots share the parent pull
run's generation fence with `a`/`b`. A later frame can promote a demanded object
without fencing unrelated background objects of that run. Older frames cannot
take a newer active frame's lease. Cleanup and backoff require the exact current
lease owner; late frame or parent cleanup cannot penalize its replacement.

Optional peer `legacy-begin` reserves a source job before streaming and returns
an opaque credit. `legacy-pull` binds that credit to the exact authenticated
session, route, lane and hash, and consumes it once. The receiver checks the
returned token and byte count and sends the existing exact-owner ACK on success,
failure or cancellation. An active source handler retains an independent share
of its permit even if ACK, expiry, session close or daemon shutdown removes the
credit table entry. Source reads use the legacy authorization/publication checks
and the provider's foreground buffer reservation. Unsupported peers use the
existing bounded legacy pull and cannot provide the new source reservation.

This extension does not establish a complete legacy job/spool/lease budget,
push generation, sustained fairness, first-serve hash cancellation or
mounted-read qualification. Their remaining audits and the package test stages
are recorded in the implementation record. Typed metadata remains fixture-only.

## CCR-WP6.3-6: pull ownership across peer-pool replacement

Pull IDs now append the pool counter as a third 16-digit lowercase hexadecimal
integer: `~w6.<incarnation>.<run>.<pool>`. The full local ID remains at most
128 bytes, including a foreground `c` slot. The previous two-integer form is
still parsed. These are local opaque worker session IDs; legacy peer object
messages, file identities and metadata activation do not change.

A real peer supervisor allocates its pool identity when constructing its
`PeerContext`, before any lane starts. All lanes and reconnects of that context
retain the pool identity. Replacing the supervisor assigns a later identity,
including when a prior lane count returns. A lane from an older context that
starts late retains its older pool, even if its run counter is newer. Production
pulls refuse an unregistered pool. Hand-built fixture contexts register lazily.
The daemon registry holds at most 16,384 context/identity associations and only
reclaims associations whose context has no remaining strong references.

Workers retain at most 16,384 direction/peer family fences in addition to the
16,384 lane-run fences. Daemon incarnation dominates pool counter, which in turn
dominates per-lane run counter. Thus a newer pool takes over an old layout's
leases immediately at their durable offsets, without the historical 15-second
idle heuristic. Old requests cannot acquire another shard's free work, write,
release or apply source backoff after replacement. The checks run again after
SQL/byte work. An expired superseded lease is cleanup, not evidence that the
replacement source failed. Existing families can advance at capacity; new
families are refused without evicting a live fence.

Canonical pre-extension pull IDs are recognized as old ownership and are fenced
once a newer qualified family claims the worker. Arbitrary historical opaque
session strings keep their old meaning; launcher/version and adopted-worker
fencing still require WP6.6. These checks cover retained pull layouts. Inbound
push-offer ownership and its authenticated session/pool lifecycle still require
separate work; the local pull-pool registry is not an inbound ownership source.

## CCR-WP6.3-7: authenticated inbound push and offer ownership

After an authenticated bulk handshake, the acceptor assigns one immutable
session generation before serving requests. Sessions joining the same received
lane count share the acceptor's current pool identity; a newly authenticated
different count starts a new pool and retires the earlier sessions' push
authority. Returning to an earlier count assigns a newer identity. This is the
acceptor's ordering of authenticated connections, not a claim that the legacy
hello carries a persistent sender pool epoch. Control and probe sessions do not
allocate push ownership.

Every local offer receives a fresh `c<16 hexadecimal digits>` frame. The worker
ID is `push:<peer>:<index>/<count>:c<frame>~w6.<incarnation>.<session>.<pool>`,
within the existing 128-byte limit. The stable base still identifies the logical
lane. These IDs are generated locally, not accepted from peer object messages.
The local worker must implement this ownership extension; adopted-worker and
deployment capability fences remain part of WP6.6.

The worker orders pool, session and offer frame before admitting wants/writes
or changing lease state. It retains the latest push frame even after that
offer's object lease is released. A late old want cannot regain free work;
late writes, cleanup and hold requests cannot modify the replacement. Existing
per-lane and family fence bounds apply. Exclusions on a fresh want retire only
predecessor leases already superseded by its claimed owner; a current other
source's lease is untouched. Another source can acquire work left by an older
offer even when the newer offer selected a different object.

The daemon coordinates publication and status/backoff mutations through shared
ownership slots. The latest session/frame order survives while any offer or
handler holds the slot. Old replies cannot overwrite or consume a newer offer,
including when the newer offer was already consumed. Pool retirement and those
short memory mutations are serialized. No ownership lock spans worker RPC,
byte verification, socket streaming or durable I/O. Reaper releases use exact
IDs, so a replacement no longer waits for the old release RPC.

After a write verdict is collected, the native worker rechecks the owner under
the same short state guard that mutates source backoff. A superseded corrupt
verdict cannot apply a memory retry after a replacement installed good bytes.
Durable local-failure SQL projection and same-run foreground error interleavings
remain part of the full legacy audit; this memory guard does not qualify them.

The slot registry has at most 4,096 weak entries, reuses live slots at capacity,
and refuses new live scopes rather than evicting an owner. Reclamation rechecks
the key after dropping the registry lock, preserving one slot under concurrent
creation. Expired offers can be pruned before capacity checks; closed-session
cleanup removes only that session's table entries. The acceptor pool registry
has at most 16,384 peer identities, retains their ordering until daemon restart
and permits an existing peer to reconnect/change count at capacity.

Whole-file hashes, staged/shared authorization, private spool verification and
legacy peer message formats are unchanged. The three-argument standalone
`inbound_objects::handle` keeps its function seam for read/report routing;
network wants/pushes use the registered session owner. An unregistered direct
wants/push call has no worker authority. Existing close-release helper fanout,
complete legacy job/byte/spool budgets, final fairness and platform qualification
remain separate outstanding work. CCR-WP6.3-8 below adds cancellation within
first-serve hashing.

## CCR-WP6.3-8: interruptible legacy first-serve verification

Legacy `objects.read` and the reserved foreground `content-v1` legacy-read
operation keep whole-file SHA-256 verification before first serve. Their
loopback request deadline, cancellation/disconnect and worker shutdown now
reach the hash loop. Verification checks its caller before and after each
64 KiB read, including EOF, and before publishing trust. An incomplete or
cancelled hash does not supply a partial digest, quarantine good bytes,
publish a verified marker or create an object retry. Individual blocking
filesystem calls remain OS-bound; cancellation takes effect when the current
bounded call returns.

The existing `Store::read_object_range` seam remains. The checked variant
accepts a synchronous caller-owned predicate and checks it around bounded
range reads as well. The foreground provider permit checks request cancellation
while waiting for its existing reserved capacity. No cancellation hook or
helper thread survives a completed request. The reserved operation retains its
64 KiB frame limit and five-second maximum; ordinary legacy reads retain the
1 MiB reply limit and use their actual loopback request deadline. Direct worker
dispatch uses a 60-second fallback for ordinary reads.

The existing ordered `SessionWorkerClient::read_stream` seam remains. Stopping
its consumer now cancels its remaining requests. The cancellable variant also
polls its caller while the result window is empty; retained background push
and inbound pull pass their lane/session/daemon stop condition through it.
Only refusals that ran nothing are retried, within the original deadline and
with interruptible backoff. Cancelling one call sends its request CANCEL and
keeps the shared worker connection usable. The existing bounded read-window
threads remain; connection dialing itself and the full retained legacy
thread/byte budgets are not qualified by this change.

After potentially long first-serve verification, the reader rechecks the
committed-shared-reference/shareable triad before reading the frame. This
is a local authorization check, not an offline/global revocation promise.
Wire identities and legacy byte digests do not change, and no normal typed
content activation is enabled. Full legacy scheduler/byte/spool budgets,
close-release helper fanout, mounted demand and report/scrub cancellation
remain separate work.
