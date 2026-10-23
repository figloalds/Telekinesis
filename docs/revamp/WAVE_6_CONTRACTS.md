# Wave 6 contracts

## Personal-use authority scope (2026-10-22)

[Deployment assumptions and DVFS boundaries](AUTHORITY_SCOPE.md) govern the
current target contracts, including the transport boundary below. Users, ACLs
and untrusted-user service security are out of scope. Deployment supplies channel
and host protection; DVFS retains its own pairing/route/reference, generation,
ownership, integrity and filesystem checks. Earlier HMAC/credential-enrollment
contracts describe existing or historical mechanisms, not requirements to
reproduce them. Simplification targets the selected simple hashed pairing-code
model. Historical qualification is preserved; this amendment changes no runtime
or test result and introduces no security release gate.

The hashed pairing-code challenge is the application admission boundary for an
online server. Missing/incorrect codes grant no peer use; channel protection
does not replace pairing. Successfully paired peers are trusted. Remaining
reference, generation, ownership and integrity checks enforce DVFS correctness,
not a hostile authenticated-user model. Unpair/reconnect must preserve admission.

## Current scope amendment — CCR-WP6.G-1 (2026-10-21)

The user confirmed a greenfield release with no prior-version support. The
[WP6.G interlude](WAVE_6_PLAN.md#31-wp6g--greenfield-interlude-in-progress)
has started after CCR-WP6.4-30. Its target removes legacy representations, save/transport
fallbacks, promotion, prior store/journal upgrades, old-reader qualification,
mixed-version interoperability, previous-executable fences and downgrade
workflows from the target contract and release gates. These are scope removals,
not unfinished compatibility gates. Fresh stores use a single current
direct/manifest content path and selected current private-journal/protocol
versions. Current-format recovery and clear unsupported-version refusal remain
required, as do authorization, durability, ownership, retention and resource
bounds. Current small-file direct objects are not legacy whole-file support.

The sections below record implemented contracts and earlier CCRs. References
there to mandatory legacy/migration/compatibility work are superseded only to
the extent they support prior formats or versions. Their code descriptions and
evidence are not claims that WP6.G has already removed those paths. WP6.G must
replace the relevant normative codec/schema/API sections and golden fixtures
with the selected current-only definitions as implementation lands; do not
silently reuse an old encoding with a new meaning. Historical evidence remains
intact. Production activation remains off, and all current-path correctness,
platform, performance and failure gates remain required.

## CCR-WP6.G-14: current file-content representations

Only Direct (binary kind 1, representation 1) and Manifest (kind 2,
representation 1) are supported file descriptors. Kind 0, representation 0 and
JSON `legacy` descriptors are unsupported; their former encodings are never
reinterpreted. Direct/Manifest bytes, SHA-256 domains, bounds and canonical empty
identity retain their existing definitions below. An old binary vector is kept
only to test refusal.

The provider no longer reads/verifies whole-file legacy objects or converts them.
Live and retained private saves use current chunk rebuilding without conversion
caches or alternate converted providers. Save capture requires `content_ref`;
it cannot infer a base from a bare hash and object size. Full replacement may
omit old bytes/structure; unchanged subtrees may remain unhydrated. Required
changed chunks/structural pages still fail on missing or corrupt bytes.

This amendment supersedes positive legacy-descriptor/provider/conversion
contracts in the historical sections below. Bare strings still exist in other
metadata/projection/status/preview consumers and standalone/transport routes;
those removals remain open, so this does not claim descriptor-only ingress or
a complete single current path. G14 scoped evidence is
[current-content-codec-checks.json](../../test-evidence/wave-6/current-content-codec-checks.json).

## CCR-WP6.G-15: current Saved lifecycle accounting

Heartbeat `pending_saves` includes current private Saved revisions awaiting
publication, independent of buffered dirtiness and already adopted baselines.
The current journal initializes an atomic count from its existing pending index.
Checkpoint/adoption transactions compute the resulting count before COMMIT and
publish it only after success. Failed commits keep the earlier durable count;
new buffered edits cannot replace that count with zero. Heartbeat reads use no
Engine, private-journal or SQLite lock. Existing wire fields and journal/store
formats are unchanged. The old standalone table counter remains during removal
of that execution path; this is not a new save scheduler.

Fatal poison retention uses the shared off-lock helper after metadata rollback
or connection replacement. The unused standalone `persist_after_poison` API is
removed. Current native lifecycle, failed publication/retention, group/adoption
and actual mount checks are scoped in
[native-saved-lifecycle-checks.json](../../test-evidence/wave-6/native-saved-lifecycle-checks.json).
Other standalone save/staging and transport fallback removals remain open.

## CCR-WP6.G-16: one service at public mount attachment

Public WinFsp attachment and FUSE `attach`/`attach_managed` initialize/reuse the
current NativeWrites owner after path checks and before creating the OS mount or
mountpoint. This includes convenience/library calls without RPC startup; an
already active service is reused without a second executor. Unsupported private
journals refuse that attachment. The application C/Rust `tk_mount_start` ABI no
longer has an optional native-content argument; WinFsp capability/operation-guard
selection uses the current native path on every volume. This adds no user setting
and makes no prior-executable ABI compatibility promise.

Scoped direct/RPC actual-mount and controller evidence is
[mount-default-current-checks.json](../../test-evidence/wave-6/mount-default-current-checks.json).
The new refusal case covers fresh valid metadata, unchanged unsupported journal
bytes, absent sidecars and absent mountpoint. It does not establish refusal before
recovery of unrelated pending directory metadata. Other adapter/Engine/staging
fallbacks remain open in the inventory.

## CCR-WP6.G-17: current handle reads and chunk-local mutation

Regular-file staging requires a typed descriptor. Whole-file CAS references,
cached whole-file handles, writable-copy conversion and object snapshots are
removed from `Staged` and both adapters. Direct small-file objects still use
the current provider; directories/control data and the remaining standalone
memory/disk deferred-save route are separate and await consolidation.

Read progress means verified progress in the requested range, with independent
stall, total-deadline, cancellation and capacity outcomes. Feeding an unverified
whole-file spool is not a current file-read progress signal. Immutable handle
selections survive unrelated activation; a changed content head replaces the
selection. Generic object-ID preview/fetch remains a separately audited caller.

A native mutation marks its session dirty before off-lock preparation to fence
checkout. If no visible byte sequence changes, completion restores only that
operation's prior dirty marker, guarded by the same file and edit identity.
Zero-byte writes and refused corrupt-chunk writes/truncation must not invent
dirty bytes or change metadata. Earlier accepted edits remain dirty; partially
accepted writes that lose their reply retain their progress and clocks.

Scoped current-handle evidence and separate broader diagnostics are in
[current-handle-content-checks.json](../../test-evidence/wave-6/current-handle-content-checks.json).
This does not close the remaining standalone staging/table/transport inventory.

## CCR-WP6.G-18: descriptor-only Core metadata

Every accepted file-content register is a validated Direct/Manifest descriptor
JSON object. Strings, null, numbers, booleans and arrays are refused as
`INVALID_CONTENT`. This applies before the public nonlocal commit can expand
whole-file objects, before bundle byte installation/provenance, before inbox
staging and at feed precheck. An invalid content value cannot advance metadata
admission, peer knowledge or outbox acknowledgement. A batch rejected before
staging leaves no partially admitted event/root/source rows.

Projection setters/getters no longer interpret strings as file content. The
temporary `Entry.content` member encodes/decodes only null until its remaining
consumers are removed; a non-null string is refused even beside a valid
`content_ref`. Supported current projections keep their encoding and selected
metadata schema/header 3. There is no old-data conversion or new migration.
Regular-file metadata size comes from the validated descriptor's logical length,
without the old `object_sizes` fallback; directory/control/session handling is
separate. Missing local bytes/structure do not prevent this metadata lookup.

Bulk fixtures exercise public Core commits and durable root handoff. They no
longer fabricate old file-object catalogs or serialized projection/head rows.
Their direct data is installed outside the metadata batch, and creation
preconditions do not alter Core's ability to admit valid conflicting metadata.
Scoped evidence, including full feed and current-store checks, is recorded in
[descriptor-only-metadata-checks.json](../../test-evidence/wave-6/descriptor-only-metadata-checks.json).
Status/preview/demand, old standalone saves/tables and transport consolidation
remain separate open work.

## CCR-WP6.G-19: current content status and folder details

Content counts refer to distinct logical descriptors: active live file roots for
current counts, and granted shared historical roots for shared counts. Physical
chunks are not files. Canonical empty content needs no installed object. Reports
follow typed verified jobs and canonical indexed manifest adjacency, with cached
listings of data and manifest directories; no CAS file bytes are read or hashed
for status. Present but unverified/unexpanded content is `indexing`, absent
coverage is `missing`, and recorded corruption is `corrupt`. Missing bytes count
logical occurrences, including repeated chunks and the full range of an absent
manifest page. The bounded missing list carries entity/revision/reference,
descriptor, path, state and missing_bytes, never a manifest digest as a file hash.

A locally readable received root is shared-complete only when its pages/chunks
also have public receipts. Private dedup verification does not manufacture those
receipts. Local publishable roots retain their existing disclosure semantics.
The cache follows database changes, branch and both directory listings, and
never reuses a transactional computation. Status observes verified metadata and
presence; normal verified reads/scrubbers discover hidden same-length in-place
external corruption. It is not a synchronous integrity scrub.

The worker samples current repository transfer reservations and live range
waiters; unverified old spool growth is not progress. Local maintenance verifies
one bounded data job per tick when no structural work is pending, without a peer
or Engine lock. The actual folder-details RPC captures/rechecks view identity
around a secondary SQLite snapshot outside Engine, retains conflict review tokens
and pagination, and embeds the worker status claims. Uncached full summaries
still walk roots/reachable indexed structure; large-store/load/build gates are
not closed by this tranche. Scope and evidence:
[current-status-checks.json](../../test-evidence/wave-6/current-status-checks.json).

## CCR-WP6.G-20: reference preview and explicit range demand

The worker/Folder control surface uses `preview-content`, `request-content` and
`cancel-content-request`. `reference` is the registered event/entity reference ID,
not a descriptor's digest or a file-byte SHA. GUI conflict alternatives derive it
from their revision/entity. A granted reference resolves its immutable descriptor;
withdrawn/unknown references refuse. The old `preview-object`/`request-object`
file routes have no fallback or translation. Generic local `cat-object` remains
a distinct artifact/debug consumer pending the rest of its catalog/transport audit.

Preview reads/verifies at most the first 65,536 bytes and returns reference,
descriptor, logical size, truncated flag and text/binary indication. It does not
require unrelated chunks. Locally present private data can be read; private
missing data does not enter peer demand. A shared missing/corrupt range waits
through current typed demand. Provider I/O and waits hold neither Engine nor a
metadata transaction. Pins, selected view and admission generations bound the
operation; changed view/grant is checked again before delivery. JSON control
keeps the existing 15s bound; session calls honor their shorter absolute deadline
and cancellation while preserving large JSON reply body framing and the session.

Explicit request takes reference, optional offset (default 0) and len (default
the remaining prefix up to 1 MiB). The range must fit the descriptor and the
1 MiB bound. Available data returns queued=false/available=true. Otherwise the
worker owns a foreground range demand and pin and returns queued=true plus its
cancellation token. Repeating the same reference/admission/view/range joins that
owner; retries do not extend its fixed 24h lifetime. Cancel drops only that UUID.
Completion, expiry, withdrawal, checkout and worker stop also release it. These
are volatile priorities in the worker incarnation, not durable file-save or
availability acknowledgements. Durable admitted roots/background replication
remain the restart owner; no old object_requests/WANT/spool row is written.

Calls reserve read capacity before allocating at most 1 MiB. At most two control
byte calls run per worker, leaving loopback handlers for metadata and content
completion. Sixteen process-wide explicit prefetches have a separate budget and
release their read-call slots after acknowledgement. No per-request thread or
new transport security/protocol negotiation is introduced. Scope and evidence:
[reference-consumer-checks.json](../../test-evidence/wave-6/reference-consumer-checks.json).

## CCR-WP6.G-23: validate private format before directory recovery

Current service installation must validate/open the selected private journal
before consuming pending directory dispositions or committing their namespace
changes. Unsupported private input leaves those intents and canonical metadata
untouched through both RPC startup and direct attachment. Recovery still completes
before requests are accepted on a current store. This ordering uses the existing
journal open; there is no prior-format reader or upgrade path.


## CCR-WP6.G-22: canonical creation only

Native creation commits typed empty content and its name before acknowledging
the handle. It has no optional deferred-create mode, parallel pending-name map,
first-save creation commit or late uncommitted namespace restoration. Lookup and
enumeration use the canonical Store view; COW supplies dirty byte length. A new
unflushed child therefore prevents directory removal without a second name index.
Current creation/namespace receipts and private recovery preserve ownership and
replay. Existing standalone stage recovery cannot introduce a missing file.

Namespace operations own source/destination session handles even when a COW
session already exists. Capture and retention share the short Engine lock;
the final user close cannot evict them while entity gates are awaited. These
readonly operation handles fence quiet/checkout and release on every outcome,
including a request deadline. They do not authorize another writer's byte save.

The local scenarios use current provider/range byte oracles. Explicit mounted
invocation requires an actual WinFsp/FUSE mount. The 300-file copy and eight
writers x150 files retain their original bounds; ordinary close/fsync uses the
current executor/durability contract, replacing old spool scheduling assumptions.
The report's 26-function retirement ledger identifies each current coverage owner.
Remaining standalone staging/snapshot/pending-saves behavior is not removed or
qualified by this creation tranche. Scope and evidence:
[deferred-creation-checks.json](../../test-evidence/wave-6/deferred-creation-checks.json).


## CCR-WP6.G-21: mounted mutation dispatch

Attachment installs the current native write service before any callback.
Ordinary mounted create/write/truncate/basic metadata/rename/delete must use
current byte/namespace owners; an unavailable current helper cannot select a
standalone memory/disk/deferred Engine implementation. Current shared savers
remain responsible for flush/close and retained readonly writer semantics.
Readonly descriptors have no byte save. Directory metadata/release/fsync and
virtual root/discovery behavior are explicit: root permits only its no-op 0755
mode, discovery reads its buffer, and neither accepts byte mutations. FUSE
validates fsync handles and releases failed truncating opens. Closed/readonly
immutable rename sources acquire a temporary current metadata owner without
hydrating base bytes and then use the existing namespace executor/receipt.
This does not remove
the remaining standalone APIs/table or claim full-path/release qualification.
Scope and evidence: [current-adapter-checks.json](../../test-evidence/wave-6/current-adapter-checks.json).


## CCR-WP6.G-2: selected current private-journal layout

The only supported existing private journal has SQLite application ID
`0x544b5753` and `user_version=7`. Its existing v7 schema, sealed Working/Saved
records, overlay maps, exact native clock pairs and causal/ownership identities
keep their meaning. There is no v4/v5/v6 upgrade chain or matching-reader/export
path. Descriptor/provider conversion was subsequently removed by G14 above;
other runtime fallbacks remain separate items in the
[removal/replacement inventory](notes/wp6-greenfield-inventory.md).

An absent or genuinely empty database initializes directly in this layout.
Schema, inventory and header commit atomically in a FULL SQLite transaction;
an interrupted initialization leaves an empty or complete current database.
The locator `write-sessions-v1.sqlite` does not select the accepted layout.

A standalone input without a nonempty WAL first passes a fixed 100-byte SQLite
header eligibility check through ordinary read-only file access. This refuses
unsupported input before SQLite itself can create empty WAL/SHM sidecars. A
nonempty WAL remains authoritative through read-only SQLite inspection; the
main header may be stale or torn during checkpoint recovery. This eligibility
check never substitutes for the transactional snapshot. The writer handle is
checked again before any journal-mode change. Any other header,
or any unmarked database containing schema objects or a nonzero schema cookie,
returns `UNSUPPORTED_WRITE_JOURNAL`, classified `Incompatible`, with an actionable
instruction to use a fresh store directory. Refusal must not rewrite the database,
WAL or saved records or silently initialize/upgrade unsupported state. The
bounded read-only snapshot pool enforces the same current-layout check.

Prior-reader and prior-layout upgrade tests are removed as obsolete. Current
restart preserves exact Saved record bytes and their checksums,
operation/checkpoint identity and native/headless clocks. Resuming deliberately
rotates the Working owner and advances its sequence to fence older handles;
the mutable row seal changes with that ownership. A 104-case header/mode/clock
matrix covers refusal
without persistent-byte or directory-entry changes, plus malformed, previously
used unmarked and empty non-ASCII locator cases. Three actual exit-86 children
cover fresh
initialization atomicity. All other current save/publication/adoption/recovery
regressions remain required. Historical compatibility evidence remains intact.

## CCR-WP6.G-3: selected current Core metadata layout

`metadata.sqlite` accepts application ID `0x544b4d53`, `user_version=3`, or a
genuinely empty input for initialization. `core/schema.sql` is the single fresh
layout: 63 tables, 34 triggers and 17 explicit indexes. The independent fixture
`tests/fixtures/store_v3/schema.json` retains all preceding definitions and adds
the explicitly specified directory-disposition table/index. G5 added file
dispositions; G7 adds directory dispositions with scoped current-path qualification.
G3 used layout 1, and G5/G6 used layout 2; those results are historical evidence.
There are no prior-layout readers or upgrades. Fresh creation
commits that schema, selected branch, repository/device identities, generation,
journal epoch and header together in one FULL transaction. A concurrent writer
adopts the winner's committed identities; bounded `SQLITE_BUSY` remains permitted.
No historical module stamps, ALTER chain or prior-format spool adoption runs.

The shared standalone-header guard has the same WAL authority policy as the
private journal above. Unsupported metadata returns `UNSUPPORTED_STORE_FORMAT`
(`Incompatible`) before a writable open or creation of an objects directory.
Current required identities and authoritative tables are validated together in
one read snapshot (or the caller's existing writer transaction), never
synthesized to hide damage. Secondary handles validate format, required schema
and identity; they neither initialize nor repair. Existing extra nonconflicting
schema objects are permitted, including the fault-injection triggers used by
current durability tests.

Missing performance indexes can be recreated. Only declared disposable object
reference, feed-journal and journal-branch structures/triggers may be rebuilt.
Their recreation and invalidation of the affected repair marker/cursor share
one transaction. Bounded current-data scans persist `repair:objects`,
`repair:feed` or `repair:journal` cursors and mark ready only when complete.
Restart never clears accepted journal sequence, epoch, peer acknowledgements,
receipts or authorization to manufacture an apparent clean store. Existing
save/adoption and snapshot recovery retain their current-format semantics.

This selects the Core layout only. Descriptor producers,
default native execution and transport consolidation remain in WP6.G.

## CCR-WP6.G-4: selected current catalog layout

`folders.sqlite` selects application ID `0x544b4353`, version 1. Its seven-table
layout is initialized directly from `orchestrator/catalog/schema.sql`; schema,
installation, initial lease counter, initial verifier generation and header share
one FULL transaction. The next durable lease incarnation is committed before any
actor snapshot/worker ownership is exposed. Neither lease nor verifier generation
may wrap or saturate into reuse.

Validate unsupported input before creating a lock file or mount directory,
changing permissions or opening a writer, using the shared standalone-header/WAL
authority policy. Revalidate under the directory lock. Current required tables,
installation/counters, verifier consistency and folder intents are authoritative;
missing state is refused rather than synthesized. Current validation queries share
one snapshot. Extra nonconflicting schema objects remain permitted for current
fault-injection/diagnostic use.

`mount_gates` is the only durable gate source. No `initial_sync` mirror,
`catalog_schema` migration, older-writer reconciliation or
`credentials_generation` ledger remains. A gate can legitimately outlive removal
of its mount path while awaiting satisfaction/override and controller cleanup.
Preserve that state across restart. A verifier's salt remains a current identity
consistency check. `SetVerifier` commits the new verifier, checked generation and
inbound credential revocation together. Current unpair/re-pair credential cleanup,
bounded busy retries, actor panic rollback, unknown command outcomes and directory
lock ownership retain their meaning. Invalid individual credential rows retain
their existing fail-closed behavior; they are not a request to reset credentials.

Current-format qualification and its staged test-only fixture rebase are recorded
in the [implementation record](notes/wp6-progress.md#current-catalog-ccr-wp6g-4).
This selects the catalog layout; descriptor, default execution and transport
consolidation remain open.

## CCR-WP6.G-5: native unlink ownership and durable dispositions

Removing a native file's name is a namespace-only operation. A FULL private
checkpoint precedes the tombstone and an exact retained-writer continuation;
the tombstone, continuation and sealed `unlink:v1` receipt commit in the same
metadata transaction. The request binds the operation, checkpoint, mutation ID,
entity, observed path and parent identity. A lost reply is resolved by read-only
exact receipt lookup, never by removing whatever later occupies that path.

Open descriptors keep the same private bytes and may continue reading, writing
and flushing the unnamed entity. Subsequent saves retain `alive=false`; a new
file using the old name has a distinct entity and is unaffected by old handles,
cleanup or receipt replay. Namespace retirement alone does not publish bytes or
authorize private-map pruning. Current reference retention and permission rules
remain in force. Unsupported/foreign ownership cannot grant this continuation.

WinFsp `CanDelete` validates without removing anything. `Cleanup` with
`FspCleanupDelete` performs the removal, including POSIX deletion while other
handles remain open and creation with delete-on-close. The SDK's Cleanup is void:
errors still retain/report failed work through the existing native save/adapter
health path; it cannot provide a fallible Windows close reply. FUSE unlink and
both adapters' checkpoint/commit work occur outside the Engine lock under the
entity, adapter-mutation and bounded namespace ownership gates. Existing rename,
replacement and retired-destination behavior keeps its contracts.

The initial candidate's Saved-byte checkpoint is insufficient for Windows delete
recovery: injected failure after disposition acceptance left the name visible.
A pending removal must have durable ownership, observable cancellation and exact
restart handling, rather than relying on the failed namespace transaction to
store the only request. Bind the entity and handle/request identity through
name reuse and rename; a canceled disposition must not delete on restart. Cover
new-file delete-on-close as well as deletion of an existing file. This obligation
was absent from the preserved initial candidate and is implemented by the
correction below.

The correction records each accepted file disposition in
`content_delete_intents`, bounded to 64 records with 16-record read pages. Sealed
records bind a fresh per-handle request UUID, repository/workspace, branch/view,
entity and stable byte-operation UUID. `SetDelete` records or cancels that request
in a FULL transaction before the fallible response; delete-on-close creation/open
records it before acknowledging the open. Marking the file does not remove its
name or force byte publication. Cancellation affects only the originating
handle's exact request; its lost reply is resolved by read-only lookup.

Unlink/replacement consumes the entity's matching dispositions in the same
transaction as the tombstone and retained-writer successor. A failed namespace
transaction leaves the separately durable request intact. A durable cleanup may
replay that exact owned request; ordinary fallible unlink still never retries an
unrecorded failed mutation. Pending requests fence checkout. At restart, validate
workspace/view and writer ownership before journal-owner recovery; normalize
committed byte continuations, preserve Saved bytes and resolve the current name
by entity. Consume requests before normal Saved publication or mounting. A clean
Working-only capture needs no byte publication and is left intact. Corrupt or
superseded authority is an explicit error, never an invented request or reset.
An existing request alone cannot resolve a failed staging reply: recheck current
writer, live file, readonly policy and unpublished-byte eligibility in one read
snapshot. Core repeats validation after insertion before commit. Proven byte
publication is inspected only at its exact next, unowned generation; a later
writer is refused before private-journal recovery.

The scoped qualification is
[native-unlink-durable-checks.json](../../test-evidence/wave-6/native-unlink-durable-checks.json):
418 frozen inputs, strict release Clippy on both platforms, 512 Windows/520 Linux
ordinary cases across 12 targets, and seven actual kernel parents per platform.
All 13 harness/worker-CLI images per platform and source hashes stayed unchanged.
The original failed candidate and subsequent development defects retain their
exact source/log/image capsules; diagnostic repeats and exit children are not
additional unique passing cases.

This supplies required native file removal before default-path consolidation.
It does not make typed writes the default, remove staging/old transport, implement
all directory operations, or close the remaining Wave 6 gates.

## CCR-WP6.G-6: native directory metadata (in progress)

The first replacement moves directory creation out of Engine's semantic lock.
`DirectoryCreateRequest` binds separate mutation/entity UUIDs, repository/workspace,
branch/view, observed parent/path and initial basic metadata. A bounded namespace
reservation and entity key precede one FULL pooled Core transaction. It commits
only kind, location, alive and basic metadata, together with a sealed
`directory-create:v1` exact receipt in `content_save_create_changes`. The table is
creation authority shared with the separately sealed file-create records; it is
not a directory byte owner. SQL layout remains version 2 and the private journal
remains v7. No prior-layout reader or migration is added.
Adapter/name/entity admission precedes opening the pooled connection and any
workspace-identity initialization, so admission also bounds that preparation.

Historical receipt replay cannot recreate a moved/deleted directory or overwrite
a later occupant. Final transaction checks and pre-ACK validation bind the exact
event, live initial entry, parent, workspace/view and metadata revisions. A failed
open may leave an already committed directory, but attaches no invented handle
and never silently retries an absent receipt. Initial POSIX mode and Windows
attributes belong to creation; no follow-up mode setter fences a file owner.

WinFsp directory Create and threaded FUSE MKDIR consume this coordinator when
native execution is selected. A directory handle holds metadata and the existing
enumeration cursor; it has no file content, private save, checkpoint or content
publication. The installed SDK's FINE guard still holds one volume-wide exclusive
lock across Create, deleting Cleanup and rename. Explicit native mounts now use
the supported null Enter/Leave operation guards, relying on Rust's bounded
namespace/entity ownership and transaction revalidation; kernel per-file
serialization remains. The other unconverted execution retains FINE until its
replacement. The scoped native namespace qualification below covers this change.

The directory kernel parent separates Engine acquisition and existing-handle
metadata progress from a same-parent named lookup. The initial named-lookup
300 ms assertion failed under the injected 500 ms mkdir delay on both platforms.
Linux sampling locates that caller in kernel `walk_component` with an idle FUSE
dispatcher; a separate handle control passes. Windows handle metadata also waits
under the SDK FINE guard, motivating the native guard change above. Preserve
those exact failures and record named-lookup overlap separately. The 50 ms Engine
and 300 ms existing-handle assertions remain; original load/cache/build gates are
unchanged and are not qualified by this parent.
A separate kernel parent must preserve entity and byte identity when a committed
rename overlaps a new-name open and old-name recreation under the native guard.

Directory rename/delete/basic metadata outside Engine, durable
directory dispositions and complete namespace/lifetime/remote ordering remain
required. Creation is qualified in the isolated native path and is not the default.

Final scoped proof is
[native-directory-create-checks.json](../../test-evidence/wave-6/native-directory-create-checks.json):
422 frozen inputs, strict release Clippy on both platforms, 522 Windows/530 Linux
ordinary cases across 12 targets/platform and nine actual kernel parents/platform.
All 13 harness/worker-CLI images per platform stay unchanged through proof. The
separate current-image Windows time-policy parent still fails with exit 101; its
actual failed harness/CLI/source are archived. No timestamp, named-lookup/cache,
load, directory-lifetime or other original release gate is waived.

## CCR-WP6.G-7: directory mutations and dispositions (scoped qualification)

Directories use exact metadata snapshots, never a file `SaveIntent` or byte
journal owner. Each capture binds workspace/repository, branch/view generation,
entity, kind/location/alive/basic selected heads and the complete observed entry.
The FULL writer checks that capture, source path/parent when supplied, target
parent/occupant, empty replacement/removal and bounded ancestor traversal before
committing. Partial basic updates retain unspecified fresh fields. Sealed
`directory-change:v1` receipts bind the exact command, immutable event and result;
historical replay returns that receipt and never replays a later name.

Fresh metadata layout 3 adds `directory_delete_intents` and an entity/request
index. A `directory-delete:v1` record binds one handle's request to its exact
live empty-directory snapshot. File and directory requests share the 64-request
bound; public recovery pages contain at most 16 records and each record is
bounded to 64 KiB. Owned basic/rename commands atomically advance every pending
request; remove or empty-directory replacement consumes them. Cancellation
removes only the exact handle's request. Pending deletion prevents native child
creation or moving a child into that directory, checked inside the Core writer.
Checkout refuses either kind of pending disposition.

Directory recovery runs before opening the private byte journal or publishing
Saved file checkpoints. It validates the exact snapshot and emptiness and uses
the same durable removal command. Foreign changes cause refusal, not adoption,
reset or deletion of a reused name. Adapter directory handles retain independent
existence heads; owned commands can advance handles of that incarnation, while
restoring the entity does not grant an older handle authority over the new one.
Enumeration retains an existing cut; rewinding an obsolete incarnation refuses.

The implementation routes native Windows basic/rename/disposition/Cleanup and FUSE
basic/rename/rmdir through bounded metadata ownership and pooled connections
outside Engine. The 429-input current source passes strict release Clippy,
539 Windows/548 Linux ordinary cases across 12 targets/platform and eleven
actual kernel parents/platform, retaining all 13 images/platform through proof.
The independent schema fixture and current restart/refusal tests select layout 3.
The proof is
[native-directory-metadata-checks.json](../../test-evidence/wave-6/native-directory-metadata-checks.json).
It covers the specified local handle/removal/replacement/restart cases, not all
deadline/lost-observation or remote activation lifetimes. Default execution,
transport simplification and all remaining Wave 6 gates stay open.

CCR-WP6.G-9 adds scoped reconciliation of retained directory mutations/Cleanup
after a post-commit observation deadline. It reads the existing exact receipt
for the current removal head, checks its prior incarnation and scope, and reuses
normal observation before continuing. Restorations remain fenced. Staged focused
coverage passes 184 ordinary cases per platform; read-only enumeration and full
remote lifetimes remain open. See
[directory-observation-checks.json](../../test-evidence/wave-6/directory-observation-checks.json).

CCR-WP6.G-10 also reconciles retained enumeration through the same observation
method. It consults the existing receipt only on an existence-head mismatch and
rechecks the current head before building the directory cursor. This performs
bounded metadata reads/handle updates, without another durability commit or byte
work. Both platforms pass 78 ordinary native-write cases and two actual directory
kernel parents, with the unchanged standalone negative now passing. Full remote
and concurrent-parent lifetimes remain open. Evidence:
[directory-enumeration-checks.json](../../test-evidence/wave-6/directory-enumeration-checks.json).

CCR-WP6.G-11 is implementing default native service initialization for current
workers. Startup recovers/installs the service before accepting RPCs, without a
fixture marker. Stop cancels its admissions after retention and process teardown
uses the existing bounded executor drain outside Engine. The candidate remains
failed/unqualified: new ordinary startup/recovery cases pass on both platforms
and its FUSE mount passes, while fatal native retention, legacy-pending startup,
duplicate fixture installation and retired gate expectations fail. Relevant
recovery/integrity/routing tests stay required. Core producers, standalone legacy
execution and transport remain pending, so this is not a claim of a complete
single current path or release. Evidence:
[native-default-candidate-checks.json](../../test-evidence/wave-6/native-default-candidate-checks.json).

## Transport security boundary (2026-10-22)

The user selected standard TLS/WSS or the enclosing VPN as the channel-security
boundary. Transport consolidation must reuse that protection without a custom
cryptographic channel, extra security negotiation or another transport-security
layer. This supersedes any earlier proposal to expand channel-security scope.

CCR-WP6.G-8 implements that simplification. Its scoped source removes
the HMAC transcript, mutual MAC proofs, authentication nonces, scheme negotiation
and custom enrollment wrapping. `AUTH` carries an ordinary application credential
and whether it is a stored peer credential; `AUTH_OK` is an empty acknowledgement.
Enrollment sends its secret on the selected protected channel. Existing catalog
credentials, repository permissions, revocation, connection bounds and standard
WSS certificate checks remain. No old authentication-format reader or optional
HMAC fallback is retained. Scoped pairing/session/lifecycle coverage passes on
Windows and Linux, including actual WSS and two peer/kernel parents/platform;
the broader Windows receipt/readiness diagnostic failed and remains unwaived.
Exact staged provenance and the preserved failure are in
[basic-transport-auth-checks.json](../../test-evidence/wave-6/basic-transport-auth-checks.json).

## Original freeze and implementation amendments

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

## CCR-WP6.3-9: bounded offer cleanup and session retirement

Closed/silent inbound offers share two process-wide release workers. Closing a
bulk session marks its existing bounded offer-table entries and wakes those
workers; it starts no thread and copies no queued release batch. Pending work
remains within the 4,096-offer table. Oldest eligible offers are selected first.
At most two client release attempts hold extracted hash batches, including
explicit synchronous sweeps; each normal offer has at most 256 hashes. The
guards remain held across RPC and release on return or unwind. Attempts are
best effort under the existing two-second per-call deadline and exact qualified
lease ID. A timeout is not proof that its remote handler has already ended;
the worker's own bounded handler pool and generation checks still apply.

The fixed workers wake on close/completion and poll at most 500 ms apart when
idle. Panics in a release attempt are contained and the worker continues.
Startup failure refuses retention when no cleanup worker exists and retries
missing workers no more than once per second on subsequent offers. These
workers have process lifetime; they are not created per daemon fixture, peer,
repository, offer or session. This bound does not qualify all legacy job, byte,
spool, connection or shutdown budgets.

An authenticated push session now has a permanent closed state. Its registration
guard retires it before unregistering, including unwind. Close and guarded
publication/status mutation use the same short pool gate. A late want cannot
publish after close, even when close saw no offer yet. Retiring one session does
not retire its siblings or allocate a new pool identity.

Close stops new authority and backoff publication; it does not by itself
supersede the admitted data generation. An active push may drain its already
admitted contiguous prefix while its original pool/session/frame lineage is
still current. An actual newer owner still fences that drain in the worker.
After draining, cleanup releases only that push's exact lease without a source
failure. Reconnect resumes at the durable offset. Ordinary typed activation,
wire shapes and legacy whole-file identities remain unchanged.

## CCR-WP6.3-10: ownership through legacy error projection

This extension owns `runtime::objects_effects`, `ObjectsState::effects`, the
legacy worker RPC hooks and a private Store projection helper. The public
`ObjectLease` record and existing Store failure signature stay unchanged.

Each granted object lease now owns a distinct in-process work ticket. Writers
retain it through verdict merging and final source/local error projection,
even after the ordinary lease is released. A replacement grant changes the
ticket, including reuse of the same opaque session string or another foreground
frame in the same parent run. A finished/dropped replacement never resurrects
its predecessor. Explicit release/exclusion/expiry retires the matching ticket.
The existing pool/run/offer floors still apply independently.

Weak registries are bounded to 65,536 object slots and 16,384 source-family
gates. Existing live keys are reusable at capacity; only dead entries may be
reclaimed. Work tickets and active leases retain their slots/gates. Exhaustion
returns contention/resource readiness rather than evicting an old live owner.
There is no new schema or wire token in this extension.

Source retry mutation checks the exact object ticket and run under the worker
state guard. Durable local retry projection takes the source-family gate and
object gate, then rechecks ownership and releases the worker state guard before
SQL. Advancing that family's run/pool or claiming that object serializes with
the projection. Gate acquisition never waits: contention is `WORKER_BUSY`, not
a `BUSY` promise that no handler ran. Registry capacity is
`CONTENT_RESOURCE_LIMIT`. Hashing, copying, spool I/O and object verification
remain outside these gates. The SQL connection has zero busy timeout; actual
filesystem/fsync calls remain OS-bound. Unrelated metadata reads and worker
state remain available while a prepared local error transaction is stalled.

A verified available row wins over a collected local write failure. The
availability test and retry insertion share one savepoint, so concurrent
registration cannot be overwritten through a stale SQLite snapshot upgrade.
The direct Store failure API is unchanged; this guard applies to the worker's
owned write-error projection. Failure projection remains best effort and does
not change the write's original result or weaken byte verification.

This does not add an exact per-round token to historical `a`/`b` or opaque
release headers. Ambiguous reuse after an uncertain timed-out release, complete
legacy resource budgets, all error/status writers and final long/platform
qualification remain in the legacy audit. Ordinary typed activation stays off.

## CCR-WP6.3-11: bounded legacy ownership and download round identity

The legacy lease and source-holder tables retain at most 65,536 hashes each.
An existing-hash regrant updates that hash's provenance without clearing other
owners. A new hash at live capacity receives `CONTENT_RESOURCE_LIMIT`. Holder
pressure can remove only entries with no matching lease/session. A bounded grant
batch makes at most one pressure scan, collecting at most 256 already-fenced
keys. Those predecessors may be retired without source backoff. Current and
opaque live owners remain retained. An in-flight writer continues retaining its
effect slot after its ordinary lease ends; a new key can still be refused until
that writer drops its proof.
Write admission preserves its actual contention/resource error per refused
item. It performs no spool write or source/local error projection for that
refusal; an object held by another session retains `InFlightElsewhere`.

A local, read-only `objects.content-v1.legacy-capabilities` request accepts
`WorkerCaps { route, version: 1 }` and returns
`LegacyCapabilities { version: 1, round_fences: true }`. Route and version are
checked without opening a store or activating typed content. The daemon caches
the answer by immutable worker instance, up to 16,384 instances per lane run.
Only `UNSUPPORTED_OPERATION` selects historical behavior; transient errors are
retried through the ordinary scheduler. A version mismatch is
`PEER_NEEDS_UPGRADE`.

For a supporting worker, each download round allocates one fresh monotonic
nonce under its existing daemon incarnation/lane run/pool. Its two IDs are
`pull:<peer>:<i>/<n>:a<nonce16>~w6.<inc16>.<run16>.<pool16>` and the corresponding
`b` ID. All wants/writes within that round retain those exact IDs. The worker
keeps independent a/b floors beneath the parent lane fence, including after
release. There are at most 16,384 scope entries, each with two optional floors;
exhaustion refuses a new scope before changing parent/pool authority. Delayed
wants, writes, release holds and error projection from an older round cannot
resurrect it or mutate its replacement. Advancing a leaves b current. Background
rounds cannot promote foreground c work; foreground c may promote either
background slot regardless of their independent counter values.

Kept leases and all settlement/exit cleanup retain the exact session string
they acquired. A release whose client timed out therefore cannot release a
new round's lease or add a hold/backoff to it. A successor resumes from the
committed offset; round identity does not change whole-file hashes, spool
format or authenticated peer messages. The 128-byte session bound applies.

Unsupported workers keep the previous two stable, run-qualified IDs so short
rounds still resume without waiting for lease expiry. Ambiguous delayed cleanup
after reusing those historical or arbitrary opaque IDs remains outside the
round-fence guarantee. Real old-worker interoperation, complete legacy resource
budgets, other error/status writers and final load/platform gates remain in
the audit. Ordinary typed activation stays off.

The kept-set bound does not cap sequential release RPCs per exit. Distinct
round IDs can increase that fanout; pruning provably retired IDs or bounded
asynchronous cleanup remains a WP6.3 resource/fairness qualification.

Transport qualification distinguishes successful RPC callbacks from durable
ingress recovered after a post-CAS SQLite refusal. The in-process RPC fixture
drives the same bounded receipt recovery used by worker maintenance. Automatic
bootstrap requires verified full-file bytes, complete range indexing, verified
public receipt evidence for every job and actual structural/background network
admissions within its original five-second deadline. A forced native WAL-writer
conflict after installation verifies this path; production counters and receipt
semantics are unchanged.

## CCR-WP6.3-12: confirmed round cleanup and SQLite readiness

The per-worker capability cache now retains two optional round confirmations,
within its existing 16,384 immutable-worker-instance bound. A successfully
decoded `objects.want` reply confirms that worker's retained floor, including
an empty reply. Allocating an ID, an in-flight call, refusal, timeout or malformed
reply confirms nothing. Confirmation cannot regress on a delayed older reply.

Before carrying retained leases into another round or releasing them at lane
exit, discard only IDs strictly below a confirmed floor of the same slot,
peer/lane, incarnation, parent run and pool layout at that worker instance.
Current/future rounds, another slot/lineage, foreground c and opaque IDs are
preserved. Keep the confirmation snapshot outside I/O and release each surviving
lease using its exact acquired ID. Worker/remote-replica replacement cleanup
uses the captured previous endpoint and route; a stable ID actually reacquired
on the same worker settles with its current round. Daemon stopping preserves
the adopted-worker behavior.

For producer-created retained leases, old round history therefore adds no
release sessions: at most a/b remain for that worker. Current hash lists still
use the existing 256-item release pages and 4,096-key kept bound. Two sessions
can require up to 17 page attempts; this change does not establish asynchronous
cleanup, a global shutdown-time bound, or complete legacy resource budgets.

The typed object's dispatcher treats `SQLITE_BUSY` from its zero-wait slices as
contextual contention: increment readiness retries and retain its existing
25 ms delay, without incrementing failure statistics. This includes a receipt
index refusal after verified CAS installation. Other codes retain their classes;
the frozen global/wire `SQLITE_BUSY` classification is unchanged. Durable receipt
recovery and verified public authorization remain mandatory before claiming
logical ingress completion.

## CCR-WP6.3-13: foreground preparation and source-error ownership

The loopback `LEGACY_PREPARE` handler observes its incoming request deadline,
CANCEL/disconnect and worker shutdown before selection, before lease admission
and after grant. If the last check fails, give up only the hashes granted to
that exact session before returning cancellation/deadline. Checks do not rewind
parent/round authority. The stopped-request diagnostic runs outside worker and
family guards. Direct JSON dispatch uses its bounded default deadline; ordinary
legacy `objects.want` does not gain session-context cancellation here.

Each incoming foreground Task captures a proof bound to its object root and
source peer. Concurrent sources share the epoch before the next verified local
install; cancellation retires only its own attempt. Checking that proof and
inserting a daemon source backoff share the existing short table lock. A daemon
verified-install notification retires every older incoming proof for the same
installation/repository/direction/hash while clearing source history. Retiring
and later dropping a replacement proof cannot resurrect an older proof.
The epoch table retains at most 16,384 weak keys, reclaims only dead entries and
refuses new roots at live capacity with `CONTENT_RESOURCE_LIMIT`. Cancelled
in-flight Tasks keep their proof until they physically drop. Hashing, spool,
transport and cleanup RPCs stay outside the table lock.

The Task rechecks ownership before writes, after peer completion, and before
source-error/success projection. Its binding and daemon cancellation tokens
retire its proof. Cleanup retires the attempt before its exact-session release;
a hold is selected only while its proof and cancellation checks are current.
The existing scheduler reservation remains retained through that release
attempt. This does not establish remote-handler retirement acknowledgement or
remove every cancellation/release race.

A `Progressed` local receipt is a private prefix, never a whole-file install.
Only local worker `Completed`/`Skipped` permits foreground installation
notification after the complete requested stream and peer receipt validate.
An old cancelled verdict cannot recreate daemon source history after another
source has published its verified install.

This proof is currently used by the foreground adapter. Background/upload
error and status writers, independent local-install notification, group/source
replacement cancellation, permanent c-frame retirement/replay, reply-lost and
release-`WORKER_BUSY` cases remain in the audit. Complete physical legacy
budgets and ordinary WANT late-grant handling also remain pending. Native
boundary evidence and the final source identity are retained in
`test-evidence/wave-6/foreground-owner-checks.json`; ordinary typed activation
stays off.

## CCR-WP6.3-14: dispatcher group lifetime and deferred ownership

A dispatcher group owns a cancellation child of the dispatcher. Each legacy
or typed Task uses a linked child of that group and its lane binding: either
parent cancels the Task without cancelling the other parent or closing a shared
connection. Linked children use weak parent links, not accumulating per-request
hooks. An already-cancelled parent or a concurrent parent cancellation cannot
leave its newly linked child live.

One nonblocking refresh builder publishes at a time. At the existing 100 ms
refresh interval, validate tracked groups against the complete local folder
snapshot and ready peer/source/replica/lane membership. Local device or full
worker endpoint changes also invalidate the group. Cancel an invalid group's
Tasks before publishing its replacement. Reuse an unchanged Group, including
its readiness and per-class busy state.

The 512-repository scheduling page is not an authority boundary. A weak registry
tracks up to 1,024 current groups, including owners retained by preparing,
queued or running work outside the selected page. Validate those deferred
owners too. Discard only dead or cancelled registry entries; never evict a live
valid owner to admit another. At capacity, defer new groups. Cancelled physical
Tasks still retain their own resources in the fixed pools and bounded queue
until they unwind. Selecting a group drops the page snapshot before any I/O.

Admission, completion and error projections share a short per-group mutex.
Group cancellation returning is a barrier for those projections: an obsolete
group cannot subsequently increment counters or replace its error state.
This mutex covers no network/worker call, Task destruction or cleanup I/O.
Queue consumers and enqueue also reject cancelled groups. A polled result from
a withdrawn group cannot create fresh work. Existing contextual readiness
classification is unchanged; per-class error-episode reconciliation remains
separate work.

A typed Task retains its scheduler reservation through the local exact-offer
release attempt; the offer retains its outgoing turn until release returns.
Cancellation detected after a read returns suppresses its failure projection;
a returned parseable frame still gets its exact ACK. The client release attempt
does not acknowledge remote physical-handler retirement. Already accepted
worker calls retain their existing token/receipt ownership protocol.

Native catalog-withdrawal tests preserve the original connection, retire old
legacy/typed work and verify restored-catalog progress under their retained
250 ms withdrawal/drain and 1,000 ms replacement bounds. Registry page/capacity
and cancellation interleavings are production-helper unit checks; they do not
qualify 1,024 native repositories or full saturation/fairness. Evidence is in
`test-evidence/wave-6/group-lifetime-checks.json`. Complete retained legacy
physical budgets, remaining background/upload/error writers and final Wave 6
gates remain pending; ordinary typed activation stays off.

## CCR-WP6.3-15: shared legacy download stream reservations

Unjoined batch/stream reservations share the content scheduler's existing
global, peer, repository and priority-class capacity checks with joined object
jobs. They use their own random owner/generation, without inventing a single
object identity for a batch. The aggregate limit stays eight jobs/8 MiB, four
jobs per peer/repository, preserving one slot for each absent other class.
`usage` includes both kinds; `joined_usage` and `stream_usage` distinguish them.

Each retained legacy download pull now requests at most 256 KiB and reserves
1 MiB in this pool before removing any planned items or starting its reader/
writers. The historical lane window remains an additional outer bound, rather
than a new independent allocation of global capacity. On admission pressure,
leave the lane window/peer slot untouched, preserve plan/lease bookkeeping and
retry after 25 ms as readiness. Record no object/source failure for this refusal.
The historical peer wire operation, offsets and whole-file verification remain
unchanged; the smaller request is accepted by the existing protocol.

Use the capped payload as the effective per-key share too: a first pull that
has streamed no bytes cannot take the second background slot. A threshold
notification wakes the event loop independently of slow writes/final replies,
preserving re-pull at half even for small requests. Retired sink proofs remain
retained until the corresponding lane credit hook has published its credits.

The historical per-lane minimum of 8 MiB payload occupancy is explicitly
superseded by this common budget. Its existing 24/72 MiB workload, 60-second
completion deadline, <=16 MiB upper bound and link-pressure requirement for
two in-flight pulls remain. The same one-repository test now requires an
exercised >=256 KiB payload and <=512 KiB actual outstanding payload/window.
This allocation change is not evidence of unchanged sustained throughput;
default-window bulk/build performance remains outstanding.

The client stream retains the reservation until its call returns. Accepted
sink pieces retain their own proof while buffered or waiting for a gap; each
actual write batch clones the relevant proofs until its worker call returns.
Zero-payload writes also retain work. Dropping pending data releases only its
own proof, and cancellation or the historical runner's 500 ms exit wait cannot
return another still-executing round/write's reservation. Writer-owned proofs
also survive a sink/round unwind until that physical client work drops them.
This pool accounts conservative reserved work/buffer bytes; it does not claim
a whole-process RSS bound or retirement acknowledgement from every remote
handler after a timeout.

Dispatcher `started` counts executor acceptance separately from queue
`admitted`. Both follow the short group projection barrier, allowing demand
qualification to distinguish queued work from actual service capacity.

Native evidence fills six common background stream reservations through real
peer/worker calls, checks job/byte ceilings and verifies foreground executor
service and bytes under the retained 250 ms controlled bound. Other cases stall
actual worker writes after stream completion, including empty content, and
observe reservation retention through cancellation and eventual drain. Matched
boundary timing samples and source/executable identities are in
`test-evidence/wave-6/legacy-stream-checks.json`; default-window sustained bulk
throughput and mounted build effects remain unqualified.

Legacy uploads, classic inbound/source handlers, connection/thread/spool
resources and all physical remote-handler lifetimes are not yet covered by
this common admission. Remaining late effects, fairness, long load, mounted
saves, migration/activation and platform/build gates stay pending. Ordinary
typed activation remains off.

## CCR-WP6.3-16: same-round consumer retirement and flow measurement

When common admission refuses another pull and no client source stream is
active, retain the current round if its own sink still has buffered/pending
bytes, writes or stream-proof retirement in progress. Wait for that local
consumer's readiness event instead of finishing the sink, replacing round
authority and sleeping 25 ms. Without its own work capable of returning
capacity, keep the existing 25 ms readiness yield for other scopes. Cancellation,
errors and the existing ten-second round cap still end the round normally.
Successful admission clears the old capacity-refusal flag.

Proof retirement is distinct from an ordinary byte/settlement event: that
event can wake a consumer before the common reservation actually drops.
Track the short retirement phase under an atomic counter and send a separate
`CreditsReturned` sink event after dropping its proofs. The counter is installed
under the sink state lock before removing the proofs, and is reset on unwind.
Drop the reader's own proof before publishing `PullEnded`. These events are
local to the daemon; they change no peer wire message, lease, hash, durable
offset, payload or resource limit. No sink/scheduler lock covers transport,
worker I/O, hashing or the controlled retirement stall.

The previous implementation's actual native transfer changed its round nonce
eight times under a 250 ms local write stall. Corrected native cases retain one
nonce while verifying all 4 MiB, and while repeatedly stalling the interval
after lane credits publish and before common capacity returns. Provider reads
remain within their original 1 MiB limit; the full file is checked by bounded
ranges. A guard test checks unwind releases only its own proof and preserves
another live stream.

The existing 24/72 MiB allocation/pipeline fixture now records transfer times.
Its instrumented control has identical production bytes to CCR-WP6.3-15; its
measured test source stays identical in the final candidate. One warm-up plus
five repetitions per executable retain full verified-file completion, the
existing deadline, two in-flight pulls and the <=512 KiB actual payload/window
ceiling. The benchmark uses authenticated sessions and fake-worker memory
spools/delays, so it does not qualify real-disk fsync, mounted builds or the
final W6-BUILD/W6-LOAD gates. Exact evidence and limitations are retained in
`test-evidence/wave-6/stream-drain-checks.json`. Full Wave 6 qualification remains
pending; ordinary typed activation stays off.

## CCR-WP6.3-17: bounded round bookkeeping and empty-write completion

One retained legacy download round remembers at most 4,096 distinct objects,
including completed/failed objects used for deduplication and settlement. Want
requests cap their item count to the remaining room; accepting promoted demand
reserves its entry immediately. A returned overflow lease is released under its
exact session without a source hold/error. At the object boundary, finish the
accepted work and yield the existing content interval before a fresh round.
Do not evict live work to make room. This independently bounds the plan, begun/
failed/corrupt history and sink object table; it is not a whole-daemon cache or
connection/spool budget.

At most eight pull records may remain live or pending in a round. Remove each
record after publishing its single `ExchangeFinished`, rather than keeping all
completed pull history until the time quantum expires. A record is reportable
only after its source call ends and all accepted bytes and empty-write receipts
have left the sink. Per-round peak diagnostics distinguish these records from
the number of requests ever issued. Byte-plan estimates saturate at u64::MAX;
the existing payload cap still chooses the actual bounded request.

An empty source frame is not an empty completed local operation. Track each
accepted zero-payload write independently of byte credit. Publish its local
receipt with the same sink event batch as settlement, so completion counts are
visible before retiring the pull record. Rejected frames do not invent a
receipt. After sink finish joins the writers and drains/discards its work,
remaining accounting can close without waiting for a receipt that cannot arrive.
The events are internal; no peer wire format, lease or canonical empty hash
changes.

A real loopback/SQLite/CAS regression stalls an empty worker write for 600 ms:
the old code prematurely reported success with zero objects, while the new
code waits and reports one installed object exactly once. Production-helper
tests retain incomplete records while discarding 50,000 completed records,
enforce the object boundary and check overflow-safe byte estimates. An
authenticated fake-worker scenario transfers 4,353 distinct objects through
the 4,096-object boundary without losing their completions; its extended test
time quantum isolates the object limit from the production ten-second cap.
These are boundary checks, not a 50,000-file native saturation qualification.

Exact source/executable evidence and matched flow samples are retained in
`test-evidence/wave-6/round-bookkeeping-checks.json`. Global legacy resource
accounting, remote-handler lifetimes, full fairness/load/platform and mounted
save/build gates remain pending. Ordinary typed activation stays off.

## CCR-WP6.3-18: request-owned ordinary WANT admission

The loopback `objects.want` path now borrows the actual request deadline and
connection/CANCEL predicate, as the foreground legacy prepare path already
does. Worker shutdown is also checked. The ordinary JSON/direct dispatcher,
which has no connection cancellation context, uses a 60-second deadline and
still observes worker shutdown. Neither path adds a request thread or retained
cancellation hook. WANT bodies must be empty on loopback.

Checks precede selection, the initial lease snapshot, and the final grant
mutation, and run once more under the same grant lock before publishing the
result. If cancellation/deadline is observed after that mutation, undo only
the fresh grants made by this request. Reoffering an existing lease neither
renews its expiry nor makes its earlier writers owned by the new request;
a cancelled retry must not retire their effect tickets. The rollback and grant
share the lock, so a replacement cannot be removed between them. No scan,
network wait or diagnostic stall runs under that lock. Stop diagnostics run
only after the request has returned its locks and fresh grants.

Ordered run/pool/round floors and explicit exclusions already applied remain
monotonic. Cancellation is readiness, with no new source or object failure.
This closes observed cancellation during selection/admission; it does not
provide delivery acknowledgement for a reply lost after the final check.
That later ambiguity still requires exact-owner cleanup/expiry and remains
part of the pending reply-lost and physical-retirement qualification. Peer
wire layouts, hashes, durable offsets and TTL values are unchanged. Legacy
upload/source/inbound aggregate budgets and typed activation remain pending.

## CCR-WP6.4-1: private overlay journal and checkpoint foundation

`content::write::{WriteJournal, WriteSession}` introduces a private byte-write
session over any `ContentProvider`. Its intent captures operation UUID, branch,
workspace/entity/base generation, immutable base descriptor and content/alive
causal parents. It pins that base for the live session. Writes record only
changed aligned 64 KiB chunks with a local coverage bitmap; neither writes nor
checkpoints hydrate base bytes. Reads fetch required uncovered base bytes through
the provider and overlay verified patches. Shrink clips both patch coverage and
base visibility: subsequent growth creates zeros, never resurrected old bytes.
This local bitmap/journal encoding is not a canonical content object or wire type.

The worker shares one journal Arc, with a private SQLite connection at
`write-sessions-v1.sqlite`, application ID TKWS/version 1. A mismatched future or
foreign journal is refused before rewriting its format. Existing worker process
exclusion still applies. SQLite cache is 2 MiB, temporary work is file-backed,
WAL auto-checkpoint is 512 pages and the journal-size target is 8 MiB; these are
settings, not measurements of total RSS, device I/O or a hard filesystem quota.
Inputs/read buffers are limited to 1 MiB, record decoding to 4 KiB before copying,
and patch payloads to exactly 73,728 bytes (64 KiB data plus 8 KiB coverage).
Payload hashes and scoped chunk metadata/checkpoint checksums are validated.

Working and saved chunk maps are distinct. Ordinary mutations use NORMAL
transactions; the runtime must not acknowledge a recoverable close from them.
A successful explicit checkpoint uses a FULL transaction to verify pending
payloads and atomically replace the saved map/intent. Later buffered edits,
truncation or process exit cannot overwrite that saved selection. Recovery
resumes only the saved checkpoint, retains the operation/parents, and allocates
a fresh owner UUID under the journal transaction. Old handles and delayed old
checkpoints cannot write, checkpoint new-owner work or return successful reads.
A newer saved checkpoint supersedes older preparation tokens; their reads fail
stale instead of mixing versions. A cancelled operation never returns success;
a checkpoint already committed before cancellation remains recoverable.

The journal conservatively charges 73,728 bytes for each distinct pending chunk,
with a 64 MiB shared ceiling and at most 64 dirty intent groups. Before exceeding
either limit the caller checkpoints synchronously, under a revalidated initiator
owner, then retries. Those bounds do not implement the still-pending four-worker/
64-job canonical save executor. SQLite VM progress checks poll cancellation and
deadline every 1,000 instructions; private-lock waits poll at 1 ms. Blocking OS
storage calls remain OS-bound. Providers run outside the journal lock, and callers
must not hold the metadata engine lock during journal operations. Progress hooks
are scoped to the lock guard and removed on errors as well as success.

Repeated replacement drops unreferenced private payloads while protecting both
saved and working references. Truncation/recovery can leave conservative orphans;
the reclaimer examines at most 256 indexed payload keys per cursor page. Pending
intent/retention enumeration is likewise paged at 256 UUIDs. These cursors are not
workspace change-journal cursors: consumers must reconcile again after changes.
Recovery roots must be included in the retention inventory before reclamation.
There is deliberately no unproven metadata-commit acknowledgement/retirement API.
The operation UUID names one pending save episode, not an OS handle lifetime.
The future commit coordinator must fence/retire that episode and select a fresh
operation/base for later writes after committing a revision. Corrupt pending
work and shared backpressure also still require per-operation recovery/error
policy before this journal can be used by mounted callbacks.

Canonical chunk/manifest preparation, metadata/outbox generation revalidation,
namespace/create/delete/rename intents and ordering, four save workers, adapter
cleanup/flush/fsync integration, startup/shutdown coordination and full retention
integration remain required. No mounted callback uses this module yet, no close
or fsync durability claim changes, and typed activation remains off. Standalone
process-exit/SQLite tests do not close the Windows/FUSE, power-loss, ENOSPC, build,
amplification or full save-recovery gates.

## CCR-WP6.4-2: canonical preparation and complete patch-map integrity

`WriteSession::prepare` builds canonical content for a selected durable
checkpoint and returns an opaque `PreparedSave` holding the content pin and
selection/owner identity. It neither publishes metadata nor retires recovery
records. The coordinator still needs an atomic entity/generation/causal fence.
Read-only `validate_prepared` is not a replacement for that commit fence.

Preparation classifies indexed aligned spans as unchanged, changed or zero.
Unchanged hashes come only from the captured descriptor or authenticated ancestor
pages; a cursor retains at most one verified page per level. Changed leaves use
the session's provider range seam and verified private patches. Shape changes
rebuild only the necessary paths and boundary chunks, including direct/manifest
and root-depth transitions. Required absent structural pages return their missing
object key, never a whole-base staging fallback. Fully replaced output needs
neither old data nor old structural pages. A bounded cache of canonical zero
subtrees represents growth/holes without allocating work proportional to the
logical zero-filled length. New objects remain on private object/CAS surfaces.

A needed legacy base is converted from the target's local whole-file object in
one sequential input stream with a final legacy SHA-256 check. Import and builder
buffer reservations are separate; conversion precedes builder admission, avoiding
nested background permits. The session retains at most one converted-base pin
for the selected target provider. Repeated preparation can reuse it; restart or
switching targets may require conversion again. This is not a durable conversion
index. An absent needed legacy object is readiness for legacy hydration. Complete
overwrite/shrink can avoid that conversion. Provider reads remain outside journal
locks; selection checks also run while awaiting buffer/conversion admission and
between bounded legacy input reads. Failure/cancellation never publishes a root.

The preparation audit reproduced a journal flaw: deleting an overlay-map row
could make a successful read/preparation silently select unchanged old bytes.
Journal records now bind the complete sorted patch map with a SHA-256 digest and
count, including scoped metadata proofs, rather than checking only present rows.
Mutations validate existing maps before changing them, preventing corruption from
being re-sealed as valid. Reads and preparation validate their selected maps;
preparation checks before and after construction. The hash work is proportional
to pending changed-map metadata, not unchanged base-file bytes or repository
history. Missing payloads and missing/misindexed mappings are errors, not holes.
A valid saved record/map can be resumed even if a newer buffered header is damaged;
its new owner UUID fences the discarded owner. Pending enumeration retains saved
records independently of the damaged buffered header; it is not an availability
or health verdict.

This changes the private journal to **user_version 2** at the existing family
filename `write-sessions-v1.sqlite`. It adds scope seals and a checksummed
inventory (count plus XOR of scoped header digests), so losing an entire intent
cannot make its bytes appear unreferenced. The experimental v1 format is refused
without rewriting/discarding it; no live migration or activation is provided.
Canonical content encodings, existing metadata databases and peer wires are
unchanged. v1 fixtures are not automatically reinterpreted as trustworthy v2 maps.

Reclamation first reconciles that inventory and every current/saved map, then
examines payload keys. An opaque cursor binds the journal instance UUID,
`PRAGMA data_version` and connection `total_changes`; changed or foreign-instance
cursors restart reconciliation. Each call processes at most 256 scope/map/payload
records in total, under a short private SQLite transaction. No payload deletion
starts before a complete consistent audit. Corrupt or unknown reachability stops
collection and retains bytes. Replacement opportunistically drives bounded
maintenance; the runtime still must drive remaining pages under load and at
startup/shutdown. A continuous-load reclamation/budget guarantee is not inferred
from these finite fixtures.

An error during optional reclamation does not turn an already committed healthy
file mutation into failure. `reclamation_error` retains one bounded failure
episode, excludes cancellation/deadline, and clears only after a complete healthy
reconciliation/collection pass. Explicit reclamation still returns its error.
The future worker status path must surface and drive this state; it is not yet a
mounted/user-visible recovery workflow. Ordinary mutation/checkpoint errors and
cancellation continue to propagate normally.

Standalone native CAS/WAL preparation tests and amplification counters do not
satisfy metadata publication, namespace intent ordering, mounted close/flush/
fsync recovery, total device-I/O amplification or native/W5 build/load gates.
The four-save-worker/64-job executor, exact receipt-owned retirement, runtime
error/status/retention integration and full platform/compatibility qualification
remain required before activation.

## CCR-WP6.4-3: metadata save ownership, selected checkpoints and receipts

The metadata side of local canonical publication lives in
`core::content_saves`. It is callable only in the existing isolated
`fixture:content_v1` stores. Normal runtime/mount callbacks do not call it;
repository activation remains off. Metadata schema ownership stays with the
integrator (`core.rs` initialization and the `commit.rs` event observer).

`begin_content_save` captures an existing live file's workspace UUID, branch,
entity-local generation, base descriptor, visible content/alive parents and
immutable event template. The template freezes device, timestamp, branch,
basic-register change when present, and namespace frontier. Recovery does not
reconstruct these from a newer view. The checkout epoch is read from durable
config, separately from the entity generation and stale in-memory Store fields.
Workspace identity is initialized once in config. Capture bounds the frontier
at 256 IDs and each checksummed record at 64 KiB; exceeding a bound fails
without partially allocating a new publication right.

`content_save_entities` retains an entity-local generation and operation UUID
owner. Starting a newer operation replaces that owner under SQLite's writer.
Both values must match at selection/publication: a deleted/recreated counter
cannot transfer the old owner's right even if its numeric value repeats. A
normal local content/alive/location/basic commit invalidates ownership in the
same event transaction, including rename, delete, replacement and permissions.
Remote events do not silently invalidate or review an open writer's old bases;
concurrent alternatives remain subject to the existing causal oracle.

`select_content_checkpoint` durably selects the coordinator's checkpoint by
the complete checksummed intent/sequence/map record. A lower or different equal
sequence is stale. A newer selection fences an earlier prepared callback before
publication. This API does not itself observe the private journal or fence its
owner: the future session coordinator must order checkpoint selection with
publication, resume and rollover. The opaque `PreparedSave` proves the canonical
construction selection; it is not a stand-alone guarantee that a mutable
journal has not advanced since preparation.

`publish_content_save` checks the prepared pin belongs to this native CAS,
then rechecks durable workspace/checkout/entity ownership and the selected
checkpoint under `BEGIN IMMEDIATE`. It publishes the captured event with the
prepared descriptor, root metadata/admission outbox and checksummed receipt in
one outer FULL/WAL transaction. It refuses nesting inside a caller transaction
and refuses weaker synchronous/journal settings. Selection and receipt updates
must actually persist their exact expected records; an ignored/discarded write
cannot acknowledge publication. The receipt is verified against the actual
event and durable root/admission records before acknowledgement/replay.

The metadata operation UUID is the event identity. A matching committed retry
returns the original receipt before consulting the now-obsolete ownership
generation. It therefore cannot reapply a revision after later deletion or
checkout. A lost post-commit reply leaves a queryable receipt. Receipt lookup
uses one short WAL read snapshot, validates the event/root/outbox evidence and
returns absence for an unknown operation; absence never authorizes retirement.
Serialized receipt bytes alone are not deletion authority.

Writer acquisition uses deadline/cancellation-aware 1 ms busy retries, restoring
the connection's timeout. SQL execution polls the request at 1,000 VM operations;
interruption rolls back the whole publication and preserves recovery data.
Native device fsync cannot be interrupted by this VM hook. No journal lock,
file hashing/copying or content hydration is held in these metadata transactions.
An old slow metadata derivation still needs the existing off-lock preparation
integration before a bounded mounted fast path can be claimed.

One operation publishes at most one selected checkpoint. Later buffered or
durable edits remain in its journal; selecting them after publication explicitly
requires operation/base rollover. This change adds no journal retirement API,
automatic rebase, pending create/delete/rename recovery, active save executor,
mounted flush/close promise or user-visible status. The next integration must
retain/rebase newer work, adopt committed receipts on restart, retire only the
receipt-owned episode, and coordinate namespace, checkout and shutdown fences.
Platform crash/ENOSPC/power-loss, total physical amplification, full build/load,
retention/migration/compatibility and all outstanding Wave 6 gates still apply.

The Wave 6 error extension explicitly registers both the private journal and
publication codes. Superseded session/checkpoint/entity/view/workspace and
already-committed selection outcomes are Contention, not failure-counter input.
Stable capture/sequence exhaustion is NeedsUserAction; unsupported journal
versions are Incompatible. Corrupt records, lost selection/receipt persistence,
poisoned ownership and violated publication/rollback invariants are Fatal in
their pending-save/journal/connection scope, retaining recovery data rather than
automatically repeating a damaged operation. These additions leave the frozen
historical table and the existing recoverable-content repair classes unchanged.
Status/error consumers still need explicit save scoping; the taxonomy alone
does not wire a mount recovery workflow or permit an unrelated engine breaker.

## CCR-WP6.4-4: unwind cleanup, failed connections and required save records

`content_save_write` now contains Rust unwind long enough to remove its request
progress handler, undo tentative metadata/cache state and release the outer
writer, then resumes the original panic payload. It does not convert panic into
successful publication. Before-commit unwind leaves no event/outbox/receipt;
after-commit unwind leaves the same queryable receipt and idempotent replay as a
lost reply. Process abort/exit continues to rely on durable SQLite/journal
recovery rather than Rust cleanup. Cleanup does not retain a cancelled request's
SQL hook, and restores the connection's original busy timeout.

A failed rollback is a distinct `CONTENT_SAVE_ROLLBACK_FAILED` outcome. The Store
handle is permanently marked unusable for saves, and its derived caches are
cleared. It attempts to replace its connection with an empty in-memory handle
and close the real connection, abandoning the failed transaction without an
automatic reconnect/retry. Errors in replacement/close are retained alongside
the original and rollback error; owners must discard this handle even if that
secondary cleanup fails. During unwind the secondary failure is logged before
resuming the original panic. These are connection/save outcomes, not permission
to quarantine another engine or discard promised journal bytes.

The correctness flag lives on Store, not in disposable caches. Save/publication/
receipt entry points reject a marked handle with `CONTENT_SAVE_CONNECTION_FAILED`
(Fatal). `SyncStorePool` discards it on return even when its empty replacement is
in autocommit; the next checkout opens a fresh secondary Store. A direct owner
must likewise discard/reopen its Store and revalidate durable evidence. This is
not activation of a mount save coordinator. The existing private journal retains
its checkpoint and can resume under a fresh owner after the actual metadata
connection is released.

Required capture/config/owner/event rows that are missing, incorrectly typed,
out of range or oversized produce `CORRUPT_CONTENT_SAVE`, rather than an unknown
retryable SQL error. Bad persisted record validation carries the same scope.
An unknown operation in the optional receipt lookup still returns absence.
Real SQLite busy/interruption/authorization/I/O failures are not recast as
content corruption by this record conversion. Ordinary stale owner/checkpoint
outcomes keep their existing Contention classes.

Qualification uses actual SQLite authorizer denial of rollback for returned
errors and unwind, an actual commit hook that rejects COMMIT, cancellation of a
completed request followed by independent SQL, a second writer after cleanup,
fresh Store/journal recovery and pooled handle replacement. Authorizer denial
is not ENOSPC, power-loss, inability to allocate the empty replacement or an OS
close failure; those platform/bounds gates remain open. Private-branch save
publication additionally exercises deduplicated native bytes with private
admission flags and retained permission/creation fields. The journal format,
canonical encoding, peer wires and activation policy do not change.

Receipt-owned retirement, chunk-local rollover while later edits continue,
namespace intents, four-save-worker/64-job scheduling/batching, actual slow-disk
journal coordination, full retention/status and mounted durability still require
implementation and the complete Wave 6 qualification.

## CCR-WP6.4-5: exact publication history and metadata continuations

The metadata component now retains `content_save_history`, keyed by operation
UUID and complete checkpoint digest, with an additional unique generation key.
Capture, selected checkpoint and receipt are bounded checksummed records. Their
archive is committed and read back with the event/outbox/current receipt in the
same FULL/WAL transaction. An ignored/discarded archive cannot acknowledge a
save. Existing pre-archive current receipts may be archived only after checking
their actual immutable event/root/outbox evidence. No old event is rewritten.

`content_checkpoint_receipt` queries an exact immutable checkpoint across later
generations. `content_save_receipt(operation)` continues to describe only the
current generation; its absence cannot stand in for historical receipt lookup.
Old committed preparation/selection retries validate and return their original
receipt without replacing a newer capture, selection or event. Unknown exact
checkpoints remain absent; a successor that proves a missing parent archive
turns that missing required evidence into corruption instead of recreating it.

`continue_content_save` atomically archives/checks the committed parent, claims
the next local entity generation, persists an immutable successor capture and
records an idempotent predecessor mapping. The journal operation UUID is stable,
so future adoption need not rename its entire patch map. Publication identity
includes the captured generation. Initial event UUIDs stay unchanged; successor
event IDs are SHA-256 of a versioned domain, operation UUID, generation and
predecessor checkpoint digest. One generation publishes at most one checkpoint.

The successor's byte base is the committed parent's descriptor, and its content/
alive parents are the parent's actual event, even when a remote competitor is
currently visible. Its new frontier/clock/basic template is captured once in
that transaction, not reconstructed on retry. This is a new generation, not
retroactive observation/review of a remote content alternative. A local rename,
delete, basic change, newer operation or checkout fences creation of this new
generation. Already-persisted continuations remain retrievable as recovery
evidence, including after later generations; retrieval does not grant a stale
generation permission to publish.

Both current and mapped successor records, ownership and parent history are
checked before acknowledgement. A lost response returns the same persisted
capture rather than allocating another generation or timestamp. Successor
records are indexed by predecessor checkpoint; older continuations validate
against either the current capture or the archived capture for that generation.
Lookup does not enumerate history. The optional predecessor field is omitted
when serializing initial captures, retaining their existing byte encoding and
checksum. This codec check is not an old-executable migration/downgrade gate.

The returned `SaveContinuation` is opaque and has no serde deserializer. It binds
the previous checkpoint, new intent, canonical native store path and a retained
base pin. It is a metadata handoff, not evidence that bytes remain available,
not journal-deletion authority and not a mutable-session rollover. The native
pin keeps lifetime separate from availability. The private v2 journal remains
unchanged and cannot yet adopt the new intent atomically; its older/newer
checkpoints and buffered bytes remain retained.

Qualification uses separate fixture journals for subsequent metadata generations
while retaining the original journal. This deliberately does not simulate a
completed same-journal transition. It covers exact historical replay, stable
operation keys with distinct events, remote alternatives, local/checkout fences,
discarded writes, missing/corrupt history, lost replies, unwind/failed rollback
and real process exits during archive/continuation. `CONTENT_SAVE_NOT_COMMITTED`
is Contention; lost continuation persistence is Fatal to its save/connection
scope. Activation, peers and mounted callbacks are unchanged.

The receiver must still implement durable private-journal adoption, preserve
later edits/checkpoints, fence old owners, retain/rebase truncation semantics,
reclaim only receipt-owned data in bounded pages and recover both sides of each
cross-database boundary. Complete original Wave 6 resource, amplification,
platform, build/load, migration/retention and status gates remain required.

## CCR-WP6.4-6: immutable private overlay indexes

`content::overlay_map` provides a separate SQLite sparse-map primitive for the
pending journal receiver. The active private v2 journal, canonical formats,
metadata publication and mounted callbacks are unchanged. No root is an adoption
receipt, independent capability, byte-availability proof or deletion authority.
The embedding journal must seal roots with its bounded intent, pin their lifetime
and revalidate its owner/selection before publishing a replacement root.

Nodes use `TKOM` version 1, operation scope UUID, map kind, level and key prefix.
The domain-separated SHA-256 covers the complete body. Sixteen-way branches
contain child hashes, referenced-leaf counts, maximum mutation sequences and
minimum truncation lengths. Patch keys cover 2^32 chunks (eight levels);
truncation keys cover full u64 sequences (sixteen levels). Patch leaves contain
payload digest, valid length and sequence; truncation leaves contain sequence
and length. Bodies are at most 1,024 bytes (branches: 927). These private bytes
are not TKFM/TKFC or a peer wire. Unknown root versions require upgrade;
scope/kind/prefix/hash/stat/type/size failures are journal corruption.

Point reads and path-copy updates visit at most nine patch nodes. An authenticated
sequence floor hides published generations without copying historical leaves;
`entries()` still counts physically referenced leaves, including hidden ones.
The minimum cut over `(published_sequence, target_sequence]` preserves shrink/
grow zero semantics when a canonical base advances; consulting only the original
base's low-water mark would resurrect bytes. Whole-subtree chunk truncation and
sorted bulk construction do not retain a point-update path for every entry.
Missing demanded nodes never become an absent patch or a zero-filled hole.

Mutation requires a caller-owned SQLite transaction, otherwise
`WRITE_JOURNAL_TRANSACTION_REQUIRED` (Fatal to that save scope). Every installed
node is read back and compared byte-for-byte before success, covering ignored,
deleted and altered writes. An ignored insertion is acceptable only when the
exact node already exists. Oversized/incorrectly typed bodies are bounded by
the SELECT before materialization. Actual SQL authorization/I/O failures retain
their cause. Root durability, FULL/WAL policy, progress/busy hooks and committing
the corresponding intent remain the caller's responsibility.

Walk and pruning cursors are volatile, bounded by trie depth/fanout and poisoned
after partial errors/cancellation. Restart from a pinned root rather than resume
past a failed task. Pruning returns a replacement root only when complete;
staged immutable nodes can be committed between pages. A rolled-back or failed
page commit invalidates the cursor. Each page permits 1..256 node read/install
units; an install additionally performs one bounded verification SELECT, counted
separately as `MapWork::node_checks`. The unit budget is not a count of all SQL
operations, payload work, WAL writes or device I/O. A collector must independently
fence the live root set and retain in-flight readers; this module collects nothing.

Qualification covers independent binary vectors, WAL reopen, a BTreeMap mutation
oracle, immutable forks, sequence floors, relative truncation watermarks, bounded
pages, poisoned cursors, real SQLite fault triggers and fixed point/prune work at
5,000/50,000 entries. Component timings exclude transaction begin/commit and are
not mounted save/build or physical amplification evidence. Same-journal adoption,
receipt-owned retirement, cross-database recovery, runtime integration and all
original Wave 6 gates remain required; activation remains off.

## CCR-WP6.4-7: root-based private journal version 3

The `WriteJournal` implementation now uses the immutable overlay indexes.
Private SQLite `user_version` is 3; application ID and file location remain
unchanged. Existing private versions 1/2 and unknown versions are refused before
schema/durability changes, retaining their records. No implicit lossy migration,
old-executable downgrade fence, production activation or mounted upgrade is
claimed. Canonical encodings, peer wires and metadata event identities do not
change. Private checkpoint serialization now includes sealed overlay roots;
historical experimental checkpoint metadata still needs an explicit migration
policy before activation.

Working owns cumulative patch and truncation roots plus a separate pending-patch
root. Saved retains independent immutable patch/cut roots in its bounded record.
Root scope, counts, maximum sequences, patch-map digest and the pending count are
checked against the intent/header. Root bodies are verified before their
summaries are trusted; descendants are verified on demand or by the bounded
collector. Ordinary reads/mutations no longer hash every Saved row. Demanded
missing/corrupt nodes or payloads are errors, never unchanged base data or holes.
The root's sequence floor is initially zero; publication adoption has not yet
advanced it in this implementation.

A write path-copies its cumulative and pending patch paths with the current
mutation sequence. Shrinking truncates both roots by subtree and clips a partial
boundary leaf; clipping a Saved boundary also consumes pending-byte quota.
Truncation sequence/length is recorded independently for the future base change.
Growing does not restore clipped patch masks or the base's low-water mark.
An older Saved read retains its original bytes while Working changes or shrinks.
Resume selects only Saved, assigns a fresh owner, clears pending state and can
recover even when the newer buffered header is damaged.

Checkpoint uses FULL/WAL, validates only the bounded pending-patch set and its
payloads against Working, then persists the complete Saved root by reference.
The 64 MiB pending-patch and 64 dirty-group limits remain; an indexed dirty-session
query accounts pending counts instead of counting historical patch rows. The
checkpoint still performs synchronous work under the private connection mutex;
independent reads during slow fsync and the save executor/batching remain open.
No durability policy is weakened.

Working/Saved bytes, dirty flag, queue count, scope seal and inventory update are
read back before acknowledging a mutation/checkpoint. Ignored or altered header,
payload and inventory writes cannot return success. Required malformed/missing
SQL record values are scoped corruption; actual authorization/I/O/interruption
errors retain their cause. SQLite progress/cancellation cleanup and lost-reply
Saved recovery remain in force.

Payload collection first audits inventory and every current Working, Saved,
pending and cut root in pages, marking live payload hashes in SQLite with a
volatile pass UUID. Marks are read back before use. Only after the entire root
set and inventory validate may unmarked payloads be removed. A foreign commit,
own mutation or different journal instance restarts the audit. Each page bounds
combined node/scope validation and payload examination to 256 units; mark/readback
SQL and device work are additional. Missing roots/intents or discarded marks
retain payloads. A failed maintenance episode does not invalidate an unrelated
already committed write and remains separately observable.

Historical immutable nodes are currently retained. Mark rows use one row per
payload hash, overwritten by later passes and removed with collected payloads.
Continuous root changes can restart a long audit; bounded page size is not proof
of collection progress, bounded history storage or the 30-minute resource gate.
Node reclamation, receipt-owned retirement and root/reader lifetime registration
remain to implement before activation.

Qualification combines actual journal read/write/FULL-checkpoint behavior after
5,000/50,000 committed Saved entries with byte/oracle, crash/restart, scope,
corruption, deadline, native preparation and publication regressions. The scale
fixture bulk-seeds an authenticated Saved root before timing, with one shared
payload and sparse zero regions; it does not simulate free creation of 50,000
ordinary edits. SQLite changed-row counts include bounded maintenance, not
physical/device write amplification or all read/SQL work. This component evidence
does not pass the mounted build, save pipeline or physical amplification gates.

Same-journal `SaveContinuation` adoption, exact prior-checkpoint retry evidence,
staging/revalidating later Saved/Working roots, new owner/base pin, cross-database
crash recovery and conservative receipt-owned retirement are still required.
Namespace intents, runtime adapters, scheduling/status, all original platform/
load/build gates and WP6.3/6.5/6.6 remaining work stay in scope and activation off.

## CCR-WP6.4-8: same-journal continuation adoption with retained predecessors

`WriteSession::adopt_continuation` receives the opaque metadata handoff and the
native provider. It verifies the journal's canonical store path, pin/provider
identity, stable operation/workspace/entity/branch and next generation before
changing private state. Indexed checksummed `checkpoint_records` retain exact
private checkpoint identity; a metadata handoff for a missing/different private
checkpoint cannot reconstruct that evidence. Checkpoint records are metadata
history, not permission to read a superseded selection or delete payloads.

The receiver captures Working and Saved under the old owner. It evaluates cuts
over `(published_sequence, target_sequence]` independently for each view, then
prunes their patch/cut roots in pages of at most 64 node read/install units.
Each install also includes the separate bounded readback. Staging commits use
NORMAL and release the journal mutex between pages. No active owner/root/base
changes during staging. A final FULL/WAL transaction rechecks the entire captured
Working/Saved state and exact parent record; a concurrent write, checkpoint or
resume makes the staged result stale. The caller can retry from current state.

That final transaction stores the predecessor Working/Saved records in
`retained_overlays`, checks their exact readback, adds their seal to inventory,
records the rebased Saved checkpoint and atomically installs the next intent,
new owner, patch/cut roots and publication floor. Pending ordinary patches keep
their quota and bytes. Current and later Saved/Working views can have different
truncation minima; a base-pointer swap or the original low-water mark alone is
insufficient. The returned session owns the new native base pin and starts with
an empty legacy-conversion cache. Old handles/preparations remain fenced.

If no later durable edit exists, the rebased Saved baseline remains available for
resume but is not pending publication. `needs_publication`, the indexed pending
flag and public checkpoint results distinguish this baseline. Metadata selection
rejects a baseline; adoption does not synthesize an extra no-op event. A later
edit uses the captured next-generation parents and publishes through the normal
metadata transaction. The handoff remains private recovery evidence; it does
not bypass metadata's live owner/view/workspace checks after checkout or local
mutation.

A persisted marker binds the predecessor checkpoint, old owner and retained
archive. Retrying the same old handle after a committed lost response verifies
those records and returns the current new owner without reapplying roots or
allocating another archive. New mutations by that owner survive the retry.
Resume creates a new owner and clears the marker; an old handoff cannot reclaim
ownership afterward. Later continuations replace the marker and likewise fence
older retries. Stale selection/owner and corruption remain distinct outcomes.

The collector now includes current sessions and retained predecessors in its
inventory/root audit before deleting any private payload. Missing or corrupted
archives, including loss of a whole row, fail the audit; unknown canonical byte
availability does not authorize dropping these roots. Adoption performs no
receipt-owned payload deletion. Historical checkpoint/node/retained records grow
until explicit bounded retirement/reclamation is implemented. Native CAS base
root registration, in-flight root lifetimes, automatic recovery from missing
canonical bases and runtime retention/status wiring remain required before
activation; retaining private payloads alone does not satisfy those contracts.

If transaction cleanup leaves the SQLite connection outside autocommit, the
journal marks a non-cache correctness flag, retries rollback without the request
progress hook and attempts to replace/close the physical connection. All future
requests on that journal fail `WRITE_JOURNAL_CONNECTION_FAILED` (Fatal), even if
replacement succeeded. The owner must discard/reopen it and revalidate Saved.
This prevents uncommitted adoption markers from masquerading as committed
lost-response evidence. Actual authorizer-denied rollback is exercised for
returned failure and unwind; allocation/OS-close failure and power loss remain
separate platform gates.

Private SQLite `user_version` is now 4 for the publication flag, exact checkpoint
records, retained records and adoption marker. Versions 1/2/3 and future/foreign
formats are refused without rewriting their recovery bytes. This is not a live
migration or an old-executable upgrade/downgrade rehearsal. Canonical content,
peer wire, SaveIntent capture codec and event identity algorithms are unchanged.

Qualification covers an actual same-journal publication/adoption/event chain,
independent Saved/Working truncate/grow views, concurrent writes during staging,
cancellation/SQLite deadline, wrong store/provider, lost response, discarded or
missing required records, retained payloads after canonical-byte loss and six
real child process exits at six staging/archive/owner boundaries for both a
published baseline and distinct later Saved/Working shrink/grow state (twelve
exits). The overlapping cases inspect Working's persisted sequence, visibility,
authenticated leaf/payload and masks before resume, run the product root/inventory
audit, recover Saved and publish it exactly once while preserving the older
receipt. Resume deliberately discards ordinary buffered work; this is a process
exit matrix, not power-loss or ENOSPC evidence. A separate seeded
5,000/50,000-patch probe uses genuine metadata handoffs, two later immutable
views and bounded pruning; initial preparation/history cost is excluded and
reported. No mounted durability, physical amplification, long storage bound or
native/W5/W6 build gate is inferred from component timings.

Receipt-owned retirement/node collection, native root registry/read lifetimes,
namespace intents, save executor/batching, actual slow-fsync independent reads,
runtime adapters/status, WP6.3/6.5/6.6 remaining work and every original platform,
build/load/soak/compatibility gate remain in scope. Activation stays off.

## CCR-WP6.4-9: private recovery root source and retained Saved readers

The journal exposes `recovery_roots` in pages of 1..64 source records. It includes
all current sessions (also those without Saved) and retained predecessors, one
base FileRevision per scope, without loading their complete patch maps. Header/
checkpoint/scope validation and the inventory count/XOR cover the enumeration;
loss of an entire source cannot turn into a complete empty root set. Partial
results are not a complete catalog. Volatile cursors bind journal instance and
SQLite own/foreign change tokens. Any intervening write invalidates the cursor;
the caller discards partial results and restarts. A completion fence can be
revalidated against the same private inventory/token.

This is one recovery root source, not whole-store reachability or deletion
authority. Metadata/history/conflict/snapshot roots, explicit native pins and a
native deletion barrier must still be joined by retention. Ordinary writes and
maintenance conservatively invalidate this source's token even when its base
set is unchanged; progress under continuous mutation is not qualified. Startup
does not materialize a vector or permanent native pin for every retained scope.

`pin_native_recovery_root` validates an opaque source from this journal, acquires
the same-store native pin outside the journal mutex and revalidates its source.
The caller retains that pin through its native walk/read. A pin keeps lifetime
separate from byte availability; missing bodies do not become empty files.
Sources/cursors/fences from another journal instance cannot be reused. No root
source object is a serialized publication or serving capability.

`open_retained_checkpoint` returns an immutable Saved reader from an archive's
verified bodies/seal. It registers a bounded read ticket under the journal mutex
before acquiring the base provider pin outside that mutex. At most 64 independent
retained readers exist per journal; failed pin/acquisition and reader drop return
the ticket. The reader owns the journal/provider/base pin, surviving closure of
the previous sessions and re-opening of the worker journal. Future retirement
must inspect these tickets while holding the same journal mutex; no archive
deletion/retirement API is introduced here.

Retained reads validate the archived identity/seal before and after byte work and
at each chunk. The saved selection is independent of the current owner and later
edits; it cannot resume or overwrite them. A shared overlay range helper applies
the same valid-length/mask/base-visibility rules to live and retained reads.
Provider byte I/O occurs outside the journal mutex. Cancellation, required missing
nodes/payloads, altered archives and wrong ranges return errors; output is
discarded on error.

`restore_native` reconstructs and pins those Saved bytes using ordinary COW
classification and verified native installation. It returns content only, never
a PreparedSave/metadata publication ticket. Intact native subtrees are reused;
required missing old-base data remains an error. A needed legacy base converts
once per reader/target and retains the verified converted pin; full replacement
does not hydrate an absent legacy base. Request cancellation and archived-scope
checks remain in force during buffer waits/conversion/building. This is an
explicit recovery operation, not automatic fallback by a live session.

Qualification covers bounded complete root enumeration, stale/cross-instance
cursors/fences, missing inventory scope, native-store identity, base pins after
session closure/reopen, retained read quotas and failed-pin cleanup, slow/cancelled
base reads alongside current writes/checkpoints, archive corruption and actual
canonical-manifest loss followed by retained reconstruction without another
metadata event. Legacy recovery uses trusted private archive fixtures; actual
metadata/native continuation fixtures cover the normal retained reader path.
The provider and prior journal/publication/oracle suites remain required.

No persistent schema, canonical format, wire, event algorithm or activation
policy changes. Private user_version remains 4. Automatic source registration in
native retention, whole-store root reconciliation and actual GC/reader races,
automatic retained fallback, receipt-owned retirement and node/history/archive
reclamation remain open. Native/WinFsp/FUSE/WSL/two-peer/build/load/soak/physical
amplification and all other Wave 6 release requirements are unchanged.

## CCR-WP6.4-10: bounded orphan private map node collection

Private reclamation now audits and sweeps immutable overlay nodes as well as
payloads. Map walks report the hashes of nodes verified on each page. The
collector walks all distinct current Working/pending/Saved and retained
predecessor patch/cut roots at floor zero, readback-verifies their epoch marks
and completes the sealed scope inventory count/XOR audit before sweeping.
Only nodes absent from that exact completed pass are candidates for deletion.
Missing/corrupt reachable bodies or lost marks fail the audit; unavailable
canonical bytes do not remove archived roots. Superseded checkpoint_records
are identity history, not independently readable recovery roots. Their old
private map bodies may be collected; the history records themselves stay.

Each page shares a budget of 256 inventory/traversal/candidate units across
audit, payload and node phases: `validated + examined + nodes_examined <= 256`.
The original `removed`/`examined` counters retain payload-only semantics; node
counts are separate. Mark writes and readback verification add SQL operations
inside traversal units. This is neither a 256-statement nor a physical-I/O
bound. Instance and SQLite own/foreign change tokens still invalidate a pass
after any intervening mutation. Completion of a partial/stale audit confers no
deletion authority, and continuous-mutation progress remains unqualified.

Adoption registers an RAII staging lease under the same journal mutex used by
node sweeping, before it commits any staged roots. Up to four active stages
are admitted. The lease spans all NORMAL staging commits through the final
FULL root transaction, errors and unwind. While any lease exists, node sweep
returns `nodes_deferred` and a continuation; payload work may proceed. This
protects staged nodes not yet referenced by a durable header. After a crash,
reopen must audit again: committed headers protect their nodes, and abandoned
staging nodes are orphans. This guard does not implement the runtime save
executor, its four workers or its 64-job queue.

Node marks use a connection-local FILE-backed TEMP WITHOUT ROWID table keyed
by the 32-byte hash. Reopen loses marks and volatile cursors and cannot reuse a
previous pass. Node deletion also removes its mark. The SQLite main/temp cache
configuration is 1536/512 KiB, preserving the prior 2048 KiB aggregate cache
configuration; this is not an RSS/storage bound. No persistent schema change
is introduced and private user_version remains 4. SQL row reclamation does not
promise file shrinking or establish freelist reuse, temp-file growth or device
amplification under sustained work.

The 5K/50K root-journal component probe now allows at most 600 changed SQL rows
instead of 400, because each audited node adds a TEMP mark DML to the previous
payload marks. Its bounded 256-unit maintenance page is unchanged. These
counts include TEMP writes and exclude readback queries/physical bytes; this
accounting change does not waive any latency or amplification release gate.

This is orphan private node collection only. Retained archives and their reader
tickets remain, checkpoint identity history remains, and native CAS objects
are not deleted. Receipt-owned archive/history retirement, joined native root
registration/deletion barriers, actual native GC/reader races, automatic live
fallback, runtime/mount integration and every remaining Wave 6 qualification
gate remain required. Activation stays off.

## CCR-WP6.4-11: bounded runtime content-save coordinator

`runtime::content_saves` connects private FULL checkpointing, canonical
preparation, exact metadata selection/publication, view refresh and same-journal
continuation/adoption. It is an explicit headless worker component: no startup,
RPC or mount callback constructs it yet. It refuses mounted engines or engines
with legacy sessions/handles. This guard must be replaced by the actual provider
adapter integration before activation; it is not that integration. Typed writes
still require the existing isolated fixture gate and activation remains off.

One executor registers weakly with the engine and starts exactly four fixed
workers. At most 64 operations are outstanding, including active jobs and
producers checkpointing before queue insertion. Admission precedes checkpoint
allocation/work, rejects another in-flight job for the same operation and has
no synchronous worker fallback. Each reservation has its own UUID; only its
permit can release that operation's slot. The executor binds one same-store
WriteJournal Arc and refuses other stores or independently opened journal
instances. Thus a caller cannot multiply the private dirty-byte budget by
submitting journals per file. The existing worker/store exclusion remains a
prerequisite; this does not license multiple engines/workers for one store.

`SaveFile` keeps stable local identity and replaces its session only after
successful adoption. Per-file serialization covers writes, truncation,
checkpointing and final adoption. Canonical preparation occurs outside both
the file mutex and the metadata engine mutex, so writes during preparation
survive into the successor. Range reads capture the session without retaining
the file mutex through provider I/O; ordinary stale-generation errors still
require the caller to discard output. This handle does not yet represent a
durable namespace entry or a mounted open-handle identity.

Normal enqueue admission checkpoints private work; a ticket acknowledges the
selected metadata revision and private adoption. Enqueue alone is not a mounted
close/flush acknowledgement: a stale owner after an adoption lost reply can
first replay its already-committed private state, and carried Working may still
need a later checkpoint/publication. Adapter integration must use the complete
flush/recovery fence before acknowledging that boundary. A FULL
checkpoint also preserves later Saved bytes while an older committed parent
still needs adoption. A bounded exact-generation metadata history lookup finds
that parent after a lost reply/reopen, rather than mistaking the newer Saved
checkpoint for the committed parent. It validates capture/intent/receipt and
grants no new publication or deletion authority. An adoption that committed
before its reply may replay through the old session marker. An unpublished
superseded checkpoint can instead advance to the latest Saved on retry.
`flush` repeats this pipeline until the adopted generation is clean, including
later Saved work. Continuous edits may keep it busy until its deadline.

New canonical content stays pinned across publication and acquisition of the
continuation base pin. Exact committed retries refresh the engine observation
and activation delta as well. This does not reconcile lost mounted notifications
or join all native reachability sources; those remain integration requirements.
Preparation/adoption failure, cancellation or a caught job panic returns the
permit and retains journal recovery data. Failed jobs are not autonomously
retried by a background sweep yet. Completion/error storage belongs to the
caller-held ticket; the pool does not accumulate a completed-job history.

The enqueue context owns the operation, linked to pool stop. Cancelling a ticket
wait alone leaves the accepted save running. Metadata-lock acquisition polls
with the context; checkpoint/provider/build work occurs outside that lock.
`stop` closes admission and cancels jobs. `shutdown` caps its drain wait at the
earlier caller deadline or five seconds. It does not kill uninterruptible OS
I/O or arbitrary blocking provider cancellation hooks. On timeout, remaining
workers/reservations keep the pool registered, preventing replacement pools
from multiplying them on that engine. Drop signals stop without an unbounded
join. Global shutdown/checkout/status wiring is still required; this component
does not establish the whole-runtime five-second resource gate.

No persistent schema/canonical/wire/event changes; private user_version remains
4. Compatible FULL checkpoint/metadata batching, independent private reads under
slow fsync, namespace/open-handle recovery, actual mounted callback mapping,
startup reconciliation/retry/status, Engine quiet/checkout/shutdown fences,
receipt-owned retirement/native retention and all original Wave 6 platform,
build/load/outage/soak/compatibility gates remain open.

## CCR-WP6.4-12: compatible private checkpoint groups

`WriteJournal::checkpoint_group` accepts at most 64 distinct owners from the
same journal instance, validating every owner/header/root under the writer
transaction before advancing only the requested dirty sessions. Their Saved
headers and checkpoint identity records commit in one FULL transaction. Results
preserve input order. An error acknowledges none; a lost reply can leave the
entire group committed. Unrequested Working remains buffered, and an invalid,
stale or foreign owner cannot partially advance the group. The existing bounded
pending-payload verification and private inventory/seal checks remain in force.

`checkpoint_work` exposes process-local logical counters for successful FULL
checkpoint COMMIT calls and dirty sessions advanced. Empty/read-only checkpoint
transactions also count when a COMMIT is issued; an empty input group does not
open one. Counts exclude ordinary writes, adoption, canonical installation,
metadata commits and physical fsync/WAL/device I/O. They are not durable receipt
or reclamation authority and reset when the journal is reopened.

`SaveExecutor::enqueue_batch` reserves the entire group against the existing
64-outstanding limit before allocating its sorted file/guard/job vectors. It
acquires file locks in operation UUID order, checkpoints the compatible group,
performs exact committed-parent lookup and queues independent publication/
adoption jobs on the existing four workers. Returned operation/ticket pairs
follow that UUID order. Duplicate, oversized or foreign groups and files with
an earlier pending pipeline are refused without partial queue insertion. Error
after FULL checkpointing retains Saved for retry, even if no job was inserted.
There is no additional writer thread, inline save fallback or enlarged budget.

`flush_files` is a volume-flush slice for at most 64 supplied stable file handles.
It drains earlier pending pipelines first, groups fresh compatible work, waits
for the selected jobs and repeats until clean or deadline. It observes every
ticket even when one fails. Larger namespace/startup enumeration and stable
snapshot/checkout fencing remain the caller's integration responsibility.
Enqueue alone still does not acknowledge mounted close/fsync, particularly after
an adoption lost reply with carried Working. The headless/unintegrated callback
guard and existing fixture-only typed-content gate remain unchanged.

This batches private recovery checkpoints only. Canonical installation,
per-revision metadata selection/publication/continuation and private adoption
retain their existing transactions and pins. A batch of 64 private checkpoints
is not one physical fsync for 64 complete saves. Automatic coalescing of
independent close producers, compatible metadata transaction batching, slow-fsync
independent readers, mounted callback/namespace/lifecycle integration, retention
and every original Wave 6 release gate remain required. No schema/canonical/
wire/event change; private user_version stays 4 and activation stays off.

## CCR-WP6.4-13: provider-backed reads through the filesystem adapters

A typed file entry now selects an immutable `Staged::Content` descriptor rather
than interpreting the absent legacy object string as an empty file. Its known
length comes from the validated descriptor even when the manifest or some file
chunks are absent. The legacy staging/hydration path remains separate. Native
snapshot, direct staged read, resize and writable-open paths fail explicitly
until the write-session integration supplies their COW implementation; they
cannot silently materialize an empty base or overwrite a typed revision.

The WinFsp and FUSE adapters acquire the selected provider pin before
acknowledging a typed open. Pin acquisition, local provider construction and
range I/O run outside the Engine mutex. Short metadata sections capture the
selection and revalidate the store, view, handle and selection identity. A
provider returning a pin for a different descriptor is refused. Engine waiting
obeys the request deadline/cancellation; the context links to the registered
object worker's stop token. A failed open releases its provisional handle.
Cancelling that token runs its hooks outside the inflight-objects mutex.

The session holds the selected pin until close or a clean revision refresh;
an in-flight read retains its own selection through completion. Refreshes that
select the same descriptor retain the existing binding. Clean non-writer
sessions refresh their complete entry metadata, including timestamps and
content revision, as well as bytes. Dirty or writable sessions keep the
existing pinned-base rules; deletion does not discard an already open file's
bytes. A concurrent selection change makes an old read fail `STALE_VIEW`
rather than return bytes from a superseded selection. Adapter callers discard
all output on failure, and WinFsp acknowledges zero transferred bytes.

The provider-independent range API short-reads at 1 MiB. Every provider call is
at most 1 MiB. WinFsp fills its borrowed ABI destination to the requested length
or actual EOF using bounded calls under one immutable selection/context; it
does not introduce false EOF at an internal buffer boundary. FUSE allocates
only after descriptor selection and retains its existing 1 MiB request/reply
bound. A verified ready range can succeed while an unrelated chunk is absent.
Missing/corrupt demanded data is an error, never zeros or apparent EOF.
Existing WinFsp disabled caching and FUSE direct-I/O settings remain in effect.

Provider lease release is bounded and performs no disk/network I/O; providers
own any separate bounded asynchronous cleanup. Concurrent losing pins are
explicitly dropped after releasing the Engine mutex. This is a lifetime/API
contract, not a hard interruption guarantee for arbitrary blocking providers
or OS I/O. Provider installation is an explicit one-time Engine seam before
descriptor sessions; default local provider construction is lazy and off-lock.

This change supplies actual mounted local reads, not foreground demand/lease
scheduling, availability-index repair or live content activation. Native COW
writes, durable namespace and complete close/fsync/startup/shutdown fences,
root registration/GC and all original Wave 6 release gates remain required.
No persistent schema, canonical, wire or event encoding changed. Private
user_version stays 4; typed fixture activation remains isolated from user
workspaces.

## CCR-WP6.4-14: mounted missing-range demand and observed availability repair

A selected native range that fails with a verified provider's missing/corrupt
object identity can now register foreground demand on its worker's existing
content scheduler. Ready local reads keep their direct path. A worker-less
reader, an independent provider identity or a typed legacy reference retains
its original unavailable result; the existing legacy string/whole-object
hydration path is unchanged. No new typed legacy transfer format is invented.

The demand captures the session's immutable content event and entity, active
branch and checkout generation under the short Engine lock. Its reference is
the existing domain-separated event/entity reference, never a bare hash. Reads
use the stable local workspace UUID already used by save intents, allocating
its existing config row only on the first missing-range demand, off Engine.
Read `FileRevision.generation` records the captured workspace view generation;
the immutable reference additionally identifies the exact content event. This
is not a save-operation ownership generation or publication right. Reopen and
later native save intents reuse the same workspace UUID. No schema, canonical,
wire, event or capture encoding changes are introduced.

Only a currently granted shared reference with the selected root can start
network demand. Private observations can invalidate stale local availability
but cannot register a remote waiter. Each wait revalidates the reference grant
generation, sharing, handle selection, content event, branch and checkout
generation. Identical bytes in a newer content event do not authorize continuing
an old waiter. The final grant/selection checks linearize a successful wait;
already-ready local bytes keep their existing open-pin semantics. Existing
object-engine source authorization and receipt checks remain mandatory.

One demand covers at most one provider call (1 MiB). Its RAII lease remains
until that range is verified or the request fails. It does not impose whole-file
availability. Missing/corrupt physical observations clear only that object's
verified index bit, without changing receipt or permission evidence. A new
missing object, or an index that incorrectly became verified again, invalidates
that object and replaces the waiter lease so an earlier satisfied index cannot
hide later required repair. No new lease is churned merely for another poll.
Independent readers keep independent waiters/deadlines/cancellation while the
existing scheduler joins installations by native provider/object identity.

Admission charges a process-wide maximum of 64 native read waits before opening
a secondary connection or allocating demand state. Adapter-specific waiter slots
also reserve filesystem dispatcher capacity. A ready range consumes no wait
slot, including when all missing-range slots are occupied. Each accepted wait
holds one secondary handle; returned idle handles retain the existing limit of
eight. The pool's new nowait checkout sets SQLite's busy timeout before view
refresh, including on a newly opened connection, then restores the previous
timeout before reuse. SQL busy retries poll the request context and selection;
all SQLite/provider work remains off Engine. This extends the frozen Store and
PooledStore seams with explicit timeout ownership rather than inheriting a
five-second SQLite wait inside a shorter request deadline.

Adapter fences are checked while acquiring Engine and during SQL retries.
Native public range contexts link the caller and worker stop as weak-parent
cancellation children, affecting neither parent when a request ends. Content
state creation also rechecks stop under its installation mutex. These controls
bound cooperative waits, not uninterruptible OS I/O, filesystem sync or an
arbitrary blocking provider. Progress between required missing object identities
refreshes the adapter stall clock; total deadline remains fixed. No new threads,
retry daemon, inline transfer fallback or mounted write activation is added.

Complete native mounted COW saves/namespace/startup/lifecycle integration,
aggregate engine budgets and shutdown/soak qualification, retention/deletion
barriers, WP6.5/6.6 and all original Wave 6 release gates remain required.

Qualification exposed a legacy credit-return race as well: global stream
reservation could fail, then the sink could return its own last proof before
the runner inspected pending work. Interpreting that second observation alone
as no possible progress prematurely replaced a still-unfinished round's nonce.
The sink now increments a process-local credit epoch after dropping returned
proofs and before making its retiring count zero. The runner samples that epoch
before reservation; on refusal with no pending sink work, a changed epoch makes
it retry capacity before yielding. If no own progress occurred it still yields
for other scopes. The counter is readiness evidence, not lease authority or a
durable generation; no resource limit, round cap, wire identity or grant changes.

## CCR-WP6.4-15: save metadata transactions outside Engine

The headless content save coordinator now uses secondary metadata connections
for committed-checkpoint/receipt lookup, checkpoint selection, publication and
continuation. SQLite writer acquisition, the FULL commit and failed rollback
run outside Engine. Existing capture validation, exact receipt replay, content
pinning and private continuation/adoption remain mandatory. This is a prerequisite
for mounted COW writes; the mounted/legacy-session refusal remains enforced.

A save pool clones its registered worker's `SyncStorePool`, sharing the same
Arc and eight-idle limit. A standalone headless caller creates its own pool.
The existing four save workers and 64 outstanding-operation limit still bound
the coordinator; cloning does not create threads or increase those limits.
Zero-timeout checkout refreshes the secondary's active branch/generation, and
busy retries poll the accepted job's context. Returned handles restore their
prior busy timeout. Non-autocommit or quarantined handles are closed instead
of returning to the pool. Cooperative SQL waiting observes cancellation and
deadline; uninterruptible OS I/O/fsync is not forcibly interrupted.

Short Engine acquisitions check stop/switching, exact repository root, active
intent branch and checkout generation. They also reject a quarantined primary
connection or an open caller transaction: a healthy secondary must not escape
an outer transaction on the primary. After durable publication, Engine checks
the captured identity again before its existing event delta, activation and
observation update. A stale completion retains recovery state and the durable
receipt; a committed earlier branch cannot refresh/adopt into another branch.

Fenced receipt observation explicitly advances `EngineVitals.store_revision`
without SQL. The primary connection's `total_changes` cannot detect a secondary
commit; this explicit invalidation makes the existing status cell refresh after
the publication. Replayed observations may advance the counter again: it is a
freshness signal, not an exact count of authored events or durable authority.

A publication panic on a secondary cannot poison Engine. If rollback fails,
the connection is quarantined and closed, releasing its SQLite writer; retry
uses a fresh handle without duplicating the committed event. A successful
rollback permits normal connection reuse. Private Saved and
pending checkpoint data remain until successful adoption. No weaker durability,
new metadata batching, autonomous retry or close acknowledgement is introduced.

The present tests exercise readonly metadata through Engine and other private
writes while the save transaction is blocked. Other legacy primary metadata
mutations can still hold Engine while waiting for SQLite; short observation
metadata reads also remain under Engine. Headless notification capture is None,
so this stage does not qualify mounted notification correctness or the complete
independent-engine contract. Native COW callbacks, namespace/startup/lifecycle
integration, private-reader independence during FULL fsync, retention, aggregate
budgets, WP6.5/6.6 and every original Wave 6 release gate remain required.

## CCR-WP6.4-16: native existing-file COW adapter integration

`NativeWrites::install` explicitly installs a private journal and the existing
bounded save executor before any mount/session/handle exists. Installation
requires the existing isolated content-v1 fixture marker; no production path
activates or migrates a user repository. Ordinary repositories retain their
existing adapters. A registered worker shares its secondary pool; otherwise the
explicit headless fixture owns one. The four-worker/64-outstanding save limits
and eight-idle connection limit remain. At most 64 distinct entity preparations
hold keys; same-entity preparations join one key and recheck the installed file.

A writable existing descriptor keeps its immutable provider pin before open
is acknowledged. Off Engine, the factory captures a metadata save operation
whose branch, checkout generation, descriptor and content/alive parents must
still equal the open selection inside the actual SQLite writer transaction.
A same-byte newer content event fails this check before superseding any owner.
Private session creation/provider pinning is off Engine; final installation
revalidates the selected handle and descriptor. Readers and writers of that
entity then share one stable `SaveFile` through `Staged::Cow`.

WinFsp and FUSE route native reads, writes, append, constrained writes, length
changes and allocation-size semantics through this file outside Engine. WinFsp
uses its borrowed destination/source; FUSE keeps its 1 MiB buffer limit. Larger
borrowed requests use bounded pieces. A per-file gate orders mutable reads,
writes and explicit flush so a flush cannot pass a write already started, nor
can a multi-piece read mix private generations. Append chooses its offset under
the same file ownership. Other entities and Engine readonly metadata remain
available during save publication. Length is cached after private commit,
including a lost buffered-write reply, for metadata callbacks without journal
I/O; actual bytes still validate durable session ownership and sequence.

Native close/cleanup currently waits for the existing FULL checkpoint,
publication and continuation/adoption pipeline, including the burst-close
path. Explicit flush/fsync has the same durable local boundary and never waits
for a peer. WinFsp's void cleanup reports failures through the existing mount
error path; dirty private sessions/Saved intents remain retained. FUSE release
now runs on its bounded request worker pool, and O_TRUNC/size-changing setattr
prepare/truncate/flush native sessions outside Engine. No legacy whole-file
snapshot, spool copy or inline native journal I/O is used as a fallback.

Buffered/checkpointed epochs are process-local retention evidence, never wire
or publication authority. Only a successful private checkpoint marks the
current buffered epoch recoverable. A direct under-Engine retain refuses a
newer native buffer with `CONTENT_SAVE_REQUIRES_OFFLOCK_RETAIN` (fatal misuse),
rather than attempting private fsync under Engine or claiming it retained data.
The adapter's force-detach/release/lost hooks and forced worker stop checkpoint
native files outside Engine before legacy bookkeeping. Deadlines/cancellation
still cannot forcibly interrupt arbitrary OS I/O/fsync. Existing panic/accept-
error best-effort exit hooks are not newly qualified for native buffered data.

On explicit installation/reopen, the journal enumerates Saved records in
bounded pages and resumes at most 64 active-branch existing-file operations
before handles open. Their private bytes are visible immediately and keep the
Engine nonquiet until successful retry. Old committed receipts replay through
the existing coordinator without a second event. Inactive branches, a missing/
deleted entity or competing operations remain recovery-required with bytes
retained; namespace conflict resolution and autonomous startup retry remain open.

This integrates existing-file native COW callbacks, not all mounted save
semantics. WinFsp CanDelete/rename and open-source FUSE rename explicitly refuse
unsupported native open-handle namespace changes. Cleanup also refuses native
delete requests; void-cleanup delete semantics are not qualified. Durable native
creates, rename/delete/atomic replacement and
retained open-handle identity still require the namespace package. COW base
reads/preparation can retain an unavailable outcome and Saved data; connecting
their immutable base to authenticated foreground remote demand and qualifying
generic providers remain required. Automatic close coalescing, compatible
metadata batching, private readers during slow journal FULL fsync, complete
root registration/GC/retirement/storage progress, aggregate runtime budgets,
WP6.5/6.6 and every original release gate remain open. Schema/canonical/wire/
event encodings and private journal user_version 4 are unchanged.

Final regression qualification exposed a legacy native-download admission race:
an `objects.write` could meet the short per-family effect gate held by
`objects.want`, return local `WORKER_BUSY`, and retire an otherwise healthy
download round. Write admission now waits cooperatively outside Engine, lease
state and object gates, sharing the existing two-second ordering deadline.
Cancellation/worker stop abort the wait; an obsolete owned run is refused, and
`claim_run` still revalidates ownership under the acquired family gate before
byte work. Other family operations retain their immediate admission behavior.
No peer backoff, retry interval, authority rule or capacity limit is relaxed.
A controlled actual want/write interleaving holds that gate for 350 ms and
requires complete 4 MiB equality, one sampled live lease nonce and no source
backoff. A separate unit proves the admission deadline and worker stop leave
lease ownership empty. The Windows direct-image sample records two overall
download-job invocations while observing one live nonce. That counter includes
empty anti-entropy invocations, but this sample does not attribute the second
invocation or prove zero redundant rounds/bytes; that stronger claim remains
unqualified. The original test assertion is unchanged.

## CCR-WP6.4-17: authenticated native COW base demand

An optional process-local `BaseWait` belongs to the native save coordinator,
not the generic provider interface or serialized journal. Explicit
`NativeWrites` installation attaches it only for a registered worker; ordinary
repositories remain inactive. Adoption preserves the hook while using the
successor intent, and explicit Saved resume attaches it to the new worker via
weak ownership. Standalone sessions and independent providers retain their own
availability semantics.

On a typed native unavailable/corrupt base read, demand uses the intent's
captured content event, entity, workspace and immutable descriptor. The native
pin identity must match the worker provider. Short Engine checks fence branch,
checkout generation, switching/stopping and the stable private file operation;
secondary zero-wait SQL checks reference/root, grant generation and current
authorization before demand, throughout the wait and after verifying arrival.
Withdrawing/regranting the same bytes invalidates the original waiter. No bare
hash request or peer authorization bypass is introduced. The original provider
read or builder still verifies its exact bytes/shape after arrival.

Builder path errors carry a local base position. For missing structural paths,
rebuild returns and releases its buffer permits and all journal guards before
network waiting; Saved is revalidated before retry. These paths use a one-byte range and
the reserved structural class, which excludes data-chunk candidates. Partial
reads request their needed base ranges separately. A fully overwritten chunk
does not require its old bytes, even if those bytes are absent at both peers.
Unchanged chunk identities can remain unhydrated after publication.

Missing-base waiters share the existing global 64-reader admission limit.
Mutable mount reads reserve adapter capacity before waiting on their per-file
gate, including cached reads; capacity refusal remains immediate. Read stalls,
request deadlines/cancellation and worker/fence changes release the scoped
demand. Preparation uses its existing save deadline and fixed four-worker/
64-outstanding executor. Failed preparation retains Saved and private bytes.
No Engine, file-state mutex or journal guard spans a remote wait; the affected
native file's mutation gate still orders callbacks. Partial-data reads made
inside the builder currently retain its buffer permits while waiting; that
independence requirement is not complete.

This interim integration can wait for required base data/pages during fsync.
It therefore does not satisfy the original no-peer fsync contract. Required
hydration must occur before mutation acknowledgement, followed by a local-only
flush/fsync path. The authenticated preparation tests qualify the mechanism,
not that final callback/durability contract. No plan target is reinterpreted as
merely avoiding replication acknowledgement.

This does not qualify generic-provider native canonical rebuilding or legacy
conversion hydration. A newly published root's missing reused chunks may still
require peers to learn its new authorized reference; serving those chunks via
an older reference before metadata convergence is not newly qualified. Native
namespace/startup/lifecycle completion, batching, retention/GC, aggregate
runtime budgets, WP6.5/6.6 and every original release gate remain required.
Schema, journal user_version 4, canonical and wire/event encodings are unchanged.

The final transport regression also exposed want's immediate per-family gate
refusal retiring healthy pending-write rounds. Its snapshot and grant phases
now share the existing cooperative write-admission helper and one two-second
gate-wait deadline. Caller cancellation is checked on contention, and the
existing locked `claim_run`/grant checks and cancellation rollback still apply.
No Engine, lease-state mutex or object gate is held while waiting. Release and
other authority operations keep their previous immediate admission behavior;
no source backoff, ownership rule or queue/byte limit changes.

### CCR-WP6.4-18: pre-mutation preflight and strictly local fsync

This supersedes CCR17's interim network-in-fsync and partial-data buffer-wait
behavior. It does not weaken the authoritative plan's no-peer fsync contract.

Before native write/resize applies private bytes, a read-only canonical-shape
visitor selects the exact Working operation/sequence, prospective size and
overwrite range. It verifies only required existing manifest pages and old
byte gaps. Fully covered chunks and new zero ranges need no old bytes. It
does not construct new content hashes, install CAS objects or force a FULL
checkpoint. Existing input verification still hashes those inputs. The visitor
uses the builder's bounded cursor/scratch and three background buffer permits.

An unavailable native page/data read returns from the visitor before BaseWait,
releasing its cursor, scratch, buffer permits and journal guards. File state is
also released. Authenticated authority/deadline/cancel checks remain those of
CCR17. The exact Working selection is revalidated after arrival and again
under file state immediately before applying the mutation. A concurrent private
mutation makes that selection stale; the pending incoming bytes are not applied.
Adapter capacity is reserved before the per-file gate. Local preflight and each
bounded write piece also check request cancellation and the adapter fence.

Recovery and headless writes mark the session as requiring preflight of its
existing dirty paths. A successful native mutation clears the marker; partial
write failure keeps it. This marker is process-local, not a new durable format.
Legacy conversion remains local and may verify the whole old file; a fully
private replacement or truncation to zero does not require absent original
bytes. Remote legacy conversion remains unqualified.

Checkpoint reads and canonical preparation use local reads only and cannot
invoke BaseWait. Fsync retains a FULL Saved checkpoint before local preparation
and metadata publication. If a required local object disappears after an
acknowledged write, fsync reports unavailability without any demand-hook call,
retains Saved/private bytes, and can retry after local restoration. Successful
write/resize must permit fsync after the source peer is disconnected. No remote
replication acknowledgement is part of that durability boundary.

Evidence: [local-fsync-checks.json](../../test-evidence/wave-6/local-fsync-checks.json),
380 frozen inputs (`24b26c2c…`), strict Clippy on Windows/Linux, 418/425
reported main passes in 13 selected targets and three explicit real-kernel
parents/platform. The receipt child driver's ordinary early return is not
independent recovery proof; repeated runs/child executions are not extra cases.
The report records exact source/executable hashes, commands and individual
preflight/local-fsync measurements; the implementation record explains limits.

The current native namespace/lifecycle, complete retention/GC, aggregate runtime
budgets, structural admission after idle/saturation, WP6.5/6.6 and original full
release matrix remain open. No production activation, migration or CAS deletion
is authorized by these isolated mechanism tests. Journal version 4 and all
canonical/wire/event encodings remain unchanged.

### CCR-WP6.4-19: bounded structural idle polling

Structural preparation may receive a new missing-page demand after the prior
tree traversal became idle. Its empty-preparation delay is now 50 ms, separate
from foreground's 10 ms and background's unchanged exponential delay (up to
640 ms). Fixed workers, existing class/byte/queue limits, source selection,
authorization and cancellation are retained. No notification is required for
correctness and no background byte polling is accelerated.

`DispatchView` exposes a bounded per-group diagnostic snapshot of consecutive
empty preparations (capped at seven) and remaining delays by class. It performs
no provider/SQL I/O. A snapshot or polling interval is not an admission promise.
The explicit qualification scenario observes its own repository's idle phase,
removes a real local manifest page and requires new COW structural admission
within the original 250 ms while two actual background source reads remain
stalled. Repair/private bytes/local fsync and unchanged background completion
counts are checked. The original 639 ms scheduled-delay failure is preserved
with exact source and raw logs; the focused corrected Linux case passed.
Full frozen-source stage qualification is recorded in the implementation log.

Evidence: [structural-idle-checks.json](../../test-evidence/wave-6/structural-idle-checks.json),
380 frozen inputs (`cb68b611…`), strict Clippy, 419/426 reported main
passes in 13 selected targets and three explicit real-kernel parents/platform.
The early-return receipt driver and repeated/child runs are not independent
additional proofs. Exact source/executable hashes and source-archive readback
are retained. Final direct Windows/Linux admission was 94.2927/51.516882 ms,
with zero stalled background completions and successful repaired bytes/fsync.

This scenario does not replace arbitrary repository/source counts, aggregate
foreground/structural/background fairness, CPU/RPC budgets, outage/load tests,
the 30-minute soak or any other original release gate. Activation remains off.

### CCR-WP6.4-20: owned basic-info metadata and capture rebase

The Core `apply_content_save_basic` primitive accepts an exact registered
SaveIntent, a distinct mutation UUID, a complete BasicInfo register and a
private sequence/time-retention mask. Caller entity ordering is required.
The FULL WAL writer rechecks operation, workspace, branch/view and generation
ownership. A private proof exempts only the exact local basic-info event from
ordinary invalidation, while its capture is unselected/uncommitted. Ordinary
metadata and namespace operations keep their existing stale-save fences.

Basic event, bounded/checksummed receipt and capture/successor update commit
together. Exact replay returns the original receipt after later publications;
UUID reuse with changed request is refused. Receipt/capture/successor/selection
readback detects silently ignored, deleted or reverted writes and aborts the
whole transaction. Published events are immutable; metadata cannot reuse the
future byte event's identity. There is no provider/private-journal byte work.

An optional private capture policy binds write/change-time retention to one
sequence. Selecting older bytes is stale; later bytes expire retention. Exact
selection replay keeps clocks, and superseding selection atomically changes
the template before publication. Content/alive parents, descriptor and private
operation stay exact. Remote content competitors remain conflicts.

Save-in-progress is classified as contention, without failure/backoff
escalation. Receipt/continuation/owner/UUID integrity failures and invalid
internal timestamp-policy arguments are fatal in the pending operation/save
scope, retaining recovery bytes. They must not quarantine unrelated metadata,
object or network engines. Existing historical error classes stay unchanged.

This is a Core primitive, not complete native metadata integration. Native
callbacks must support unavailable-base metadata, failed selected saves,
per-handle Windows timestamp flags and crash-safe sequence binding. Attr-only
and partial timestamp requests must use fresh metadata/preconditions rather
than overwrite unrelated fields from a stale full-register snapshot. The
implementation record tracks these next steps and stage qualification.

Evidence: [owned-basic-checks.json](../../test-evidence/wave-6/owned-basic-checks.json),
381 frozen inputs (`6f40fb36…`), strict Clippy, 429/436 reported main
passes in 13 selected targets, including ten new ordinary Core parents. Three
actual child exits belong to one parent; repeated/child runs and the existing
early-return receipt driver are not independent additional proofs. Three
existing real-kernel parents/platform are regressions. Exact executable/source
hashes and every source-archive member were verified. Earlier silent-write and
classification failures are retained, not omitted or relabeled as successes.

The additive metadata receipt table needs retention/storage qualification.
The optional capture field is readable by new code from old captures; old
strict readers cannot decode new policies. WP6.6 compatibility fencing remains
required. Journal version 4 and canonical/wire/event encodings stay unchanged;
activation, migration and live CAS deletion remain off.

### CCR-WP6.4-21: native mutation clocks and private journal v5

Working/Saved seal the actual mutating handle's write/change-time disable mask
and mutation milliseconds at the existing NORMAL/FULL boundaries. Explicit
automatic native writes differ from absent legacy/headless policies. Saved
recovery restores its policy/clock independently of unacknowledged newer bytes.
Ordinary writes gain no extra FULL checkpoint. Atomic sequence/time/length
mirrors support local metadata observation without journal I/O under Engine.
Only an actual new private sequence changes cached timestamps; partial commits
remain visible despite a lost reply, and failed preflight cannot reset times.

Core selection uses the mutation clock, not fsync clock, combining the saved
handle mask with same-sequence explicit retention. Captures/successors retain
base times until the selected byte policy determines their update. A recovered
older capture can add basic info without parenting a new remote competitor.
The writer test alternates two handle masks and three publications and checks
exact basic-info equality across delayed fsync; it does not call the setter.

Private journal version 5 is required before new policies are stored. A valid
v4 inventory upgrades atomically under FULL WAL without rewriting records,
payloads, seals or checkpoint identities. Unknown/older incompatible layouts
still refuse to open. Actual process exits before/during/after header commit
exercise rollback/retry; this does not replace the full power-loss matrix.

Explicit compatibility qualification executes the exact attested CCR20 v4
reader, requires upgrade refusal, and verifies unchanged v5 records/header and
old image hashes. It does not establish legacy executable workspace isolation,
capability negotiation, launcher fencing, downgrade/export or production
migration. Those remain WP6.6 requirements; activation stays off.

Native set-basic callbacks, checkpoint-bound rebasing of failed selected saves,
partial-field freshness and timestamp-disable/reset requests still need full
integration. No existing namespace, lifecycle, retention, aggregate resource,
build-performance or release gate is waived by these timestamp primitives.

Evidence: [native-times-checks.json](../../test-evidence/wave-6/native-times-checks.json),
381 frozen inputs (`b57d63f1…`), strict Clippy, 433/440 reported main
passes in 13 selected targets, three existing kernel regressions and one
actual-v4-reader parent/platform. Child exits, repeated runs and the ordinary
early-return receipt driver are not independent additional proofs. All source
members, 26 new executable images and the two old reference images were hashed
and checked. The visibility case reads cached handle/session basic info, not
every path/inode getter; those renderers and invalidations remain required.

### CCR-WP6.4-22 — native attribute rendering and cleanup boundary

In the isolated native path, FUSE stat uses typed descriptor length before
open and native COW session basic info while a private session exists. The
getter performs no provider/CAS work to determine a typed file's declared size.
Metadata queries must expose an acknowledged private write's timestamp without
publishing unrelated uncheckpointed bytes.

Read-only native cleanup/close cannot create a new durability promise for
another handle's buffered writes. It may finish an already FULL Saved recovery
or failed save. The recoverability decision is made under the per-file mutation
gate and remains protected through publication/adoption. WinFsp cleanup, FUSE
flush and common release use this boundary. Explicit fsync/FlushFileBuffers and
volume flush remain durability points, including fsync through a read-only
handle; writable native close retains its existing FULL recovery promise.
Legacy save behavior is unchanged.

The implementation record preserves the actual getter/cleanup failures and the
first skip's unchanged recovery-test failure. The local path/handle scenario
does not establish remote metadata activation ordering, all directory/cache/
notification surfaces, native set-basic or native namespace integration. No
Wave 6 gate or production activation requirement is waived.

Evidence: [native-attrs-checks.json](../../test-evidence/wave-6/native-attrs-checks.json),
381 frozen inputs (`781885a4…`), strict Clippy, 435/442 main harness cases
in 13 selected targets, three actual kernel parents and one actual-v4-reader
parent/platform. The unchanged crash-recovery parent and the new concurrent
cleanup parent passed. Source archives and executable hashes were verified;
default ignores, early-return drivers and child/repeated invocations retain
their stated counting limits.

### CCR-WP6.4-23 — exact resulting clocks in private v6

One native byte revision records its actual resulting write/change FILETIMEs,
the mutating handle's mask and mutation milliseconds. A disabled field keeps
the prior visible value, including an earlier buffered write; it cannot be
reconstructed from the final mask and one mutation clock. Working, FULL Saved,
adoption and recovery carry the exact pair. Core uses the recorded fields with
same-sequence explicit metadata retention taking precedence. Ordinary writes
keep their existing NORMAL transaction; no new FULL boundary is introduced.

Length/sequence mirrors remain atomic. Clock caches use short memory-only
mutexes with no guard spanning journal/provider/fsync/Engine work. Recovered
native sessions expose Saved clocks before publication. Native set-basic and
remote activation ordering remain separate unfinished integration requirements.

V6 recognizes byte-compatible headless v4/v5 records and upgrades only the
header under FULL WAL. Native v5 records are refused with upgrade-required:
they need matching-reader recovery/export because their missing per-field
history cannot be inferred safely. This is conservative development-format
handling, not a complete production migration/export path. Actual attested
v4/v5 reader compatibility is qualified separately; full WP6.6 remains required.

Evidence: [native-clock-checks.json](../../test-evidence/wave-6/native-clock-checks.json),
381 frozen inputs (`5be800d3…`), strict Clippy, 438/445 main harness cases
in 13 selected targets, three kernel parents and two actual-reader compatibility
parents/platform. The explicit old v5 native fixture is refused without changing
records/header or physical DB/WAL hashes. Cargo builds without running tests;
the main counts come from one hash-attested direct run. Default ignores,
ordinary helper drivers and child/repeated invocations retain their stated
counting limits. Source and executable identity checks passed.

### CCR-WP6.4-24: partial native basic metadata (development)

`ContentBasicUpdate` carries only requested fields. Core reads current basic
metadata under its writer, overlays those fields and commits one metadata
event with an exact request receipt. A FULL private checkpoint binds the
unpublished byte revision; an unpublished selection can be rebound without
publishing or rewriting its byte event. Final transaction readbacks must
verify capture, selection and receipt. Explicit write/change times and
attributes remain effective for that exact byte revision and expire after a
later byte mutation. Ordinary local metadata outside this command still fences
save ownership; remote alternatives remain causal competitors.

The native mutation gate orders metadata with writes/truncates/saves. Before
metadata, bounded executor adoption may finish a previously committed save,
but adoption-only work cannot checkpoint or publish newer private bytes.
Reply loss uses read-only exact receipt reconciliation. No receipt means the
failed command is discarded, never automatically reapplied. An expired reply
after commit keeps pending observation until the next mutation can reconcile
it. Core durable capture/receipt state supplies restart recovery even though
the runtime observation is memory-only.

Native basic observation includes exact clocks, the last byte revision and
whether unpublished changes remain. Owner fence increments are not byte
revisions. Recovery and activation apply the canonical published/adopted floor
before overlaying private clocks. A read-only cleanup may finish an orphaned
FULL Saved revision, but must not publish that revision while an active writer
remains. Metadata-only temporary FUSE handles release without invoking save.

Current WinFsp op14 and threaded FUSE SETATTR use this coordinator. Focused
actual-kernel checks cover set_times on both platforms and chmod on Linux,
with no canonical byte publication before fsync. FUSE size requests retain
their earlier immediate save boundary; buffered truncate and related
performance gates remain required.

Actual WinFsp 2.1 disable/reset is **not qualified**: a retained kernel probe
fails because the driver converts -1 to zero before SetBasicInfo, erasing the
distinction from unchanged. See the [driver implementation](https://github.com/winfsp/winfsp/blob/v2.1/src/sys/fileinfo.c#L1223-L1230).
The separate ignored Windows probe retains its original failure assertion.
Direct native decoder tests cannot substitute for that required OS contract.
Production activation stays off; no waiver or heuristic inference from zero
is authorized. Complete remote/cache/namespace/lifecycle, batching, aggregate
resource, build/load/storage and production compatibility gates remain open.

### CCR-WP6.4-25: native open-handle truncate stays buffered

The native FUSE SETATTR size branch no longer invokes an immediate byte save.
A size change through an open handle waits for fsync/close to reach its durability
boundary. A path-only truncate still closes its temporary native handle before
success. Legacy size handling is unchanged. This supersedes CCR24's interim
native size limitation, without qualifying batching or build performance.

The actual WinFsp/FUSE kernel parent now installs a failing private checkpoint
hook during File.set_len, requires no hook hits and no canonical publication,
then appends/fsyncs and verifies exact bytes, reopen and the derived-state oracle.
An actual pre-fix Linux failure and exact source are retained. Targeted proof is
in [buffered-truncate-checks.json](../../test-evidence/wave-6/buffered-truncate-checks.json):
25 ordinary native parents/platform,124 Linux library parents and one explicit
kernel parent/platform, with stable image hashes and exact archive readback.
The earlier full selected-target report is not relabeled at this new source.
The required failing WinFsp time-policy gate and all other Wave6 release gates
remain open; activation stays off.

### CCR-WP6.4-26: live private reads through short WAL snapshots

A private journal has up to four lazy read-only SQLite connections, each with
512 KiB page-cache limits. Live reads use short transaction snapshots for
headers, map/payload extraction and final validation. No snapshot/connection
permit spans provider or network I/O. Owner/sequence fences, map authentication
and payload verification remain active; a changed owner can still produce
scoped stale-read contention. This is not universal adoption/read/GC closure.

SaveFile mirrors its active session Arc through a short memory-only mutex so
checkpointing under file state does not prevent read selection. Durable adoption
finishes this mirror without a fallible deadline acquisition. Native reads keep
the byte-mutation gate, which writes/truncates acquire after ownership. Save and
metadata durability work retain ownership without taking that byte gate.
The existing started-write/read ordering and adapter-capacity regression stays.

Two real FULL-WAL regressions stall before checkpoint commit for 1,500 ms,
requiring independent-file and native same-file reads under 300 ms, correct
private bytes, no canonical publication and a still-stalled writer. Pool tests
exercise capacity, cancellation/deadline/reuse and error/panic cleanup without
poisoning writer authority. [private-reader-checks.json](../../test-evidence/wave-6/private-reader-checks.json)
records all samples and exact source/image/archive identities:462 Windows/470
Linux ordinary cases across15 targets, plus separate kernel/compatibility parents.
The actual Windows disable/reset probe still fails and remains required.
Physical slow-disk, full load/resource/retention, every ownership interleaving,
production activation and the complete release gates are not established here.

### CCR-WP6.4-27: owned existing-file rename

`apply_content_checkpoint_rename` binds a raw rename request to an owned save
intent and its exact FULL Saved checkpoint, or a clean operation. It commits
only namespace metadata, its receipt and the updated capture/selection under
FULL Core WAL. Operation identity and byte/alive parents remain exact. The
opaque owner exempts only the source; a replaced Core entity retains ordinary
save-generation fencing. Ordinary local namespace mutations still fence saves.

The exact receipt replays before current-owner/path resolution, so a later
rename, parent-directory move or byte publication cannot cause an old reply
retry to move the file back. Silent capture, selection or receipt changes must
fail the final readback and roll the namespace transaction back. Remote location
and content alternatives remain unreviewed. A changed same-sequence checkpoint
and the reserved byte event identity cannot be accepted as a rename.

Native rename takes adapter capacity before the file gate, adopts only already
committed bytes, and anchors current private bytes without publishing them.
Pending observation uses read-only receipt lookup; absence never retries an
uncommitted move. Later write/truncate/save/basic changes reconcile first.
Activation updates name/parent/location revision on the stable private file,
keeping content/alive ancestry. Mounted Windows uses bounded root invalidation
without retaining a whole pre-commit directory projection.

WinFsp op12 and threaded FUSE12/45 support existing-file rename. Actual kernel
roundtrips read private bytes via the new name and keep the open operation,
then write/fsync/read/reopen with no byte publication caused by rename. Native
replacement of an existing destination is still explicitly refused. A raced-in
destination yields collision through Core no-replace, never implicit overwrite.
This is a required continuation toward full atomic replace/open-handle lifetime,
not a waiver. Native create, unlink/delete-on-close, complete lifetime/recovery,
root retirement, build/load/resource and production compatibility gates remain.

### CCR-WP6.4-28: retired destination save ownership (Core/private foundation)

`apply_content_checkpoint_replace` requires the source's owned FULL checkpoint
(or clean intent) and the exact destination intent/checkpoint. It revalidates
both owners and the resolved destination under one FULL Core writer. Namespace
replacement fences the old destination, then atomically installs its successor
and an independently checked namespace handoff. Raw request/event receipts and
both captures/selections/owners are read back before success. A same-name race,
reserved byte-event UUID or silent continuation write cannot acknowledge a move.

The retired capture keeps the base content/content parent, takes the removal
event as its alive parent and uses a distinct event-identity domain. Its byte
publications and later successors carry `alive=false`; they retain historical
content/admission roots without restoring the removed name. Ordinary local
restore/metadata still fences publication. Earlier published successor replay
recognizes a namespace transition rather than requiring a nonexistent byte
publication of the replaced generation. Owned basic updates may target this
verified retired entity and cannot make it alive. Newer source/destination
checkpoints use the same transactional clock/policy selection as byte saves.

`NamespaceContinuation` is opaque Core proof, queried read-only after restart
or a lost reply. It is not a byte receipt or deletion authority. Private adoption
uses FULL WAL, rotates the owner and records an exact replay marker. It preserves
all Working/Saved/pending maps, base visibility, lengths, native clocks and byte
sequences, including newer buffered bytes. It neither substitutes the base nor
prunes patches. Old handles are fenced; a lost private reply can recover the new
handle without repeating namespace work or overwriting later buffered bytes.

Private journal v7 explicitly fences earlier readers. Headless v4/v5 and all
exact-clock v6 records can upgrade by an atomic header change with exact record
identities; native v5 clock ambiguity still requires explicit matching-reader
recovery/export. Reference v6 executables are preserved from CCR27 before new
builds. This private fence does not replace the required actual legacy workspace
fence, production launcher/migration/export or downgrade rehearsal.

Native replacement remains refused until dual-file ordering, handle observation,
lost-reply reconciliation and automatic recovery of retired Saved records are
integrated and qualified on actual WinFsp/FUSE mounts. These remain mandatory,
as do native create/unlink/delete-on-close, complete lifetime/remote/cache
behavior, root and receipt retirement, batching, aggregate budgets, matched
build/load/storage, production compatibility and all Wave6 integration gates.
Content activation stays off; no physical CAS deletion is authorized here.

Final scoped proof is [retired-destination-checks.json](../../test-evidence/wave-6/retired-destination-checks.json):
393 frozen inputs,487 Windows/495 Linux ordinary cases across16 targets,
plus three actual kernel and three actual-reader compatibility parents/platform.
All selected image hashes stayed stable. Ten new ordinary namespace/retirement
parents include one nine-exit recovery parent. The source/error-code audit and
strict release Clippy passed. Actual v6 writer recovery/upgrade preserved its
exact checkpoint/clocks/bytes; actual v4/v5/v6 readers refused7 unchanged.

The original250ms structural gate was missed once on Windows (251.2949ms);
that exact failure remains archived despite later passing repetitions and the
final94.9796ms sample. Broader scheduler/load stability remains required.
The current-image actual Windows time-policy probe still fails and remains a
required driver-contract gap. Neither is waived by this headless foundation.
Native replacement remains guarded pending the required adapter/lifetime/recovery
integration and all other Wave6 release gates.


### CCR-WP6.4-29: native replacement and retained destination handles

This sequential integration extends CCR28's Core/private foundation to existing
typed native files. Acquire adapter capacity before either entity wait; acquire
source/destination entity keys and adapter mutation gates in sorted entity order.
Promote a read-only/unopened typed destination before the dual-file ownership
phase. FULL checkpoint both operations and commit the exact destination proof;
rename does not publish either byte revision. An absent observed destination
uses Core no-replace, so a raced-in canonical entity cannot be overwritten.

Observe `lookup_visible` and reserve the branch/parent/folded-name together under
Engine before off-lock durability. The memory map holds at most SAVE_QUEUE64
reservations, identified by operation UUID; Drop removes only that exact owner.
Deferred creates, immediate creates and legacy adapter renames reject a reserved
destination promptly. Unrelated names remain available. The reservation does
not substitute for transaction validation: `NamespaceTarget` captures the parent
identity, and `apply_content_checkpoint_rename_at` revalidates it under the Core
writer. Exact receipt lookup includes that observation even after the parent
moves. The request's optional omitted `expected_parent` preserves older request
serialization; no historical event/receipt is rewritten. No schema/wire/private
format increment is introduced by this extension.

Destination namespace checking is marked before the Core command. Its next
mutation reconciles a namespace handoff independently of source observation.
A cached previous intent survives lost private adoption replies; adopt memory
immediately, then retry Engine observation without repeating the private rebase.
Only an unpublished Saved checkpoint participates in handoff equality: a clean,
already-published Saved record is retained unchanged. Byte reads with a stale
private owner reconcile and retry once. Normal private reads do no Core lookup.

Startup accepts an unnamed Saved operation only with Core retirement proof,
adopts its private continuation, installs a dirty reference-zero native session
and queues bounded background retry. No canonical preparation is performed under
Engine as an unavailable-queue fallback. Existing saver backoff retains failures;
abandoned source operations are also swept. This reuses the historical saver
queue and does not qualify final global queue/shutdown/byte budgets.

FUSE retained open inode metadata reports nlink0; fd-originated setattr without
FATTR_FH binds an existing open handle for the unnamed inode. Unsupported STATX
must return ENOSYS before checking dead namespace liveness, enabling GETATTR
fallback. WinFsp's SupportsPosixUnlinkRename is true only when NativeWrites was
explicitly installed in an isolated fixture. POSIX flags are forwarded; legacy
fallback rejects POSIX rename. Ordinary unlink/delete-on-close is still rejected
before disposition/open ACK. Cleanup of a destination already removed by rename
requires Core retirement proof and durably saves without deleting the new path.

Final scoped proof is
[native-replacement-checks.json](../../test-evidence/wave-6/native-replacement-checks.json):
395 inputs frozen at
`39ed1695f6be2e20560819d34cac6f5dbaffa907ef97deafa8e4c83c5e62f656`,
strict release Clippy/format/diff checks,501 Windows/509 Linux ordinary cases
across16 targets/platform, plus four actual-kernel and three actual-reader
compatibility parents/platform. All selected executable hashes stayed stable.
Fourteen new ordinary parents include13 runtime cases (one drives nine actual
exit86 children) and one Core observed-parent case. Children/repetitions are
not extra unique passes. There are20 Windows/19 Linux default ignores.
The initial frozen pending-name test failure on both platforms is archived;
its fixture now settles the legacy save before raw Core deletion, matching the
adapter's required ordering. Collision/reservation assertions remain unchanged.
The current-image Windows time-policy probe still fails and is separately
archived; no passing count or activation waiver includes it. Earlier observed
Windows structural251.2949ms remains a broader stability/load gap despite final
93.3811ms Windows/54.622432ms Linux samples under the unchanged250ms bound.
Native create, legacy migration, unlink/delete-on-close, full lifecycle/remote/
cache/GC/retirement/storage/global-budget/build/compatibility/integration gates
remain mandatory. Production activation stays off; the full Wave6 goal is active.


### CCR-WP6.4-30: typed native file creation and initial save ownership

In explicitly installed NativeWrites fixtures, new regular files use canonical
typed empty content from creation. NativeWrites installs/verifies and pins that
single empty root outside Engine. `ContentCreateRequest` binds independent
creation mutation/entity/byte-operation UUIDs, branch/view generation, observed
parent/path and initial basic metadata. One FULL Core writer commits the kind,
location, alive, empty descriptor and basic fields, durable admission, initial
byte-owner capture and sealed exact receipt in `content_save_create_changes`.
No private-journal/provider byte work runs under that writer. The existing save
capture logic is factored into a transaction-only helper; selection and fencing
semantics for existing files remain unchanged.

Read-only creation receipt lookup verifies exact request/event/intent identities
and raw immutable event payload. Replay is checked before current parent/name
resolution and cannot recreate a subsequently moved/deleted file. The receipt
is not current byte-owner authority. New writes still require the usual owner
and generation checks. Nil/reused/reserved identities, wrong parent/view, name
collisions and silently ignored/deleted capture/owner/receipt writes fail before
creation ACK and roll back the namespace event with ownership.

Runtime creation reserves adapter capacity, the branch/parent/folded-name and
new entity key before durability outside Engine. FUSE binds its original inode
parent even if the path changes before reservation. Private session creation
follows Core commit; recheck ownership after private preparation before attaching
a SaveFile/handle. A failed or expired reply may leave the canonical empty name,
but cannot invent a successful open or silently retry an absent receipt. An
already committed receipt may reconcile the same command without duplicate
creation. Existing read-only empty selections can share the attached session.
No bytes were acknowledged before this attachment.

WinFsp regular-file create and threaded FUSE MKNOD/CREATE use this coordinator.
Initial attributes/times/POSIX mode are part of creation, avoiding a later
ordinary metadata setter fencing its new byte owner. A creating writable handle
can retain access to its newly created readonly file; later writable opens obey
readonly permissions. Writes remain buffered; native fsync and writable close
retain FULL Saved/canonical guarantees. Empty clean close does not manufacture
a byte publication. This immediate durable empty namespace event is stronger
than the legacy deferred-create visibility guarantee; compatible durability
batching and measured build budgets remain required. Legacy raw Engine/Core
creation is retained for the default non-activated path and old fixture seams.

Initial diagnostic release proof at399-input source
`27246339e7ca79d7b832aa7b05ca1077eec44ea737583ebc4a498d01002e6fa6`
passed Linux4 Core creation parents,8 runtime parents (including nine actual
exit86 children) and an actual FUSE create/close/replacement kernel parent.
Strict Clippy passed on both platforms. That source later failed the valid-capture
audit below; its earlier passes remain diagnostics. The corrected final source
and image qualification are recorded below. This does not establish production activation, legacy promotion/migration, native unlink/
delete-on-close, full namespace/remote/cache/lifetime/GC/root/create-receipt
retirement, global budgets/batching/shutdown, cache/change journal, build or
compatibility/integration release gates. All remain mandatory.


The additional valid-capture audit found that intent/shape equality alone was
insufficient: a valid sealed capture with a changed access time could acknowledge
creation at both capture-insert and receipt-insert boundaries. Exact negative
probes and the initially passing513/521 matrix are retained in the implementation
record. The helper now returns and checks the complete expected Capture, and
creation compares it again after receipt insertion. A permanent fifth Core
parent covers both valid substitutions. The runtime owner-fence parent also
covers a readonly open before private-session attachment, and the kernel parent
creates its manifest under a new directory. Corrected399-input source
`7c15299df8e908600e49f93eb310a816f1ed86e4ac3d70525b8f5f445a9fc150`
passed the full scoped qualification; earlier source272 results remain diagnostics.
Directory namespace mutation under Engine, receipt/root retirement and every
other original Wave6 budget/performance/compatibility/integration gate remain
mandatory. No physical CAS deletion or production activation occurred.


Final CCR30 proof is
[native-create-checks.json](../../test-evidence/wave-6/native-create-checks.json):
399 frozen inputs at the corrected7c15299d identity above, strict release Clippy,
format/diff checks,514 Windows/522 Linux ordinary cases across16 targets/platform,
plus five actual-kernel and three actual-reader compatibility parents/platform.
Thirteen new ordinary parents compared with CCR29 are8 runtime cases (one drives
nine exit86 children) and5 Core creation cases. There are22 Windows/21 Linux
default ignores. The explicit probes against the preserved old and corrected
optimized Core images reproduce then reject both valid-capture substitutions;
these are separate probes, not additional ordinary case counts.

The exact current-image Windows time-policy probe still fails and remains
required. Final structural samples92.5753ms Windows/50.920146ms Linux meet the
unchanged250ms scenario bound; earlier observed251.2949ms remains an aggregate
stability/load gap. No bound, assertion or original release gate was waived.
Creation/namespace/root receipt retirement, legacy promotion/migration, native
unlink/delete-on-close, directory metadata ownership outside broad locks,
complete lifecycle/remote/cache/GC/global-budget/batching/shutdown/cache/journal/
build/compatibility/activation/export/downgrade and the full platform/peer/outage/
storage/power-loss/load/soak integration scope remain mandatory.
