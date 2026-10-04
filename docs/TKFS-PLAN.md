# TKFS architecture, implemented PoC and acceptance plan

Updated 2026-10-03 to the user-approved direction: a singular distributed
filesystem with source control, where a branch **is a writable in-place worktree**.
Shared-branch saves propagate directly between devices without checkpoint/merge.

The previous 101,897-byte plan, including unrelated research/user notes, is
preserved unchanged in [TKFS-PLAN-2026-10-02-LEGACY.md](TKFS-PLAN-2026-10-02-LEGACY.md).
Its SHA-256 is `dd00e62cd9e142a4b71e0715d9e77cff7476e6e72d680be85ce4aff1323d6fde`.
The old central ref service, private overlay, read-only shared trunk and automatic
text-merge defaults are superseded by the contracts below.

## Architecture and implemented scope

One Rust runtime owns each device's SQLite database, whole-file SHA-256 objects,
shared local write staging, branch selection and real Windows WinFsp mount.
Devices/repositories/branches/files/events/requests have distinct stable identities.
State is on native storage outside the mount. One selected branch/mount per
runtime is supported; simultaneous same-path isolated agents are later work.

```text
Windows applications -> WinFsp C shim -> Rust runtime <- thin CLI capability RPC
                                           | local SQLite/CAS
                                           <-> encrypted TCP <-> paired device
```

`src/core.rs` owns storage/causal projection/conflicts/checkpoints; `src/runtime.rs`
owns staging, RPC and replication; `src/mount.rs`/`native/mount.c` own the real
WinFsp boundary; `src/main.rs` is the launcher/thin CLI. `src/backend.rs` supplies
a verified immutable-object interface and an explicitly invoked native local
test-bucket adapter. No cloud bucket/provider adapter or central ref service is
required. Direct peer replication is the chosen minimal metadata transport.
Both configured devices may write a shared branch. No arbitrary last-writer
ownership takeover or owner lease exists; future ownership must define explicit
scope, generation, handoff and fencing.

## Causality and conflicts

An immutable semantic event contains ID, device, branch descriptor, provenance
timestamp, observed dependencies and atomic changes. Each change addresses a
stable file ID and a content/location/kind/existence register, recording its exact
revision parents. Dependencies gate complete event activation and namespace
prerequisites; register ancestry governs supersession. Missing parents remain
in a durable incoming inbox and activate on catchup/restart, outside the committed
journal, outbox, acknowledgements and local write dependency frontier. Known parent
links must match branch, entity and register. Invalid children/cycles and their
dependent descendants are quarantined; valid later parents/unrelated branches
continue. Startup evacuates legacy incomplete journal events into that inbox
without deleting their payloads/objects. Duplicate events are idempotent;
different payloads under one ID and known dependency cycles are rejected.

Causal ancestors are removed **before** concurrent `(device UUID,event UUID)`
tie-breaking. Receipt order and wall clocks never choose a winner. All alternatives,
bases, provenance and immutable objects are retained. Pairwise conflict IDs and
historical DAG derivation prevent a successor/correcting rename delivered first
from hiding earlier concurrency. Equal-valued concurrent heads retain ancestry.

Viewing a canonical winner never resolves a conflict. Explicit resolution lists
the exact reviewed conflict IDs and a selected revision, parents their union and
persists a resolution receipt. Unseen concurrent revisions still compete. Automatic
text merging is deferred. Content/existence/same-file rename register resolution
and displaced-namespace recovery are implemented. Recovery moves a stable entry
to a free visible path while reviewing exactly the supplied namespace/location
records. Unseen contenders remain retained; deleted entries first require explicit
existence resolution. Historical namespace records derive from competing topology
witnesses in dependency cones of historical ancestors and same-name competitors,
closed transitively without coupling unrelated siblings, including arbitrary-width
concurrency rather than only pairwise cuts. Unchanged TRUE presence chains with
no tombstones add no witnesses; with tombstones, retain rank extrema per unbranched
presence segment and causal relationship to topology/false facts. Redundant review
parent edges are reduced only in the analysis graph; actual journal parents and
all content/existence alternatives remain immutable and inspectable. A small-DAG
exhaustive causal-cut oracle and reversed replay verify these presence witnesses.

The cap is 4,096 competing witness antichains of width at least two **per namespace
dependency cone**. Empty/sequential cuts and unrelated histories do not consume
one global history budget. A topology-contended commit exceeding the cap fails
before acknowledgement and retains existing records. Both the parent/delete/
child/correction case and three-origin cycle followed by an ordinary correction
pass all 24 delivery permutations/restart. The explicit witness bound limits pathological topology contention;
these tests are not a formal proof of every namespace graph.

## Namespace matrix

| Case | Visible policy | Durable preservation |
|---|---|---|
| Rename vs edit | Independent location/content registers combine on stable ID | Original/edited bytes and location ancestry |
| Concurrent rename | Causal ordering, then stable location winner | All locations/bases |
| Delete vs edit | Edits assert existence from their observed base; true/false heads conflict | Tombstone, contents and authors |
| Deleted parent vs descendant edit/create | Invalid descendant suppressed | Entry/events/objects plus parent conflict |
| Same-name/case-key creation | Stable entry-ID namespace winner | All placements/registers |
| File/directory collision | Same namespace policy; no silent conversion | Both kinds/contents |
| Concurrent directory cycle | Deterministically suppress invalid paths | Cycle/parent records and locations |
| Temp-file replace | One atomic move+tombstone event | Both immutable file versions |

Local operations reject invalid names/parents, nonempty-directory deletion and
direct cycles. Names preserve spelling with version-1 NFC plus Unicode lowercase;
this is not full Windows ordinal-casing equivalence. Hidden contenders are
inspectable and recoverable through the CLI; broader interactive recovery UX
remains future work.

## Save durability, handles and checkout

Local handles to one entry share staged bytes. Explicit flush publishes one
complete revision and continues a long-lived handle from that revision.
Cleanup/close attempts publication too. Buffered write alone is not a durability
promise. Windows cleanup cannot return failure: failed publication logs and sets
runtime health; explicit flush returns errors.

Publication flushes a unique same-volume temporary object, installs its immutable
hash name with Windows write-through rename, then atomically commits journal,
current state, conflicts and outbox using SQLite FULL-synchronous WAL. RPC receipt
and mutation share the outer commit. Orphans before SQL commit are harmless and
retained. Startup verifies all referenced objects, including losers, and rebuilds
from the journal. Missing/corrupt content fails clearly before serving/applying it.

Local causal dependencies use the accepted branch frontier, not every journal ID.
An ordinary content/existence successor of sole heads updates the projection
incrementally in the same SQLite transaction, retaining all older records. Generic
appends derive only newly introduced register pairs; restart/repair derives the
whole historical set. Linear register chains skip pair enumeration; branching
register analysis uses indexed reachability. Inbox activation waits for committed
prerequisites and batches a received frame behind one SQL durability barrier,
with per-event savepoints and no acknowledgement before the outer commit.

Checkpoint requires no live contexts and captures one coherent complete projection
under the runtime writer barrier. It is separate from each file save and is not
an application transaction across several saves. Snapshot objects/IDs/messages
are retained locally. `history` lists them; `restore` copies the visible snapshot
into a fresh private branch preserving file IDs, without overwriting current work.
Checkpoint diff, cross-device checkpoint labels/history and branch merging remain
subsequent source-control work.

Checkout verifies target content, refuses live contexts, fences new opens, updates
durable selection/generation with its receipt and remounts at the same path to
discard kernel caches. Flushed dirty edits stay on their original branch. Open
writers and retained Windows mappings were actually refused in the harness.
Cwd/watchers may block checkout; use an outside shell and `--runtime`. Remount
failure leaves an explicit unavailable view; restart from the durable selection.
Every remount fault boundary still needs qualification.

Read-only handles refresh on peer changes when there is no local writer/dirty
staging. Open writers retain their actual
content/existence bases, preserving concurrent remote alternatives. Old editor
buffers that close/reopen hide their real original base; TKFS cannot infer it and
captures the newly opened handle's visible base. Reload/cooperation is required.
WinFsp disables file-data caching on open and emits peer change notifications,
queued for retry on failure. Actual FileSystemWatcher create/change/rename/delete
and an existing read-only handle pass the mounted harness. Rename notifications
use old-path deletion/new-path creation, rather than a paired Renamed callback.
Broader IDE/build-cache
integration remains unqualified. Arbitrary mapped writes and power-loss/controller
durability are unproven; tests qualify acknowledged flushes across process crashes.

## Transport, status and privacy

Two explicit devices use real TCP with AES-256-GCM encrypted/authenticated frames,
random nonces, checked sender/receiver UUIDs and request-bound responses. A fresh
32-byte key comes from `TKFS_PEER_KEY`; the peer secret is never persisted/printed.
This is trusted-device pairing, without independent signing, PKI, forward secrecy,
revocation or multi-user ACLs. Network I/O runs outside SQLite transactions and the
runtime lock. One-second retries plus `sync` provide reconnect catchup.

Protocol version 2 bounds each page below the 64 MiB encrypted-frame limit.
Large atomic publications can span pages: verified objects and event manifests
are durably staged, with no partial branch/event visibility or acknowledgement.
Locally generated event payloads are limited to 8 MiB; files/objects to 16 MiB.
Durable outboxes, verified receive-before-ack, inventories, peer-scoped inventory
caches and idempotent replay survive reconnect/crash/ack loss. Status distinguishes
unflushed staging, local durability, queued/acknowledged publication and last
roundtrip catchup, plus pending/quarantined incoming counts. Catchup requires no
incomplete incoming events and is knowledge at that exchange, not a global barrier.
Authenticated accepted-event inventories are durable receipts on both request and
response paths. Inventory replacement and reconciliation of the two-device outbox
are one SQLite transaction; withdrawn peer events are queued again. Object-only,
incomplete, quarantined and private data are excluded from event acknowledgement.
Current status also requires identical shared inventories, empty outgoing/incoming
queues, no unflushed staging/pending causal events and healthy local storage.
Every failed explicit/background exchange invalidates the last roundtrip result.
Malformed semantic events are quarantined without blocking unrelated valid events.

Outbound events must belong to shared branches. Objects are selected only by
hashes referenced by those authorized events, never by scanning global CAS.
Private events/new objects/checkpoints stay local across restart and deduplication.
Receive rejects private/unrooted data and unpaired event origins. Hash-only
references cannot promote private CAS bytes: activation requires accepted shared
bytes or verified inbound bytes. The received-byte provenance ledger survives
restart and constrains object inventories too. There is no GC or background bucket
uploader; explicit test-bucket export selects the same authorized shared closure.

`publish <fresh-name> --current-state-only` creates a new shared branch from the
canonical visible cut. It deliberately excludes private event parents/dependencies,
history, checkpoints, tombstones, hidden entries and conflict alternatives. Those
remain durable locally. It neither shares the original branch nor resolves its
conflicts. Branch IDs are authoritative; private and shared labels can overlap.
Concurrent shared publications with the same label are both retained and synced;
ambiguous label lookup asks for a branch ID. An existing local label in the same
scope prevents local duplicate creation. Full-history publication and merge into
existing branches remain open decisions.

## Orchestrator and distributable product direction

The next management layer is specified in
[ORCHESTRATOR-PLAN.md](ORCHESTRATOR-PLAN.md), recorded 2026-10-03. Add a new
orchestrator mode in the existing executable, supervising one daemon worker per
state. This preserves current store ownership and isolates the process-global
WinFsp mount bridge while allowing multiple independent states/mounts.

The proposed orchestrator takes one versioned `--defaults-file`, with a default
native data directory containing `states/<local-state-UUID>/`. Its own SQLite
registry stores managed-state identities, locally chosen mount paths, desired
lifecycle, durable management jobs, machine pairings and sharing contracts.
Per-state SQLite/CAS remains authoritative for filesystem history and branch
selection. CLI and later UI use one local authenticated management API.

Pairing orchestrators once establishes machine trust and persistent protected
credential references. Owners separately authorize remote catalog visibility and
repository/branch sharing contracts. A peer can list authorized offers and request
a local replica with its own state/device UUID and mount path; pairing does not
grant blanket state access or remote administration. Existing event-origin IDs,
private branches and explicit current-state-only publication remain intact.

This requires contract-scoped event/object inventories and receive validation,
bilateral agreement, persistent key management and eventually per-recipient
acknowledgements. Today's single-peer worker and outbox acknowledgement flag do
not support arbitrary multi-peer sharing. Start with two replicas per repository
and many repositories over one machine pairing before expanding membership/relay.

Delivery order: local supervisor/config/registry; safe state/mount lifecycle;
persistent pairing and authorized discovery; scoped two-machine sharing; service
packaging/UI; then expanded multi-replica policies. All are **planned**, not
implemented or validated. The detailed design records recovery, identity/adoption,
revocation, OS-user authorization and acceptance gates for each stage.

## Validation and remaining gates

[README.md](../README.md) contains exact build/test/run/pairing commands and limits.
Core tests run with `cargo test --offline`; real mount/peer acceptance runs with
`python scripts/e2e.py`. [VALIDATION.json](../test-evidence/VALIDATION.json) records actual checks
and the fixture/log directory under `test-runs/<UUID>/`. These are real WinFsp
paths with normal PowerShell/Python OS I/O, not substituted native directories.

| Gate | Evidence/status |
|---|---|
| Mounted save/mkdir/rename/temp replacement | Passed |
| Same-path A-B-A preserving branch bytes | Passed |
| Open writer/mapping refusal preserves branch | Passed |
| Shared effects without checkpoint | Passed between two LOCAL runtimes |
| Offline conflicts, identical state and alternatives | Passed |
| Acknowledged crash/restart persistence | Passed process kill and object/SQL injection |
| Ack loss and duplicate replay | Passed |
| Private canaries before/after restart | Passed |
| Current-state publication excludes old private data | Passed |
| Causal successor, three contenders, permutations/review | Passed core suite |
| Bounded multi-object atomic publication and restart | Passed regression; two 16 MiB objects across two pages |
| Private known-hash inbound disclosure attempt | Staged without visibility/ack/export; passed restart regression |
| Namespace parent/delete/child/correction permutations | Identical projection/full records across all 24 orders |
| Deferred invalid child/later valid parent | No premature ack; child/descendants quarantined, local flush/restart/replication and independent branch progress passed |
| Three-origin cycle then ordinary partial correction | All three historical cycle records retained across all 24 orders/restart |
| Namespace history budget | Commit refusal leaves journal/conflicts unchanged through restart |
| 10,000 sequential content saves plus delete/undelete/restart | Passed; steady first/last-1,000 rate; no topology cap |
| Two replicas with 100 offline saves each | Passed identical convergence/restart; all 10,000 historical content pairs inspectable |
| Two unrelated 65-rename chains in a shared folder | Passed without multiplying independent history budgets |
| Private/shared and concurrent shared branch labels | Passed without blocking main catchup |
| Peer notifications/read-only handle refresh | Passed actual mounted FileSystemWatcher/read checks |
| Namespace recovery and checkpoint history/restore | Passed core tests and actual mounts |
| Optional object backend | Local native test-bucket export/privacy passed; no remote provider validation |
| Two real computers | Ten live mounted checks passed on FIGLOALDS/FELYPE at `303fec8`; stale acknowledgement counters found; correction validated locally only |
| Physical offline/reconnect and corrected version | **Not run** on two computers; await user-directed next steps |
| Chosen editor/build ecosystem | **Not qualified** |
| All remount faults, disk-full, power loss | Remaining qualification |
| Checkpoint replication/diff and branch merge | Subsequent source-control work |

Bounds: 16 MiB/object, 8 MiB/local event, 64 MiB/peer frame, 4,096 competing topology
witness cuts per namespace dependency cone (width >= 2), whole-file buffering, complete retained
history materialization. Links/reparse points/ADS, persistent ACL edits, arbitrary
timestamp/attribute setters, distributed locks and live databases are unsupported.

WinFsp was **already installed**, at `C:\Program Files (x86)\WinFsp`, DLL version
`2.1.25156.ddca7bd` / WinFsp 2025. No driver/security/firewall changes, persistent
peer credentials, infrastructure, pushes/external publishing or writes to other
projects occurred. See the README for prerequisites and steps to validate a real
second machine; the existing tests make no two-computer claim. Remote object-store
qualification needs a selected provider/endpoint, bucket/container and test prefix,
appropriate temporary read/write credentials and approval for writes to that
prefix. No provider adapter/infrastructure has been provisioned. Physical power
loss, storage-controller guarantees, disk-full handling and arbitrary mapped writes
remain unqualified; ordinary buffered writes and failed close publication are not
acknowledged durable saves.
