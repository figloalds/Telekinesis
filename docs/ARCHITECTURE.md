# Folder-first architecture

1. **Filesystem state and mount.** `core`, `staging`, `runtime` and the native
   adapters implement one durable causal filesystem. Each worker is a separate
   process holding a state lock and, optionally, one real mount. The worker has
   a loopback WS control endpoint and no remote transport configuration.
2. **Orchestrator.** `orchestrator` stores folder identity, backing paths, mount
   assignment and desired lifecycle in SQLite. It starts independent workers,
   reports failures, retries crashes and stops workers cooperatively. The CLI
   and desktop call its loopback management endpoint.
3. **Networking in the daemon.** The same process runs the optional inbound WS
   listener and outbound connection pools. No pairing service or transport
   sidecar is spawned. `ws` owns handshake, message bounds and JSON framing.
   Outbound pools accept WS or WSS. WSS verifies the server through the platform
   TLS provider; the inbound listener uses plain WS behind any operator-managed
   TLS proxy. TKFS does not generate/install certificates or modify system trust.
4. **Pairing.** The first message supplies protocol version, installation ID
   and pairing code. The server checks its persisted salted verifier. The ID
   identifies a connection; it is not a certificate or credential.
   Code rotation invalidates subsequent operations on existing sessions.
   This exchange is the same for WS and WSS. The receiving catalog keeps the
   salted verifier; dialing peers keep the remote code as ordinary catalog data.
5. **Replication.** The reserved control lane lists running remote repositories,
   shares that catalog with the bulk lanes, and exchanges event manifests,
   branches, inventories and durable acknowledgements for every matching folder.
   File objects and resumable chunks use the remaining persistent lanes, with
   object hashes within each repository assigned across all bulk lanes. Each lane proxies bounded
   offer/exchange/apply pages to its local worker. Server
   responses include changes in the opposite direction. Reconnect resumes from
   durable inventories and object offsets, including after worker restart.
6. **Relay.** Pages may contain shared events from any replica origin. Events
   authored locally must still match committed local history, and all object
   hashes, causal references, repository bindings and acknowledgements remain
   validated. A <-> B <-> C can converge without a direct A/C connection.

A pair has access to all running shared repositories on the receiving daemon.
Receiving a folder allocates a fresh local device identity for the same repository.
The filesystem retains private branches as local worktrees; they are not included
in published pages. Checkpoints remain local. There are no per-device grants,
certificate pins, enrollment approval records, protected credential backends or
application-managed OS permissions.

## Independent synchronization and mounting: lifecycle study

This is a proposed lifecycle change, not the current implementation. Inspection
on 2026-10-12 confirmed that replication itself supports unmounted workers:
`paired_sync` operates on `Engine`/`Store`, and `bulk_sync` opens independent store
connections. Neither needs a filesystem adapter. Existing headless integration
tests exercise real metadata and object exchange through orchestrator processes.

The coupling is in startup and management. `main::worker` starts the mount before
`runtime::start_rpc`; a mount error exits the worker before it can advertise a
repository. The supervisor equates worker readiness with the requested mount,
and `Action::Stop` stops the worker. Initial receipt already uses a headless worker,
but replaces that process once metadata is ready to mount. Mount path changes
also require stopping the worker. This explains how an empty leftover Docker mount
directory can block every sync lane while the orchestrators remain connected.

Keep one durable state owner and make the filesystem adapter optional throughout
that worker's lifetime. Persist separate `sync_enabled` and `mount_enabled` intent,
with the assigned path independent of whether it is currently attached. Recover
the store and establish RPC first; report `worker_ready` before attempting any
mount. A serialized mount controller then attaches/detaches the adapter without
holding the engine or supervisor lock during OS calls. Mount errors and retries
must update mount status only, leaving discovery, metadata commits and object
transfers available. Store integrity/recovery failures remain worker health errors.

| Sync intent | Mount intent | Expected operation |
| --- | --- | --- |
| Enabled | Detached or failed | Headless replication continues; mount can retry independently. |
| Enabled | Attached | Filesystem operations and replication share the recovered state. |
| Paused | Attached | Local filesystem remains usable; missing remote content reports unavailable. |
| Paused | Detached | Worker can stop once pending saves and management operations are settled. |

Explicit mount/unmount and pause/resume-sync actions should coexist with the
existing stop-folder action, whose full-stop semantics must remain clear. Sync
pause must gate both outbound scheduling and inbound exchange, including active
bulk requests at bounded page boundaries; suppressing only the local dialer would
still allow peers to send changes. Current saves remain durable and queued for
resume. Initial metadata readiness gates automatic mounting only, with no worker
restart, identity change or transfer-cursor reset.

There are adapter details to address before implementing dynamic mounting:

- WinFsp's `ENGINE: OnceLock` is assigned by `mount::start` and rejects a second
  attempt, even after some failed first attempts. Bind it once to the state owner
  and permit adapter recreation only for that same engine.
- Both adapter stop paths fence filesystem opens using `switching`; shutdown
  assumes process exit. Ordinary detach/retry must restore a well-defined fence
  state, clear stale handles only after callbacks drain, and preserve dirty saves.
- FUSE dispatch/remount and WinFsp remount failures currently enter the common
  `Engine::health` field. Separate mount health from storage/recovery health so
  a mount error cannot falsely mark replication unhealthy or prevent safe detach.
- Keep discovery's mount information current on attach/detach, and discard or
  coalesce filesystem notifications while detached. Reattaching reads the current
  projection, so replaying every headless change as an OS notification is unnecessary.
- Linux has `start_managed`/`recover_managed` helpers, but the current worker calls
  plain `start`. Persistent mount ownership and mount-namespace checks must precede
  recovery; do not automatically remove arbitrary existing directories or mounts.
  Container mount propagation remains a separate deployment concern.

Qualify the change with failed-mount-but-successful-sync tests, repeated attach /
detach on the same PID and RPC endpoint, ongoing content transfer during detach,
busy-file refusal without stopping sync, initial sync without worker replacement,
pause behavior in both directions, and daemon restart preserving both intents.
Run adapter cases on WinFsp and real Linux FUSE. This gives lifecycle independence;
a native adapter crash can still kill its host process. Full crash isolation would
require a separate adapter process and a filesystem RPC boundary, a larger follow-up.

Pool lanes reconnect independently with exponential backoff after errors. Lane 0
is always reserved, including when bulk lanes fail; bulk work never borrows it.
The default four connections provide one control lane and three bulk lanes.
Configured totals of one are promoted to two. Bulk protocol 1 shards object hashes
across the pool, keeping each object sequential while different objects transfer
concurrently. Bulk pages carry object inventory, demand, offsets and bytes only;
they never activate or acknowledge causal metadata. A sender offers only shared
objects referenced by metadata already advertised by the receiver. Resume state
comes from durable receiver spools, so reconnection and changes to pool size do
not lose progress. Overlapping retries verify the existing prefix before append.
Per-object locks also coordinate duplicate deliveries through different peers.

Worker RPC sockets are persistent and owned separately by each peer lane, on
both dialing and serving orchestrators. Application errors preserve the socket;
transport errors discard it without replaying an uncertain request. Changed
worker addresses and idle sockets reconnect before the next call. Content-bearing
rounds and advancing metadata yield for 10 ms instead of waiting for the idle sync interval. This retains
serial object cursors and the reserved control lane while avoiding a fixed pause
after every content page. Repository status exposes the last successful exchange's
object byte counts and offer/exchange/apply times; remote exchange time includes
network and remote worker processing, not just wire latency.

Bulk workers use independent SQLite connections and perform disk I/O outside
the semantic engine mutex. Object registration uses a short immediate transaction
after durable installation; SQLite still serializes writers. Discovery also reads
committed state independently. Metadata activation commits at most 32 events or
a 50 ms target per exchange, checked between atomic events. Remaining events stay
in the durable inbox without acknowledgement. Incoming rows are staged together;
already accepted duplicate events are checked without replaying them. Contended
metadata requests return `WORKER_BUSY` for a short retry instead of accumulating
threads waiting for the engine. Individual atomic event validation is not preempted.

The HTTP upgrade request/response negotiates `X-Tkfs-Wire: tkfs-binary-gzip-v1`.
Its versioned binary envelope contains JSON metadata and raw object attachments.
Each message independently uses gzip level 1 only when beneficial, without shared
compression dictionaries. Receivers bound wire bytes, declared/inflated bytes and
the reconstructed JSON size; invalid lengths, CRCs, duplicate attachment paths and
trailing members are rejected. Content hashes continue to cover original bytes.
Without the echoed capability, the connection uses ordinary JSON. Pairing separately
negotiates bulk protocol 1 and each socket's object shard. Older peers retain the
serial repository bulk path and dedicated control lane; compression and parallel
bulk require support on both ends. Authentication and TLS validation are unchanged.

Roles are negotiated during the pairing handshake. A control session rejects
object payloads and emits metadata-only replies. A peer that does not acknowledge
the role fails with `RESERVED_LANES_UNSUPPORTED`; there is no silent downgrade.
Legacy inbound sessions without a role remain supported. Idle bulk lanes use
lightweight pings; only the reserved lane performs periodic discovery. Explicit
remote-list/receive commands use a separate short-lived control session.

Metadata pages validate and activate causal events in SQLite even while file
transfers are stalled. Missing causal predecessors still defer activation;
missing object bytes do not. Format 6 carries content sizes independently of
possession, so namespace and file metadata remain meaningful on partial replicas.
Metadata acknowledgements certify the journal, while object inventories certify
available content with shared provenance. A hash-only manifest never grants
permission to advertise private-only local bytes.

The independent content scheduler compares available objects with peer object
inventories, including references in already-acknowledged events. Requests for
reads/previews persist by object hash, receive priority on bulk lanes, and are
forwarded as bounded, deduplicated demand to adjacent peers. This supports relay
without recursive blocking network calls. Background replication still attempts
all shared history; selective retention and garbage collection are not added.

Bulk receipt writes resumable spools, verifies the completed file once and moves
it into CAS. Hashing, file flushes and installation run outside the engine mutex;
parallel bulk lanes register completed objects through short SQLite writer
transactions; the legacy path registers through the engine connection.
Registration is distinct from metadata activation. Interrupted registration can
be recovered by retransmission, and stale/complete partial cursors are repairable.

Read sessions refer to immutable objects without eager staging copies. A read
hydrates and verifies its object outside the engine mutex. Mutation creates a
private staging buffer; truncate-to-zero can discard the old reference directly.
FUSE content requests use a bounded set of asynchronous completions so the device
reader can continue servicing metadata. WinFsp uses namespace-only operation guards
so waiting file reads do not hold a volume-wide native lock. The orchestrator's local catalog, mount
paths and pairing settings remain local.

Peer status reports effective/configured pool sizes, the reserved count, and
each lane's role, connection state, exchange count and latest error. A repository
can appear on both its control and bulk lane; each status is the last observation
on that lane, not a global synchronization barrier.
Folder failures are isolated within a lane and reported by repository, with
exponential retry delays capped at 30 seconds. A complete remote error reply keeps
the socket usable; transport failures or malformed reply envelopes require a
reconnect. Reconnect delays also back off to 30 seconds, and waits remain
interruptible by shutdown or unpairing. Discovery skips unavailable worker
summaries and reports their errors on the receiving daemon's folder inspection.

Single-writer locks, path overlap checks, finite frames and durable acknowledgements
remain data-integrity mechanisms. Pairing-code validation is the only application
network credential check.

## Startup snapshots and cursor replay

Each branch has a disposable, versioned startup snapshot in the metadata database.
It contains the merged projection (including register heads and tombstones), all
historical conflict/review records, the causal frontier, and a local acceptance
cursor. A SHA-256 checksum protects the snapshot envelope; a separate digest binds
it to the exact accepted journal prefix and sequence numbers. Repository, device,
branch descriptor and projection format must match before reuse.

`journal_sequence` assigns monotonic SQLite AUTOINCREMENT sequence numbers through
an event-insertion trigger. These numbers order local replay only; they never
decide a CRDT winner. Event updates/deletions invalidate snapshots through triggers,
including when an older executable modifies the journal. No history is pruned.

Snapshots are sealed in the same transaction as a full projection update and
periodically during incremental commits (at most 128 local acceptance-sequence
advances between seals for an active branch). Startup restores the base and replays
the bounded suffix using the existing proven content/metadata and sequential local
namespace successor rules. The replay only changes derived projection, conflict and
frontier tables; events, receipts, outbox acknowledgements and peer state remain
intact. Successful replay seals an updated snapshot atomically. Clean dismount is
not required, and an outer transaction rollback also rolls back the snapshot.

Missing/incompatible/damaged snapshots, a changed journal prefix, non-topological
acceptance order, an excessive suffix, or edits outside the incremental rules use
full reconstruction. A partially attempted replay is rolled back first. Sequential
directory moves and empty directory deletions after the complete accepted frontier
use incremental projection when the namespace is conflict-free. Directory moves
validate ancestry and preserve descendant entity IDs. Deleting a parent with live
children or introducing a cycle still requires the historical reducer, as do
concurrent namespace changes and reviews; namespace replay on inactive branches also
uses that fallback. An older state incurs one full reconstruction to seed snapshots.

Startup still verifies available content, validates/repairs the journal, recovers
incoming events and restores pending staged saves. A metadata snapshot neither
claims missing object bytes are available nor claims the replica has caught up with
remote peers. Change the snapshot format version whenever projection or validation
semantics change.

## Desktop and availability

The worker status includes `content` counts and up to 20 missing current-file
observations, plus fresh/stale per-replica metadata observations for either dial
direction. Availability distinguishes missing, queued, downloading, verifying,
retrying and available content. Metadata convergence can coexist with unresolved
conflicts or unavailable file bytes.

The desktop's folder panel uses orchestrator-proxied worker RPCs. Conflict pages
contain at most 100 unresolved records; previews are limited to 64 KiB of text.
Resolution and namespace recovery use exact conflict IDs, a review token and
idempotent request receipts. No automatic text merge is performed. Existing
open-handle/dirty-session guards still apply to semantic management mutations.

The SHA-256 algorithm is unchanged. Directory exports now hash while copying;
received objects no longer undergo a second copy-and-hash pass. Parallel hashing,
binary wire frames and paginated peer inventories remain separate performance
work requiring measurement and protocol design.
