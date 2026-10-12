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
   each repository assigned to one bulk lane. Each lane proxies bounded
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

Pool lanes reconnect independently with exponential backoff after errors. Lane 0
is always reserved, including when bulk lanes fail; bulk work never borrows it.
The default four connections provide one control lane and three bulk lanes.
Configured totals of one are promoted to two. Stable repository sharding across
bulk lanes preserves serial object transfers while several folders can transfer
concurrently. Only bulk exchanges update object resume cursors, so delayed
metadata replies cannot rewind an in-flight transfer. Bulk pages retain event
manifests for object authorization and recovery; duplicate events are idempotent.

Roles are negotiated during the pairing handshake. A control session rejects
object payloads and emits metadata-only replies. A peer that does not acknowledge
the role fails with `RESERVED_LANES_UNSUPPORTED`; there is no silent downgrade.
Legacy inbound sessions without a role remain supported. Idle bulk lanes use
lightweight pings; only the reserved lane performs periodic discovery. Explicit
remote-list/receive commands use a separate short-lived control session.

Metadata pages stage events in SQLite even while file transfers are stalled.
Events depending on missing content or causal predecessors become visible and
are acknowledged only after those dependencies arrive. This reserves transport
capacity, not disk/CPU bandwidth: worker operations still share their local
engine. The orchestrator's local catalog, mount paths and pairing settings are
not replicated; state sync covers shared filesystem events and inventories.

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
