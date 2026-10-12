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
5. **Replication.** The client lists running remote repositories, finds local
   matches, and shards repositories across persistent connection lanes. Each
   lane proxies bounded offer/exchange/apply pages to its local worker. Server
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

Pool lanes reconnect independently after errors. A repository currently uses one
lane, preserving serial page ordering; several folders can transfer concurrently.
Idle lanes keep exchanging discovery requests so pools remain persistent. Peer
status reports each lane's connection state, exchange count and latest error.

Single-writer locks, path overlap checks, finite frames and durable acknowledgements
remain data-integrity mechanisms. Pairing-code validation is the only application
network credential check.
