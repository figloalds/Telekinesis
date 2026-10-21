# Architecture

This is the design as implemented. The plan it came from, with the reasons
for each decision, is `docs/revamp/REVAMP_PLAN.md`; the defects of the
previous release it removes are mapped in `docs/revamp/ROOT_CAUSES.md`
(RC1–RC13).

The default repository path below retains legacy content writes. Wave 6 is
being implemented in isolated stores; its complete scope and current evidence
are in the [Wave 6 plan](revamp/WAVE_6_PLAN.md) and
[implementation record](revamp/notes/wp6-progress.md). An internal fixture
marker is not production activation or a compatibility fence.

## Wave 6 isolated content path (in development)

Typed immutable descriptors distinguish legacy whole files, direct content
and indexed manifests with 64 KiB chunks. `content/local.rs` verifies CAS and
manifest ranges; provider pins retain immutable lifetimes. Metadata commits
reference admission changes with their events. The object engine expands
authenticated manifest edges incrementally and verifies frames independently
of causal metadata activation. Foreground and structural demand retain their
own capacity and reference/generation authority. Empty preparation polling is
10 ms for foreground and 50 ms for structural work; background keeps its
exponential idle backoff. Structural repair introduced by a later COW mutation
therefore does not inherit background's idle delay. These polling intervals
are not global admission guarantees for arbitrary load or repository counts.

Private writes use `content/write.rs` and its immutable overlay maps. Working
bytes are buffered; FULL Saved checkpoints carry recovery intent. The bounded
`runtime/content_saves.rs` executor constructs changed canonical paths, commits
metadata through secondary connections and adopts the continuation on one
stable `SaveFile`. Receipt replay prevents a lost reply publishing twice.
Complete root reclamation and production recovery/lifecycle remain unfinished.

Core's scoped `apply_content_save_basic` separately supports an owned metadata
commit plus an unselected capture rebase, with exact request replay and
checkpoint-bound time retention. It preserves ordinary stale-save fencing.
This primitive is not yet wired to native set-basic callbacks; native timestamp
flags, selected-save handling and namespace continuity remain development work.

Explicit `NativeWrites::install` before mounting/opening attaches existing
descriptor files to this pipeline on WinFsp/FUSE. Callbacks share a per-file
mutation gate outside Engine; metadata length uses an atomic cache. Native
write/resize preflight verifies the required base paths and surviving bytes
before applying and acknowledging the mutation. Missing ranges register
authenticated demand through `runtime/content_base.rs` after releasing all
preflight buffers, cursor and journal guards, without holding file state.
Exact Working/session validation prevents a completed preflight from applying
over a concurrent mutation. Full-covered old chunks and zero ranges need no
old data; preflight creates no CAS objects or FULL checkpoint.

Native close/fsync use FULL Saved checkpoints and strictly local canonical
preparation/publication. They never invoke peer recovery. If required local
base bytes disappear after an acknowledged write, preparation fails locally
and retains Saved/private bytes for retry. Recovered or headlessly modified
sessions include their existing dirty paths in the next mutation's preflight.
Deadline, grant/checkout changes, cancellation and stop preserve private changes.

This path does not yet supply durable native namespace operations, complete
startup/status/shutdown, production activation/migration, the full retention
audit or the Wave 6 build-performance gates. Its exact contracts and remaining
qualifications are maintained in [WAVE_6_CONTRACTS](revamp/WAVE_6_CONTRACTS.md).

## Processes and failure domains

```text
           CLI / desktop                       CLI / desktop
                 | loopback control                  |
   worker ---- daemon ===== tkfs-peer/3 sessions ===== daemon ---- worker
   worker ----/        lane 0 control, lanes 1.. bulk        \---- worker
     | mount (WinFsp / FUSE), store (metadata.sqlite, objects/)
```

- **Daemon** (`tkfs orchestrator`): one per data directory
  (`orchestrator.lock`). It owns the folder catalog, supervises one worker per
  folder, dials and accepts peers, and serves status and management on a
  loopback control endpoint.
- **Worker** (`tkfs daemon --state STATE --supervised`): one per folder state
  (`owner.lock`). It owns the folder's store and its mount and activates the
  events it receives. Workers never dial anyone: the daemon talks to them over
  loopback (JSON RPC, and binary sessions for object bytes), presenting the
  worker's per-instance token.
- **Clients**: the CLI and the desktop are clients of the daemon's control
  endpoint, authenticated by the daemon's token (`DATA/orchestrator.token`).
  The desktop uses one long-polling status subscription and separate
  connections for commands, so a command never waits behind the
  subscription.

Every mutable resource has one owner, and no lock is held across I/O:

| Resource | Owner |
|---|---|
| `folders.sqlite` | the catalog actor (thread `tkfs-catalog`); everyone else reads its published snapshot |
| a worker process | that folder's FolderController (thread `folder-<state>`) |
| a peer's sessions | its PeerSupervisor (`peer-<installation>`) |
| a connection's socket | its session I/O thread |
| a worker's mount | its MountController (`mount-ctl`) |
| a worker's inbox activation | its activator thread |
| the status | the StatusHub (passive; rebuilt at most 4 times a second) |

A fault stays in its domain:

| Domain | A fault stops | It never stops | Recovery |
|---|---|---|---|
| Object | that object | other objects, the other direction, the lane | per-object backoff; `CORRUPT_AT_SOURCE` makes the sender re-verify and quarantine its copy |
| Inbox event | that event and its causal descendants (`blocked`) | unrelated events, other branches | transient: retry 1 s → 5 min; validation: quarantine, `revalidate` |
| Exchange (peer, repo, op) | that key | other repositories on the same connection | per-key backoff by error class; contention is not counted |
| Connection | its requests in flight (never replayed) | other lanes and peers | redial 1 s → 60 s with jitter, reset after 5 s healthy; after a network loss the peer's lanes redial on one shared cadence (1 s, growing with the outage to 60 s) and a lane that connects wakes its siblings |
| Peer | that peer | other peers, inbound sessions, folders | backoff, `Incompatible` (10 min), `Unauthorized` (60 s) |
| Mount | file access for that folder | sync, status, graceful stop, other folders | attach retry 2 s → 120 s, or wait for the user |
| Worker process | that folder | the management API, other folders | restart 1 s → 300 s, breaker after 5 failures in 10 min |
| Catalog | management writes | running workers, lanes, status (last snapshot) | retry; a daemon-level error |
| Status / desktop | display freshness | any producer | none needed |
| Daemon | everything until restart | running workers and their mounts | restart, then adoption |

Shared state is published as snapshots (`util::Snapshot`: an `Arc` behind a
lock plus a generation counter and a condition variable): readers never wait
for a writer, and waiters wake on generation changes instead of polling.
Errors carry a code, a class and a scope (`model::errors`, see
[status and errors](#status-and-errors)).

## Daemon

### Catalog

The catalog actor takes `orchestrator.lock`, opens `folders.sqlite` (WAL,
`synchronous=FULL`) and runs each command in one transaction (3 retries on
`SQLITE_BUSY`, a panic rolls the command back). A caller waits at most 5 s: a
command that has not started is withdrawn (`CATALOG_UNAVAILABLE`), one that is
running answers `REQUEST_TIMEOUT` (it may still be applied). If the thread
dies, writes fail and the last snapshot keeps serving reads.

The schema is additive: the tables of the previous release are never altered
(they have positional `INSERT`s that an older binary still uses); new state
lives in side tables: `folder_intents` (sync and mount intents), `mount_gates`
(initial-sync gates, mirrored to the legacy `initial_sync` table so a
downgraded binary still sees them), `peer_meta`, `peer_credentials`
(per-peer keys and their revocation) and the `verifier_generation` setting
(rotations). `folders.sqlite` is owner-only (Unix `0600`).

### FolderController

One actor per folder runs the worker's lifecycle and publishes a `FolderView`:

| Phase | Entered | Leaves to |
|---|---|---|
| `Stopped` | not desired, no process | `Starting` on Start, Retry or a catalog change |
| `Starting` | spawn or adoption probe | `Running` on a valid Ready line or adopted `hello`; killed after 30 s without `startup_progress`; `Backoff` on exit |
| `Running` | Ready, or adopted | `Stopping`, or `Backoff` on exit or when unresponsive |
| `StopBlocked` | a graceful stop answered `BUSY_VIEW` | graceful stop retried every 2 s; `Stopping{force}` on ForceStop; `Running` on Start |
| `Stopping` | Stop or Shutdown | `Stopped` |
| `Backoff` | crash, start failure, unresponsive | `Starting` after 1 s · 2^(n−1), capped at 300 s, ±20 %; the count resets after 10 min Running |
| `Failed` | 5 failures in 10 min (`CRASH_LOOP`), a fatal start error, `PREVIOUS_WORKER_UNRESPONSIVE` | `Starting` only on Retry, Start or a catalog change |

- **Stop ladder.** `desired=0` is persisted first; then a graceful `stop`
  (5 s; `BUSY_VIEW` → `StopBlocked`); ForceStop sends `stop{force}` (the
  worker saves dirty files to its pending saves, detaches and exits); then 10 s
  for the exit; then a kill of the verified process. While the worker's
  heartbeat reports its engine held by a hold that is not stalled (a large
  save: the stop's save of the dirty files waits for the engine), the 10 s
  start again, for at most 120 s after the force stop was requested (a
  graceful stop blocked before it does not count), so a busy save does not
  turn a force stop into a loss of unsaved files; a stalled or silent worker
  is killed as before. A worker that has not printed Ready yet is killed
  directly (store recovery is crash-safe).
- **Liveness.** A dedicated connection long-polls `watch` (2 s waits, 4 s
  deadline). No heartbeat for 8 s or 3 failed watches → a `hello` probe; if it
  fails, the worker is killed and backs off. An engine lock held for more
  than 120 s marks the folder degraded, without a kill.
- **Adoption.** Workers are not tied to the daemon's lifetime (no Job Object,
  no parent-death signal). At start the controller reads `STATE/worker.json`,
  checks the process is alive with the recorded start time and executable,
  and calls `hello` (repo, device, instance). Same build: `Running{adopted}`.
  Different build: graceful stop, wait for `owner.lock`, spawn. Alive but
  silent for 30 s: `Failed{PREVIOUS_WORKER_UNRESPONSIVE}`. A worker of the
  previous release (`runtime.json` without `worker.json`) is asked to stop
  first. A spawn never happens while `owner.lock` is held; waiting for it is
  not a failure (`PREVIOUS_WORKER_STOP_TIMEOUT` after 10 s). A supervised
  worker with no contact for 10 minutes stops itself when quiet.
- **Shutdown** (the action, Ctrl-C, console close, SIGINT, SIGTERM — one path):
  reject new requests, close the network (5 s), stop every folder in parallel
  (30 s), flush the reply, join the control threads, remove
  `orchestrator.json`. Busy or silent workers are `left_running` for adoption;
  `desired_running` never changes.

### Mount and the initial-sync gate

When the folder is Running, its mount is enabled with a path, and its gate
allows it, the controller sends `mount-attach` (60 s). A NeedsUserAction
failure waits for `attach` or a mount change; a Transient one is retried
2 s → 120 s. A received folder has a gate whose source is the peer it was
received from:

- `Awaiting{source, waiting_on}` holds the attach;
- it is **satisfied** when our cursor of the source's feed is at its tip,
  no inbox rows from the source exist in any state, no rows from it were
  rejected, and the source reports nothing left to activate;
- `Blocked` when the source is caught up but quarantined, blocked or rejected
  rows from it exist (`GATE_BLOCKED`): `revalidate` or `mount-now`;
- `Overridden` by `mount-now` (persisted);
- if the source is unpaired, the gate retargets to a peer that shares the
  repository.

The gate marker is deleted only after `Attached` (for a folder without a
mount, once the gate is satisfied or overridden).

### Published catalog

Each folder publishes `{repo, replica, label, branches, tip, content
counters}` to peers (`catalog.list`). The worker's `published-summary` is
read on Ready and when its branches change; the tip and content counters
follow its heartbeat. A folder whose worker does not run is published
`unavailable`, so peers wait for it (`waiting for the peer's folder`) instead
of reading it as unshared.

### Management API

The control endpoint is a loopback WebSocket (address in
`DATA/orchestrator.json`). The upgrade must carry the daemon's token
(`X-Tkfs-Token`, read from `DATA/orchestrator.token`, an owner-only file the
daemon writes at start); without it the connection is refused before any
action is read. One thread per connection, persistent request loop, at most
64 idle connections plus those parked inside a long request. Reads (`list`,
`inspect`, `peers`, `status`, `subscribe`) come from snapshots and never
wait. Writes persist the intent in the catalog, then command the folder's
controller; `start` returns at once, `stop` waits up to about 13 s
(`force-stop` up to 15 s). `diagnostics` writes a bundle under
`DATA/diagnostics/<time>/` from snapshots and files only (no worker RPC, no
catalog command), with every code, secret and token removed.
`subscribe{since, wait_ms ≤ 25 s}` long-polls the hub. The client
(`ControlClient`) never resends a mutating action after a transport failure
(`CONNECTION_LOST`); read-only actions are retried once.

### Outbound peers

`PeerManager` runs one `PeerSupervisor` per paired installation and follows
the catalog (a changed record restarts it; a removed one stops it within 5 s).
A peer's pool is `max(connections, 2)` sessions: lane 0 is the control lane,
lanes 1…N−1 bulk lanes. Each lane is a state machine: `Dialing` (every
resolved address, 3 s each) → `Handshaking` → `Ready`, or `Backoff` (1 s →
60 s, ±20 %, reset after 5 s Ready), `Incompatible` (rechecked after 10 min
or when the record changes), `Unauthorized` (60 s or a new code). An
incompatible or unauthorized peer is probed by one lane at a time.

- **Network loss** (WP5.3). A lane that was healthy (`Ready` for 5 s) and
  loses its session by the transport (`CONNECTION_LOST`, `TRANSPORT_DEAD`,
  `WS_CLOSED`; not a `GOAWAY`, not a close of this side's own) starts the
  peer's network loss. Until a lane connects again, every lane whose dial
  reaches nothing or whose session the transport loses redials on **one
  cadence shared by the peer's lanes**: the first redial 1 s after the loss,
  then one per spacing, the outage's age / 30 between 1 s and 60 s (1 s for
  the first 30 s, 1.5 s at 45 s, 4 s at 2 min, 60 s from 30 min on), ±20 %,
  without escalating the lane's own backoff. When a lane connects, the loss
  ends, every sibling whose wait is wakeable (its dial failed, or the cadence
  paces it) dials at once, and a sibling dial still in its handshake that
  began before the successful one is interrupted and redialed. A lane
  backing off after a session the peer closed before it was healthy (a
  flapping acceptor) keeps its own escalating delay. Every redial is a fresh
  session (invariant 16). A 45 s outage in the middle of a 512 MiB transfer
  has every lane `Ready` within about 0.3–1.1 s of the restore and bytes
  flowing again within about 0.2–1.1 s, from the durable offset
  ([validation](VALIDATION.md#floors-and-the-latest-measurements)).

- **Remote catalog.** Lane 0 asks for the peer's published catalog when it
  connects and every 2 s with the generation it has (`catalog.list{since}`,
  answered `unchanged` in O(1)). The last copy survives reconnects, so bulk
  lanes never depend on lane 0 being up.
- **MetaScheduler** (lane 0). One key per (peer, repository) that both sides
  hold. A key is due when the local feed tip moves, the remote tip moves,
  the worker's sync generation moves (outside the echo window of its own
  exchanges), its retry is due, or `sync_interval_ms` passed since its last
  exchange (anti-entropy). At most 4 exchanges at once, one per key. A
  folder that is not running (`WaitingForWorker`), a remote folder that is
  not serving (`WaitingForRemote`) or a paused folder is not a failure.
  Failures back off by class: contention 50 ms → 1 s (not counted), transient
  1 s → 60 s, needs-user-action 60 s, incompatible and fatal 10 min. A lost
  session charges no key; the next attempt is a fresh request.
- **Bulk lanes** run objects v3 (below), one scheduler per direction. When a
  lane ends (its session lost, or told to stop; not while the daemon stops),
  it gives up the leases its rounds kept for its next round, holding each
  repository's key meanwhile, so no round overlaps the release.

### Inbound server

One accept thread; at most 32 connections in the pre-auth handshake (more are
closed at once) and 128 sessions (`GOAWAY{TOO_MANY_SESSIONS}`); pre-auth
messages ≤ 16 KiB and ≤ 5 s per step. Each session gets a handler pool (4
threads for control, 2 for bulk, queue 16; full → `BUSY`). Requests are
checked in order: daemon not stopping, the session's verifier generation still
current, the op allowed on the session's role (`meta.exchange` only on
control, `catalog.list` on control and probe sessions, `objects.*` only on
bulk), the folder running
(`REMOTE_WORKER_STARTING`) and not paused (`FOLDER_SYNC_PAUSED`). Exchanges run
on their own threads, bounded per session (4 on a bulk session, the
dialer's 4 peer calls per lane) and server-wide, so a hung worker never holds
a handler. A rotation of this host's pairing code revokes every
per-peer credential and sends `GOAWAY{PAIRING_CODE_CHANGED}` to older
sessions within a second. `peer.enroll` stores a dialer's per-peer
credential; `peer.revoke` (a dialer's unpair) revokes it and closes that
installation's sessions within a second. A legacy (previous-release) dialer
is answered `PEER_NEEDS_UPGRADE` in its own format and recorded as an
`inbound` notice. Served exchanges are reported to the status hub, so the
accepting host shows its peers too.

## Worker

### Startup

1. Take `owner.lock` (held: `ALREADY_RUNNING`, exit 75).
2. Read the previous `worker.json` (for FUSE recovery).
3. Open the store on this thread, reporting `startup_progress` for every unit
   of work: schema, one-time backfills (chunked, resumable), object
   verification (`Changed` mode: an object whose size and mtime match its
   availability row is trusted; others are hashed), journal validation,
   frontiers, snapshot resume or rebuild, inbox classification. Activation
   is deferred to the background.
4. Recover pending saves, each on its own: one whose staging file is
   missing or unreadable, or that belongs to another branch, is kept,
   reported (`RECOVERY_REQUIRED`, a degraded store) and retried with the
   pending-save backoff, and the others load. Such a record gates only a
   checkout while it belongs to the active branch (one row per entity:
   after the switch a save of the same file would replace it); one of
   another branch waits for that branch, and its file cannot be opened for
   writing meanwhile. Remove the
   staging files no pending save names and the temporary object files
   (`objects/*.part`) an interrupted run left. Build the worker context.
5. Open the RPC listener, which requires the instance's token (a fresh
   random token, written owner-only to `runtime.token` once a previous
   instance's `runtime.json` is removed, so the token never sits next to a
   dead listener's address), and start the heartbeat (1 Hz); then write
   `runtime.json` and `worker.json` (with the token) atomically. Before its
   upgrade carries the token a connection is bounded: 5 s for the whole
   upgrade (a watchdog) and at most 32 such connections (the oldest is
   closed for a new one); the daemon's control endpoint has the same
   bounds, counted apart from its authenticated connections.
6. Standalone with `--mount`: attach now (a failure is reported, never fatal).
   Supervised: wait for `mount-attach`.
7. Print the Ready line (`{"ready":true, repo, device, instance, pid,
   address, token, version, "ipc":3, mounted, mount_error}`); the daemon
   reads the token from it (its copy in `worker.log` is redacted).
8. Start the maintenance (1 Hz), activator and notifier threads; wait for a
   stop; flush the stop reply; remove the records.

A failure before Ready prints a fatal line and exits 73 (store), 75 (locked),
78 (configuration) or 70 (internal). Store recovery always finishes before the
listener opens. A poisoned engine lock (a panic while holding it) saves every
dirty file, logs, and exits 70; the daemon restarts the worker.

### Locks

| Holder | Engine lock? |
|---|---|
| filesystem callbacks | short holds only (WP5.2). A save snapshots the file's bytes under the lock, hashes and stores them without it, and commits under it with a base check; the commit does not hash the new object again (the verified set, below). The first write to a file larger than 256 KiB copies it to a staging file without the lock. A read of bytes still downloading waits outside it. Work before the lock honours the mount's fence |
| background savers (WP5.2) | the save's two short phases, after the filesystem callbacks waiting for the engine (at most 20 ms each time) |
| activator | slices of ≤ 64 events or 25 ms; yields while filesystem callbacks wait |
| feed RPCs, `objects.*`, `status`, `hello`, `watch` | no: pooled secondary SQLite connections; status from the status cell |
| control ops (mkdir, resolve, checkout, …) | yes |
| maintenance | brief `try_lock` only |

### Mount controller

States: `Disabled`, `Detached`, `Preflight`, `Attaching`, `Attached`,
`Detaching`, `AttachFailed{error, attempt, next_retry}`, `Lost{error}`.
Attach and detach run one at a time on the `mount-ctl` thread; status is read
from a snapshot. Preflight classifies `MOUNT_PATH_MUST_NOT_EXIST`,
`MOUNT_OVERLAPS_*` and `WINFSP_RUNTIME_UNAVAILABLE` as needs-user-action;
everything else is transient. On Linux a stale `fuse.tkfs` mount at the path
is unmounted only when the previous `worker.json` proves it was this folder's.
A graceful detach needs quiet (no open handles, no dirty files), else
`BUSY_VIEW`. A forced detach fences new opens, drains the callbacks in flight
(≤ 10 s), saves every dirty file, stops the native instance and drops the
orphaned handles. Mount errors never touch the store's health. WinFsp binds
the engine once and creates a fresh file system per attach, so a folder can
attach, detach and attach again in the same process.

**The filesystem path** (WP5.2; `runtime::engine` module docs):

- **Saves off the engine lock.** Three phases: snapshot the session's bytes
  under the lock (a copy of at most 256 KiB in memory, or a second handle to
  its staging file, which writers then wait for without the lock); hash,
  write, `fsync` and install the object without it (invariant 1), recording
  in the verified set what it hashed; commit under it with the snapshot's
  bases. A session edited meanwhile stays dirty and saves again. Every
  failure of any phase keeps the file as a pending save; a crash at any step
  leaves the last committed bytes.
- **Write-behind during a burst of closes.** A lone close saves before it
  returns. During a burst (saves running, or one ended less than 100 ms ago)
  a close queues its save for a pool of at most 8 saver threads (64 queued);
  with the queue full a close saves before it returns, so at most 72 closed
  files are uncommitted at any time. `FlushFileBuffers` and fsync(2), a
  rename and a delete wait for the file's save; a graceful stop, a detach,
  a checkout and every control request that needs a quiet engine wait for
  closed files' saves (at most 5 s) instead of answering `BUSY_VIEW`.
- **Deferred creates.** A file created through the WinFsp mount is a dirty
  session until its first save, which commits one event that creates it with
  its content. It is shown by lookups and listings meanwhile (a committed
  entry of the same name wins until the new file's commit makes the name
  conflict), retained, restored and committed like any unsaved file, and
  committed first by anything that would change its name.
  `TKFS_DEFERRED_CREATE=0` turns this off; FUSE creates commit at once.
- **Reads of content still downloading** register demand and wait while the
  object's spool grows; they serve only the verified whole object, and fail
  `CONTENT_UNAVAILABLE` after 60 s without progress, after 240 s in all, or
  at once when a detach or checkout fences the mount (WinFsp:
  `STATUS_DEVICE_NOT_READY`; FUSE: `EIO`). At most half the WinFsp
  dispatcher threads (FUSE: 32 requests) wait at once.
- **FUSE threads.** Reads, writes, fsyncs, flushes and size changes run on
  threads of their own, at most 64 at once; the others wait in line, none is
  refused.

### Inbox and activation

Received events are **staged** in `incoming_events`, never activated by the
exchange that brought them. Each row has a state:

| State | Meaning |
|---|---|
| `ready` | every parent is committed |
| `waiting` | parents missing or still staged (`incoming_waits`) |
| `retry` | a transient error (SQLite busy or locked, I/O); due at `retry_at_ms` (1 s → 5 min) |
| `quarantined` | a validation error (`code`) |
| `blocked` | a quarantined ancestor (`blocked_on` names the root) |
| NULL | written by the previous release; classified at startup |

`pending_active = ready + waiting + retry + unclassified`; quarantined and
blocked rows are reported separately. The legacy `error` column stays set for
quarantined and blocked rows, so an older binary sees what it always saw.
`revalidate` re-queues quarantined, blocked and retry rows.

The **activator** thread waits for a poke (rows staged) or 1 s, then, under
the engine lock, activates one slice: `ready` and due `retry` rows in arrival
order, with point lookups only, each event committed in its own savepoint,
children released from `waiting` in the same slice. Three commit paths:

- **Fast paths** (`core::commit`): content changes, and creations and
  namespace changes that extend the branch's frontier (dependencies equal to
  it, a fresh entity or free slot, a live parent, no cycle), commit
  incrementally, also for remote events, while the branch has no unresolved
  conflict. `TKFS_REMOTE_FASTPATH=0` restores the previous rule for creations
  and namespace changes (local events only, and any conflict row of the
  branch, resolved or not, disables them).
- **Append-mode conflict derivation** (`core::derive`): a commit that needs
  the conflict analysis derives only what the new event adds, under an
  exactness precondition (the old namespace cones are kept and each touched
  cone's relevant set only gains the event); otherwise it derives in full.
  Conflict counting is always over the full-mode antichains.
  `TKFS_CONFLICT_MODE=append|full|verify` (`verify` runs both and fails the
  commit with `CONFLICT_MODE_MISMATCH` on any difference).
- **Off-lock slow path**: an event that needs a full replay stops the slice
  and returns a ticket; the replay is prepared on a read connection without
  the engine lock, then committed under the lock after re-checking every
  validation that depends on the database. If the branch moved meanwhile the
  ticket is stale and re-queued (250 ms → 30 s); after 4 stale outcomes the
  event is committed inline, alone (`starved_since_ms` reports the wait).

**The slow path as built** (WP5.1; `core::derive` and `core::commit`
module docs). A slow commit no longer derives or projects the branch from
scratch:

- **Appended projection.** It projects from the handle's view
  (`project_append`: the new event's registers, then incremental namespace
  passes over the slots and chains the event can change), declining (to a
  full `project`, the base kept as the view) for a base with pending
  events, an entity id with a comma, or an event another event names.
- **Branch index.** A per-branch derivation index kept beside the event
  cache (positions, register versions, cones, competing-cut counts
  memoized per cone) lets append mode read what it used to recompute from
  every event; a successor recounts only the cones it joins.
- **Interned derivation.** Full mode (fallbacks, rebuilds, the oracle)
  works on integers instead of `String` sets, sweeps each cone once and
  enumerates antichains over concurrent sets: a full rebuild of a store with
  9,000 open conflicts takes seconds, not minutes.
- **Writes that follow the change.** Stored rows with the same payload are
  skipped, the view's slots are patched, and the seal trusts the handle's
  own last anchor and reuses its header.
- **The verified set.** `check_commit` hashes a local event's new object
  again only when this process did not verify it moments ago (an in-memory
  set keyed by store and hash, holding the file's size and mtime, filled by
  every installer and by the engine's saves, never trusting a racy mtime or
  an object the index marks corrupt or missing).

`TKFS_CONFLICT_MODE=full` stays the reference: it projects and derives in
full on every slow commit.

A commit that crosses the startup-snapshot interval is committed alone, and
the WAL is checkpointed outside the lock between slices. `Store::
verify_derived_state` (the oracle) rebuilds the derived tables in a savepoint,
compares and rolls back; the fuzz and qualification suites run it.

### Status cell and heartbeat

The worker's `status` RPC answers from a status cell without taking any lock
(it is refreshed by mutating RPCs, after every activation slice and by
maintenance at 1 Hz; content counters of large stores are refreshed on a
background thread, `content_pending: true` meanwhile). The heartbeat thread
publishes `WorkerHeartbeat` at 1 Hz from atomics, the cell and the mount
status; `watch{after_seq}` returns as soon as a newer one exists. Maintenance
retries pending saves (1 s → 60 s; also the ones not yet restored),
checkpoints the WAL (truncating a large
copied one), and scrubs stored objects in the background (≤ 32 MiB/s, only
while the activator is idle, objects not verified for 7 days).

### Startup snapshots

Each branch has a disposable startup snapshot (format 2): an anchor row in
`startup_snapshots` (identity, the acceptance cursor, the frontier and the
journal binding, checksummed) plus, for a large projection, sealed rows. A
start resumes it and replays at most 1,024 events after it; anything else
(missing, damaged, incompatible, edits outside the incremental rules, a row
that does not match the anchor's digest) falls back to a full rebuild.

- **Layouts.** While the anchor stays within 1 MiB it holds the whole
  projection and every conflict row (**inline**, as format 1 did). A larger
  projection is sealed as **rows**: the anchor keeps only the projection's
  header and an order-independent digest of the rows, and
  `snapshot_entries` / `snapshot_conflicts` hold one row per entity and per
  conflict.
- **Incremental seals.** Triggers on `current_parts` and `conflicts` mark
  every changed entity and conflict row in `snapshot_dirty`, in the writing
  transaction; a seal rewrites the anchor and only the marked rows, then
  clears the marks in the same transaction, so a seal costs O(change)
  whatever the projection's size. A rebuild (whose change is unknown) seals
  in full, and so does a seal that finds anything unreadable in the snapshot
  tables.
- **Journal binding.** The anchor stores the journal hash and its resumable
  state, so a seal hashes only its own branch's journal rows since the
  previous seal, through the trigger-maintained per-branch index
  `journal_branch(seq, branch)`; a start recomputes the hash from the
  journal and checks it.
- **Cadence.** Seals happen with full-replay commits and periodically every
  `sealed payload / 4 KiB` events (between 128 and 1,024; a rows anchor is
  small, so a large projection seals every 128 events). A restore writes
  only the projection rows that differ from the sealed ones, so an open at
  the tip writes nothing.
- **Stored projection header.** `current_meta` holds the projection's
  conflict and pending lists when they differ from `current_state`'s, so a
  conflict-producing commit writes a delta instead of the whole projection.
  Both it and the rows anchor's header still hold the whole conflict list,
  so a commit that changes it writes O(open conflicts) bytes twice (about
  50 + 90 ms at 13,000 open conflicts; a format 3 that keys them by id is
  CCR-WP5.1-2).
- **Downgrade.** An older binary finds a format-2 anchor incompatible and
  reconstructs every branch at its first open (rewriting `current_state`),
  then seals format 1; it never reads the side tables, which this binary's
  next full seal replaces.

Triggers on `events` keep `journal_sequence` (and `journal_branch`) and
invalidate snapshots even when an older binary writes the journal.

## Protocol v3

### Sessions

The upgrade request carries `X-Tkfs-Sync: 3` and a v3 server echoes it. A
dialer that gets no echo faces the previous release and stops with
`PEER_NEEDS_UPGRADE` before sending anything. A server that gets no header
reads one legacy message (≤ 16 KiB, 5 s), answers the legacy
`PEER_NEEDS_UPGRADE` reply and closes.

Each WebSocket binary message is one frame: an 8-byte prefix (kind, flags,
header length), a JSON header (≤ 16 KiB before authentication, 64 KiB after)
and a body (≤ 1 MiB). Kinds: HELLO, HELLO_ACK, AUTH, AUTH_OK, REQUEST,
RESPONSE, CHUNK, PROGRESS, CANCEL, HEARTBEAT, GOAWAY. Larger bodies continue
in CHUNK frames, reassembled up to the op's limit (16 MiB for
`meta.exchange`); object streams are delivered chunk by chunk. Anything
malformed closes the connection.

**Handshake and authentication.** HELLO (client nonce, installation, role
`control|bulk|probe`, lane, features) → HELLO_ACK (verifier salt, server
nonce, verifier generation, features) → AUTH
(`HMAC-SHA256(K, "client" ‖ transcript)`) → AUTH_OK
(`HMAC-SHA256(K, "server" ‖ transcript)`, verified by the client: mutual).
With the pairing code, `K` is the 32-byte verifier digest: the server stores
it, the client derives it from the salt and its code. The code never crosses
the wire, and a replayed proof fails because both nonces change. The dialer
then checks the server's installation against the catalog
(`PEER_INSTALLATION_CHANGED`). Socket timeouts (5 s per step) exist only
during the handshake.

**Per-peer credentials.** After `pair` has authenticated a probe session
with the code, the dialer sends `peer.enroll` with 32 random bytes `S`; both
sides store `K_peer = SHA-256("tkfs-peer-secret-v1" ‖ S)` in
`peer_credentials`. Later sessions offer the scheme `peer-hmac-v1` in
HELLO's features and run the same mutual proofs keyed by `K_peer`. A missing
or revoked credential is refused with `GOAWAY{PEER_REVOKED}`. A dialer's
unpair sends `peer.revoke` first (best effort); the acceptor revokes the
credential and closes that installation's sessions within 1 s. A rotation
(`reset-pairing-code`) revokes every code session and every per-peer
credential (invariant 21).

**Loopback sessions** (daemon ↔ worker) use the upgrade header
`X-Tkfs-Sync: 3-loopback` and no HELLO or AUTH: the upgrade must carry the
worker's token (`X-Tkfs-Token`), as must its JSON RPC.

**I/O.** One thread per connection owns the socket in non-blocking mode with
a `polling` poller: it always drains reads (TLS buffers internally), writes
from priority queues (control frames first, then response bodies, then data
chunks round-robin per stream), and is the only thread that touches the
socket. Producers block outside it when the data queue (32 MiB) is full. A
HEARTBEAT goes out after 5 s without sending; a connection with nothing
received for 20 s is dead. Each call has its own deadline (and an idle
deadline for streams, reset by PROGRESS); an expired call is cancelled with
CANCEL and fails as `Timeout`. A remote error leaves the session usable; a
lost session makes its calls uncertain, and they are never replayed. GOAWAY
stops new requests; calls in flight finish. On Windows, a session to this
machine's loopback gets a 4 MiB receive buffer and a send buffer sized for
two 10 ms horizons at its drain rate (64 KiB to 4 MiB); any other socket
keeps the system's autotuning (a fixed window would cap a long link). On
Linux unsent bytes are bounded at 256 KiB on every socket, so control frames
never queue behind megabytes of data.

| Op | Lane | Deadline |
|---|---|---|
| `ping` | any | 5 s |
| `catalog.list` | control, probe | 10 s |
| `meta.exchange` | control | idle 30 s (every reply frame restarts it), 30 min cap |
| `objects.pull`, `objects.push` | bulk | idle 30 s |
| `objects.wants`, `objects.report` | bulk | 10 s |
| `peer.enroll` | any (the dialer sends it on lane 0) | 10 s |
| `peer.revoke` | any (lane 0, or a probe session) | 2 s |

### Metadata feed (format 7)

The **feed** of a replica is its committed shared events in local acceptance
order (`journal_sequence`). A peer reads it with a cursor `{epoch, seq, id}`
that the feed's owner validates against its own journal on every exchange;
the epoch is regenerated whenever an event row is deleted, so a cursor never
survives a journal that shrank.

One exchange (dialer D, acceptor A):

1. D's worker builds a request: a page of D's feed after A's last reported
   cursor of it, D's cursor of A's feed, a `need` list (ids A's feed listed
   that D lacks, and missing parents of D's waiting rows; ≤ 256), D's state.
2. A's worker stages D's page, answers a page of its own feed after D's
   cursor (or from the start, `reset`, if that cursor is invalid), serves the
   needs and lists its own.
3. D's worker stages A's page and the served events, records A's cursor.
4. Both workers poke their activators.

Pages hold ≤ 4,096 entries and ≤ 1 MiB of payload. An entry's payload is
omitted when the sender knows the receiver has the event (`peer_known`), and
events over 256 KiB travel only through `need` (up to 8 MiB each). Staging
validates the repository, shared branch (`PRIVATE_BRANCH_REJECTED`),
forged origin, id reuse, sizes and digest; an invalid event is recorded in
`feed_rejected` (never acknowledged). A page whose `after` is not our cursor
is refused (`CursorMismatch`) and nothing is staged.

**Acknowledgements** come only from per-id evidence: when a peer's feed lists
an event whose digest equals ours, the peer committed it (its feed holds only
committed events), so `peer_known(peer, id)` is set and our outbox row is
acknowledged. A cursor never acknowledges; a staged or rejected event never
does; replies are bound to their request (`UNBOUND_PEER_RESPONSE`). A peer
that reports a lower cursor than before (restored from a backup) is
forgotten and re-read, and acknowledgements it withdrew are queued again.

An idle exchange is O(1) (empty pages, point lookups); a catch-up is
O(delta); there are no full-set inventories and no caps.

### Objects v3

Content moves on the bulk lanes, driven by the dialer in both directions:

- **Download**: D's worker lists what it wants (`objects.want`: objects
  referenced by committed or validly staged shared events that are not
  available here, owned by this lane, not leased elsewhere, not in backoff;
  priority: files being opened, then current files, then history; smaller
  first) → `objects.pull` → A's worker reads (`objects.read`, ≤ 1 MiB, 4 in
  flight) → CHUNK frames of ≤ 256 KiB → D's worker writes (`objects.write`,
  4 in flight, tiny objects batched).
- **Upload**: `objects.wants` from A → D's worker reads → `objects.push`
  → A's worker writes.

Objects are sharded across a session's bulk lanes by hash, and worker leases
(60 s, renewed only by progress) keep two sessions or peers from moving the
same object. A lane runs at most 2 repositories at once and makes at most 4
`objects.*` calls on the peer at once (the acceptor's exchange slots per bulk
session); a pull reserves at most its repository's share of the lane's
16 MiB window; one upload round runs at a time, so pushes leave peer calls
for pulls. A source that cannot serve an object gives its lease up: the
dialer's `objects.want` excludes the objects in their backoff for that
source (`exclude`, at most 256) and `objects.release` ends the leases of a
failed or abandoned round, so another session or peer takes the object in
its next round instead of waiting for the lease to lapse. A key whose round
found nothing to do (nothing wanted, offered, excluded or pushed) is
rechecked after 5 s (the sync interval when that is longer); its triggers
(feed tips, content generations, staging) still run it at once.

**Leases of a dead session** (WP5.3) end at once: a failed round releases
its unfinished objects, a lane that ends gives up what its rounds kept, and
the acceptor gives up the leases it offered on a bulk session when that
session ends (the inbound server reports it) or once the offer and its
session have been silent for 5 s (a link that drops every byte). Lease ids
are per lane, so a release is serialized with the next session's exchanges
of that lane. **Demand first**: a busy download round asks its worker every
500 ms which objects readers wait for (`objects.demand`, read-only) and
moves them to the front of its plan, leasing one it had not planned with a
small want; an idle lane runs a round as soon as the demand grows.

**Durability.** A spool's durable offset (`spool_state`) advances only after
fsync; a write must start exactly there, and overlaps are compared byte for
byte. A completed object is hashed again in full, installed (write-through
rename, or rename plus directory fsync), then registered. A crash between
install and registration is repaired by transferring it again.
**Authorization.** A worker serves an object only if it is available, shared
(published or received shared) and referenced by a committed shared event
(`NOT_PERMITTED` otherwise, `NOT_AVAILABLE` if referenced but not here). It
accepts only objects it wants, under the lease of the writing session
(`UNSOLICITED_OBJECT`, `IN_FLIGHT_ELSEWHERE`); a private copy that happens to
be present is still transferred and verified before it is shared.
**Results** are per object: `Completed`, `Progressed`, `Skipped`,
`NotAvailable`, `NotPermitted`, `CorruptAtSource`, `SizeMismatch`,
`Unsolicited`, `Failed`. A hash mismatch is reported to the sender
(`objects.report{CORRUPT_AT_SOURCE}`), which re-verifies and quarantines a bad
copy. Failures back off per (object, source) 30 s → 1 h; an object a source
merely lacks backs off 5 s → 5 min; contention and a busy database are no
failure.

## Status and errors

The **StatusHub** receives events from every producer (folder lifecycles,
worker heartbeats, lane states, exchanges, catalogs, inbound sessions, object
errors) in memory only, and serves a `StatusSnapshot` (schema 2) rebuilt at
most 4 times a second; a 1 Hz tick computes rates (10 s and 60 s EWMAs),
ETAs, stalls and the error display policy. Pure rules in `hub_rules` decide
each (folder, peer) state, the folder aggregate (first match of a precedence
list), the peer connection badge and the stall detector; the README lists
their [meanings](../README.md#reading-the-status). Three claims are separate
fields: metadata caught up, content complete, events acknowledged by peers.

**Errors** (`model::errors`): `code_of` finds the first `SCREAMING_CASE` code
in an error chain, `classify` maps it to a class (unknown codes are
transient), and an `ErrorRecord` keeps its scope, first and last time, count,
next retry and resolution. Contention errors are hidden for 30 s, transient
ones until the 3rd consecutive failure; errors clear on the next success in
their scope; 50 resolved errors stay in the history. `FOLDER_SYNC_PAUSED` is
shown as the paused state, never as an error. Legacy `list`/`inspect`/`peers`
replies keep their shapes, computed from the hub.

**Logs** are JSON lines (`{"at_ms", "level", "code", …}`) with a per-code
rate limit (20 a minute, then a `suppressed` count). The daemon writes
`DATA/logs/orchestrator.log`, rotated at 10 MiB with 5 files kept; a worker's
`worker.log` is rotated when the worker is spawned and the file is larger
than 10 MiB. Fields that name codes, secrets or tokens are redacted. The
`diagnostics` action collects the status, the activity, each folder's view,
the redacted catalog, the ends of the logs and the versions into
`DATA/diagnostics/<time>/`.

## Storage and migration

- `folders.sqlite` and each state's `metadata.sqlite` are only ever extended:
  new tables (`CREATE TABLE IF NOT EXISTS`, each module stamped
  `schema:<module>`), and nullable columns only on `incoming_events`, whose
  writers use named columns. Tables written positionally by older binaries are
  never altered.
- Backfills (object references, availability, the feed index) are chunked,
  resumable and report progress.
- New state tables: `incoming_waits`, `incoming_sources`,
  `object_availability`, `event_objects`, `staged_objects`,
  `object_error_state`, `projection_version`, `feed_journal`, `feed_in`,
  `feed_out`, `feed_needs`, `feed_rejected`, `feed_remaining`, `spool_state`,
  the `journal_epoch` setting.
- Nothing deletes user data or history; legacy states from `6298d0b` open and
  upgrade (checked by every integration).

## Invariants and their guards

| # | Invariant | Mechanism | Guard |
|---|---|---|---|
| 1 | Verified, durable install before an object is shared | full re-hash before install, write-through install, availability after install; a local commit skips the re-hash only of bytes this process verified moments ago, unchanged since (the verified set) | `objects_v3_worker::corrupt_spool_never_installed`, `availability_index`, `core::commit::tests::a_later_corrupt_verdict_wins_over_the_verified_set` |
| 2 | Overlaps verified, no gaps, offsets advertised only after fsync | `spool_state.durable` after fsync; appends start exactly there | `objects_v3_worker::crash_mid_object_resumes_from_durable` |
| 3 | Register only after install; a crash between them recovers | re-transfer of a non-shareable object | `crash_between_install_and_register_retransfers` |
| 4 | `synchronous=FULL`, atomic commits | side rows and triggers inside the commit's savepoint | every store suite |
| 5 | Snapshots are disposable | checksummed, journal-bound, rebuild fallback | `startup_snapshots`, `w3_integration` |
| 6 | Events are immutable | derived tables only; digests computed, never stored | `EVENT_ID_REUSED` tests |
| 7 | Object sizes are immutable | `OBJECT_SIZE_CHANGED` at staging, commit and write | `offlock_commit::size_change_between_prepare_and_commit_rejected` |
| 8 | Dirty files survive | force detach saves, poison saves, shutdown leaves busy workers; a save in progress (off the lock, or queued by write-behind) is a dirty session until its commit | `worker_lifecycle::force_stop_persists_dirty`, `poison_exit_persists_dirty`, `local_fs_throughput::a_crash_at_every_step_of_a_save_keeps_the_last_committed_bytes` |
| 9 | Causal activation | `incoming_waits`; activation only with every parent committed | `activation_inbox` |
| 10 | Acks only for committed events, bound to their request | per-id evidence from the peer's committed feed | `feed_v7::staged_never_acks`, `rejected_never_acks`, `third_peer_staged_not_acked` |
| 11 | Deterministic conflicts | fast paths and append mode are restrictions of full mode; `verify` mode; oracle fuzz | `derive_oracle`, `activation_inbox` |
| 12 | Private branches never leave | the feed lists shared branches only; `PRIVATE_BRANCH_REJECTED` | `feed_v7::private_never_served` |
| 13 | Only shared, provenanced objects are sent | read authorization triad; writes only for wanted objects | `replication_regressions::paired_peer_cannot_disclose_private_cas_using_a_hash_only_manifest`, `objects_v3_worker::hash_only_request_not_permitted` |
| 14 | No forged origin | point check at staging | `feed_v7::forged_origin_rejected` |
| 15 | Routes bound to the authenticated peer | inbound routes from the authenticated installation | `inbound_v3::route_binding_uses_the_authenticated_installation` |
| 16 | Uncertain requests are never replayed | fresh requests after a loss; `ControlClient` never resends a mutation | `session_core::lost_request_not_replayed` |
| 17 | A remote error keeps the session | `ok:false` responses leave it usable | `session_core::remote_error_keeps_conn` |
| 18 | Control carries no object bytes; bulk never activates or acknowledges | op/role table | `inbound_v3::role_violations_and_route_violations_keep_their_codes`, `objects_v3_lanes::control_carries_no_object_bytes` |
| 19 | One owner per object per session | lane sharding, worker leases, object locks; a release ends only the releasing lane's own leases and is serialized with that lane's next session | `objects_v3_worker::duplicate_sessions_transfer_once`, `objects_v3_lanes::a_reap_never_ends_the_lease_of_the_lanes_next_session` |
| 20 | Fail closed against old peers | `X-Tkfs-Sync` both ways | `session_handshake::legacy_both_directions`, `qual_wp3_1_transport` |
| 21 | Salted verifier; per-peer credentials; rotation invalidates sessions and credentials; no code, secret or token in status, logs or diagnostics | HMAC over the verifier digest or `K_peer`; generation → GOAWAY; rotation and unpair revoke `peer_credentials`; redaction of logs and bundles | `session_handshake`, `status_hub`, `security`, `logging` |
| 22 | Bounded input | frame, header, page, need and body limits; pre-auth limits | `session_core::bounds`, `inbound_v3::preauth_flood_keeps_memory_flat` |
| 23 | One owner per data directory and per state | `orchestrator.lock`, `owner.lock`, verified kills | `folder_controller::adoption_*` |
| 24 | Recovery before RPC | the listener opens after the store and pending saves | `worker_lifecycle::rpc_after_recovery` |
| 25 | Workers never dial | worker sessions are loopback and accepted only, with the worker's token | review, `security` |
| 26 | Durable intent; the gate | `desired=0` persisted first; gate cleared only after attach | `folder_controller::gate_holds_the_attach_until_evidence_and_clears_only_after_attached`, `orchestrator::received_mount_waits_for_complete_metadata_across_daemon_restart` |
| 27 | Mount safety | path validation; FUSE recovery only for mounts proven ours | `mount_lifecycle::foreign_mount_untouched` |
| 28 | Startup progress from the recovering thread | backfills report from the opening thread | `worker_lifecycle::backfill_reports_progress` |
| 29 | Interruptible waits; the shutdown reply is flushed | `Cancel` everywhere; flush before exit | `qual_wp2_1_lifecycle` |
| 30 | Three separate claims; status never blocks or writes | separate fields; lock-free hub and status cell | `status_hub`, `worker_concurrency::status_never_blocks` |
