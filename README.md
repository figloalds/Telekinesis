# Telekinesis

Telekinesis (`tkfs`) keeps project folders in sync between computers. Each
folder is a durable causal filesystem: it can be mounted (WinFsp on Windows,
FUSE on Linux) or run headless, and a daemon on each computer syncs it with
the daemons it is paired with over WebSocket (`ws://`, or `wss://` through a
TLS proxy).

```text
mounted folder <-> worker --+                                  +-- worker <-> mounted folder
                            |-- daemon <== tkfs-peer/3 ==> daemon --|
mounted folder <-> worker --+   (lane 0 control, lanes 1.. bulk)   +-- worker (headless)
```

- A **worker** process owns one folder: its store, its mount, and the
  activation of the events it receives. Workers never talk to the network.
- The **daemon** (`tkfs orchestrator`) supervises the workers (restart with
  backoff, adoption after a crash), keeps the folder catalog, talks to the
  peers, and serves status to the CLI and the desktop.
- **Peers** are other daemons. Each paired peer gets a pool of connections:
  lane 0 carries metadata, the other lanes carry file content.

**If you are upgrading from the previous release, read
[Upgrading](#upgrading-from-the-previous-release) first.**

Contents:

- [Quick start](#quick-start)
- [Upgrading from the previous release](#upgrading-from-the-previous-release)
- [Reading the status](#reading-the-status)
- [Error classes and what to do](#error-classes-and-what-to-do)
- [Runbooks](#runbooks)
- [Filesystem behaviour](#filesystem-behaviour)
- [Building](#building)
- Reference: [CLI](docs/CLI.md), [configuration](docs/CONFIGURATION.md),
  [architecture](docs/ARCHITECTURE.md), [Linux](docs/LINUX.md),
  [validation](docs/VALIDATION.md)

## Quick start

On each computer:

```sh
tkfs init -f orchestrator.toml               # writes a configuration with the defaults
tkfs -f orchestrator.toml orchestrator       # the daemon (or `tkfs gui` on Windows)
```

Create a folder (in another terminal):

```sh
tkfs -f orchestrator.toml manage create Projects            # mounted at mounts/Projects
tkfs -f orchestrator.toml manage status
```

Pair two computers. The host that accepts connections needs a listener
(`[network] listen`, see [configuration](docs/CONFIGURATION.md)); a host that
only dials needs none, and one pairing syncs both directions.

```sh
# On the server: print a pairing code (shown once).
tkfs -f orchestrator.toml manage reset-pairing-code
# On the desktop: pair (paste the code), list the server's folders, receive one.
tkfs -f orchestrator.toml manage pair wss://files.example.com
tkfs -f orchestrator.toml manage remote-list wss://files.example.com
tkfs -f orchestrator.toml manage receive wss://files.example.com REPO_ID Projects
tkfs -f orchestrator.toml manage watch
```

A received folder syncs at once; its mount appears when the initial sync from
the server is complete (`manage mount-now` mounts it earlier). The desktop
(`tkfs gui`) does the same through **New folder**, **Pair with a host**,
**Browse folders** and **Settings**.

## Upgrading from the previous release

This release replaces the network protocol, the daemon's internals and the
status model of the previous release (commit `6298d0b`, whose documentation
called its sync protocol "peer protocol 6" with "bulk protocol 1"). Both
binaries print `tkfs 0.1.0` for `--version`; the new one has
`tkfs manage status`, and its daemon reports a version such as
`0.1.0+45690540870d` in `manage status` and in `DATA/orchestrator.json`.

### Upgrade both ends together

The new daemons speak `tkfs-peer/3` and **fail closed** against the previous
release, in both directions. Nothing syncs between an upgraded daemon and an
old one; local folders on each side keep working.

- **An upgraded daemon dialing an old one** finds no `X-Tkfs-Sync` answer and
  stops before sending anything (the pairing code is never offered). The peer
  shows **Peer needs upgrade** in the desktop and
  `needs upgrade (incompatible protocol)` in `manage status`
  (`PEER_NEEDS_UPGRADE`); it is retried every 10 minutes. Pairing a new
  host with an old one fails the same way:

  ```text
  PEER_NEEDS_UPGRADE: ws://10.0.0.5:43180 runs the legacy peer protocol (no X-Tkfs-Sync echo); upgrade the remote orchestrator. The pairing code was not sent
  ```

- **An old daemon dialing an upgraded one** is answered
  `PEER_NEEDS_UPGRADE: remote runs tkfs peer protocol 3; upgrade this
  orchestrator` (its `manage peers` shows it on every lane). The upgraded
  daemon lists the attempt among its own errors:
  `[incompatible] PEER_NEEDS_UPGRADE: inbound connection from …`.

Upgrade the host that is dialed (the server) first, then the one that dials
(the desktop). If you upgraded the dialer first, it waits up to 10 minutes
before it tries the upgraded server again: restart its daemon to reconnect at
once.

### Step by step: a desktop and a server behind a TLS proxy

Have the new binary for both machines. The upgrade only adds tables and
columns, and every one-time step resumes after an interruption; still, a copy
of each data directory (the configuration's `data_directory`:
`folders.sqlite` and `states/`) taken while its daemon is stopped makes a
rollback possible. The configuration file does not change (it stays
`format_version = 2`).

1. **Server first.** With the Docker example (`examples/docker-compose.yml`:
   the binary `./tkfs` sits in the bind-mounted directory, the daemon is the
   container's main process and the container restarts by itself), put the
   new binary in place under the old name, then ask the old daemon to shut
   down with the old binary:

   ```sh
   mv tkfs tkfs.previous             # the running daemon keeps its copy
   cp /path/to/new/tkfs tkfs && chmod +x tkfs
   docker compose exec tkfs-daemon ./tkfs.previous manage shutdown
   ```

   The old daemon stops its folders cooperatively and exits; Docker restarts
   the container, which now runs the new binary. (The previous release does
   not handle SIGTERM, so `docker compose stop` would end it only by killing
   it after the timeout.) To take a backup first, keep the container from
   restarting (`docker update --restart=no tkfs_daemon` before the shutdown),
   copy the directory, then `docker update --restart=unless-stopped
   tkfs_daemon` and `docker compose up -d`. Without Docker:
   `tkfs -f orchestrator.toml manage shutdown` with the old binary, replace
   the binary, start the daemon.
2. **Then the desktop.** Close the desktop window, then shut the old daemon
   down with the old binary:

   ```sh
   tkfs -f orchestrator.toml manage shutdown
   ```

   On Windows an executable cannot be replaced while any `tkfs.exe` process
   runs, workers included. A worker with files open refuses to stop and is
   left running: close the files, then `tkfs --runtime
   DATA\states\STATE\runtime.json stop`, until no `tkfs.exe` is left in Task
   Manager. Replace the binary and start `tkfs gui` (or
   `tkfs -f orchestrator.toml orchestrator`).
3. **Watch** both sides until every folder reads `Up to date`:

   ```sh
   tkfs -f orchestrator.toml manage watch
   docker compose exec tkfs-daemon ./tkfs manage watch
   ```

   If a worker of the previous release is still running when the new daemon
   starts, the new daemon asks it to stop and starts the new worker once it
   has exited; meanwhile the folder stays `starting` and shows
   `PREVIOUS_WORKER_STOP_TIMEOUT` after 10 s.

The TLS proxy needs no change if it forwards WebSocket upgrades with their
headers in both directions (nginx does by default): the new protocol adds the
`X-Tkfs-Sync` header to the upgrade request and its answer. A proxy that drops
it makes the server look like an old release (`PEER_NEEDS_UPGRADE`). See
[configuration](docs/CONFIGURATION.md#remote-access-through-a-tls-proxy).

### What migrates automatically

Nothing needs to be exported, re-created or re-paired:

- **Configuration**: unchanged (format 2, same fields).
- **Catalog** (`DATA/folders.sqlite`): opened in place on the first start.
  New tables are added beside the old ones (folder intents, initial-sync
  gates, peer metadata); existing tables are never altered. Folders, mount
  paths, desired-running flags, pairings (endpoints, codes, installations,
  pool sizes) and this host's pairing code are kept. A folder that was still
  waiting for its initial sync keeps waiting under the new gate.
- **Folder states** (`DATA/states/<id>/`): opened in place by their worker.
  New tables and indexes are added and filled once; the journal, the objects
  and every received event are kept. Nothing deletes user data or history.

### What to expect on the first start

Each folder's worker does some one-time work before it reports Ready. The
folder shows `starting for … · <phase> (step N)` meanwhile (desktop:
`Starting — <phase> (seq N)`):

| Phase | One-time work |
|---|---|
| `indexing object references` | indexes which events reference which objects |
| `verifying available objects` | **hashes every stored object once** (the new availability index); proportional to the folder's size on disk |
| `indexing the feed` | indexes the shared journal for the new metadata feed |
| `recovering incoming events`, `classifying incoming events` | classifies the events the previous release had received but not activated (see below) |
| `reconstructing snapshot`, `replaying snapshot suffix` | rebuilds or resumes the folder's startup snapshot |

The daemon only kills a starting worker that reports no progress for 30 s;
these phases report progress continuously, so large folders finish. Later
starts skip all of it (a restart with 20,000 objects reaches Ready in under
3 s on the development machine).

Received events that the previous release had not activated are classified
on the first start:

- events whose parents are all here become **ready**; the background
  activator commits them after Ready;
- events whose parents are missing **wait for their parents**, which are
  requested from the peers automatically;
- events the previous release quarantined because of a temporary database or
  disk error (it quarantined those too) are classified again like any other;
- events it quarantined for a real validation failure stay **quarantined**,
  with their descendants **blocked** (see
  [quarantined events](#quarantined-or-blocked-events)).

### What to expect during the first catch-up

- **Activation runs in the background.** The worker reports Ready before its
  backlog is activated; the mount, the CLI and the status answer while the
  activator commits events in short slices (at most 64 events or 25 ms each).
  `manage status` shows `inbox N to activate (…)` going down and
  `activation running · N activated (fast, slow) · last …`. Remote events
  that extend the history linearly take the fast path (about 1 ms each on the
  development machine); events concurrent with local edits take the full
  replay (`slow`), prepared outside the lock.
- **Metadata is exchanged once in full.** The first exchange with each peer
  reads its whole feed from the beginning, in pages of up to 4,096 events:
  ids and digests only for events the sender knows the other side has, full
  events for the rest. After that, an idle exchange costs a few kilobytes.
- **Files move before their events are activated.** Content of every
  validly received event is downloaded while the activator works through the
  backlog, current files before history, files someone is trying to open
  first.
- **Acknowledgements catch up.** `journal … N not yet acknowledged by a peer`
  goes down as each peer's feed shows the events committed there; a peer that
  is still activating them acknowledges them as it commits them.

The folder's `sync` row tells you where it is:

| You see | Meaning |
|---|---|
| `Waiting for initial sync from <peer>: 412 events remaining at source, …` | a received folder whose mount waits for the initial sync; the reasons say what is left |
| `Syncing metadata · 1 of 1 peer · N events left` | events to send, receive or activate on either side |
| `Downloading · … left · rate · ETA`, `Uploading · N files to upload` | file content is moving |
| `Syncing` | more than one kind of work |
| `Stalled N min — CODE …` | work remains, the peer is connected, nothing progressed for the stall threshold (at least 60 s): see [stalled](#stalled) |
| `Blocked — N events quarantined, …` | only quarantined or blocked events remain: see [quarantined events](#quarantined-or-blocked-events) |
| `Up to date · 1 of 1 peer` | done; `· N events rejected` is shown when the peer refused events (see below) |

Every state is listed under [Reading the status](#reading-the-status).

### Recovering a replica that was stuck on the previous release

The previous release could get stuck in ways this one removes: activation
that replayed the whole journal per event, a 15-second request timeout that
tore down every lane, pages that re-sent the same window of received but
unactivated events, inventory caps, and quarantines that spread. Symptoms
were a large `outgoing` (unacknowledged) count on one side, a large
`incoming` (pending) count on the other, and lanes that stayed connected
without progress. After upgrading both ends:

1. **Let the first start finish** (above) and the folder reach Running. Do not
   delete queues, stores or `metadata.sqlite` rows, and do not edit
   acknowledgements: the backlog is the data that has to arrive.
2. **Watch the receiving side's inbox** (`tkfs manage watch`):
   - `N to activate (… ready)` goes down while `activation` shows events
     committed. That is the catch-up. On the development machine a
     10,000-event catch-up between two daemons takes about 18 s, and a
     150,000-event one 12 to 17 minutes (150 to 200 events/s), most of it
     activation.
   - `retrying` rows had a temporary error and retry by themselves (1 s up to
     5 min).
   - `waiting for parents` rows need events this replica does not have yet.
     They are requested from every peer that shares the folder (up to 256 per
     exchange, again every 60 s while a peer answers that it has none). If
     they stay waiting, the parents may be missing everywhere: see step 5.
   - `quarantined` / `blocked` rows failed validation: see
     [quarantined events](#quarantined-or-blocked-events).
3. **Watch the sending side's journal**: `N not yet acknowledged by a peer`
   goes down as the receiver commits events. Each peer row explains what is
   left: `N to send`, `N pending at the peer` (received there, not yet
   activated). Events the receiver refused show in the receiver's own status
   (`N rejected` in its peer row) and stay unacknowledged here: see
   [quarantined events](#quarantined-or-blocked-events).
4. **Check content**: `content … missing (…)` and `history N of M available`
   go down while the peer row shows `download … left · rate · ETA`.
   `content.complete` (`all N files available`), `metadata caught up` and
   `all acknowledged by peers` are three separate claims; the folder is fully
   recovered when all three hold on both sides.
5. **If events wait for parents that no replica has** (the folder stays
   `Syncing metadata` and then `Stalled`, the inbox shows `waiting for
   parents`), compare both replicas with the read-only script (run it on each
   machine against the folder's state directory, never the mount):

   ```sh
   python scripts/inspect_metadata.py --state DATA/states/STATE --output local.json
   python scripts/inspect_metadata.py --compare local.json remote.json
   ```

   `missing_parents_absent_on_both` counts required events that neither
   replica holds, even in its inbox. They can only come from another replica
   or a backup of one: pair a host that has them, or restore a state that
   has them and let it sync. The waiting events activate as soon as their
   parents arrive.
6. **If a folder's mount fails** after the upgrade (for example an empty
   directory left at the mount path by a container), the folder still syncs
   headless: see [mount failed](#mount-failed).

## Reading the status

`tkfs manage status` (or `watch`) and the desktop show the same snapshot.
[CLI](docs/CLI.md#status) explains each row of the text form.

### Folder sync states

A folder's state is the first of these that applies; peers that do not share
the folder are ignored.

| State | CLI | Desktop chip | Meaning |
|---|---|---|---|
| Stopped | `stopped` | `Stopped [Start]`, or `Not syncing — <lifecycle>` | The folder is stopped, or its worker is not running yet (starting, restarting, stopping). |
| Needs attention | `needs attention` | `Needs attention: <reason> [action]` | The worker failed (crash loop, start failure), the initial-sync gate is blocked, the mount failed with an error you must fix, or a folder error needs action. |
| Paused | `paused` | `Paused [Resume]` | Sync is paused for this folder (`manage resume`). |
| Not shared | `not shared` | `Not shared with any peer` | No paired peer has this repository. |
| Stalled | `stalled` | `Stalled N min — CODE …` | A peer that shares the folder is connected and has work left, but nothing progressed for the stall threshold. |
| Initial sync | `initial sync` | `Initial sync from <peer> · N of M events` | A received folder waits for its initial sync before mounting. |
| Syncing metadata | `syncing metadata` | `Syncing metadata · N left` | Events to send, receive or activate. |
| Downloading | `downloading` | `Downloading 1.2 GiB of 3.0 GiB · 3.4 MiB/s · ETA 6 min` | File content to receive. |
| Uploading | `uploading` | `Uploading 120 files` | File content to send. |
| Syncing | `syncing` | `Syncing · ↓ … · ↑ …` | Several kinds of work, or peers doing different ones. |
| Blocked | `blocked` | `Blocked — 3 events quarantined [Revalidate]` | Only quarantined or blocked events remain. |
| Backing off | `backing off` | `Retrying in 12 s — CODE` | Every sharing peer is retrying after an error. |
| Offline | `offline` | `Waiting for <peer> (offline 5 min)` | No sharing peer can be synced with now (offline, connecting, backing off, needs upgrade, unauthorized, or its folder not serving). |
| Up to date | `up to date` | `Up to date` | Nothing left; `· N of M unreachable` when some peers cannot be reached, `· N events rejected` when a peer refused events. |

### Per-peer states

Each folder lists every peer that shares it, with its own state:

| State | Meaning |
|---|---|
| `paused` | Sync is paused for this folder. |
| `needs upgrade` | The peer runs the previous release (`PEER_NEEDS_UPGRADE`) or another incompatible version. |
| `unauthorized` | The pairing code was refused or rotated: pair again. |
| `offline`, `connecting` | The peer's connection is down, or being dialed. |
| `backing off (retry in …)` | The connection waits to redial (1 s up to 60 s), or an exchange for this folder failed and is retried. |
| `waiting for the local worker` | Our folder is not running. |
| `waiting for the peer's folder` | The peer's copy is stopped, starting, paused or unavailable (`REMOTE_WORKER_STARTING`, `FOLDER_SYNC_PAUSED`). Not an error: it resumes by itself. |
| `stalled` | Connected, work left, no progress for the stall threshold. |
| `blocked` | An error that needs you on this pair, or only quarantined/blocked events left. |
| `syncing metadata`, `downloading`, `uploading`, `syncing` | Work in progress. |
| `up to date` | Nothing left with this peer. |

The metadata line of a peer: `to receive` (events of the peer's feed we have
not read yet), `to send`, `received, not yet activated` (here), `pending at
the peer` (received there, not activated yet), then events received from
this peer that failed here: `quarantined`, `blocked` (behind a quarantined
one) and `rejected` (refused before staging); and `caught up` / `not caught
up`, `last exchange`.

### Peer connection

| CLI | Desktop | Meaning |
|---|---|---|
| `online for …` | `Online` | At least one lane (or one inbound session) is up. |
| `connecting` | `Connecting…` | Dialing or authenticating. |
| `backing off (retry in …)` | `Backoff (retry in 12 s)` | Every lane waits to redial after a failure (1 s doubling up to 60 s, reset after 30 s connected). |
| `offline for …` | `Offline N min` | No lane and no inbound session. |
| `needs upgrade (incompatible protocol)` | `Peer needs upgrade` | The peer runs the previous release or an incompatible protocol; rechecked every 10 min. |
| `unauthorized: pair again with the current code` | `Unauthorized — re-pair` | The peer refused our pairing code (rotated or wrong); rechecked every 60 s. |
| `revoked` | `Revoked` | Reserved for per-peer revocation; nothing sets it in this build. |

Direction: `outbound` (we dial it), `inbound` (it dials us; we have no
pairing record for it), `both ways`. Lanes: lane 0 is the control lane
(metadata), lanes 1… are bulk lanes (content). Lane states: `idle`,
`dialing`, `handshaking`, `ready`, `backing off`, `incompatible`,
`unauthorized`, `closed`.

### Worker lifecycle

| Phase | Meaning |
|---|---|
| `stopped` | No worker; `stopped (start pending)` if it should run. |
| `starting for … · <phase> (step N)` | Recovering the store; killed only if it reports no progress for 30 s (`WORKER_START_TIMEOUT`). `adopting the running worker` after a daemon restart. |
| `running · pid N` | Serving; `adopted` when this daemon took over a worker a previous daemon started; `degraded: the engine lock is stalled` when one operation has held the store for more than 120 s (never killed for it). |
| `stop blocked` | A stop was refused because files are open or unsaved (`BUSY_VIEW`); retried every 2 s. `force-stop` ends it. |
| `stopping` | Stop in progress (graceful, then 10 s for the exit, then a verified kill). |
| `restarting in N s` | The worker exited or failed to start; it restarts after 1 s doubling up to 5 min (±20 %). |
| `failed` | Not restarted until you act: `CRASH_LOOP` (5 failures in 10 min), a fatal store error, or `PREVIOUS_WORKER_UNRESPONSIVE`. |

### Mount

| CLI | Desktop | Meaning |
|---|---|---|
| `attached at PATH` | `Mounted` | Mounted. |
| `attaching` | `Mounting…` | Mount in progress. |
| `attach failed … · retry in N s` | `Mount failed · retry in N s` | A temporary mount error, retried 2 s up to 120 s. |
| `attach failed … · waiting for you to fix it` | `Mount failed [Retry]` | An error you must fix (`MOUNT_PATH_MUST_NOT_EXIST`, `MOUNT_OVERLAPS_*`, `WINFSP_RUNTIME_UNAVAILABLE`); then `manage attach`. |
| `lost` | `Mount lost [Mount again]` | The mount went away under the worker. |
| `waiting for the initial sync before mounting at PATH` | `Waiting for initial sync [Mount now]` | The initial-sync gate holds the mount. |
| `detached on request` | `Detached [Attach]` | You detached it (`manage attach` mounts it again). |
| `headless (no mount path)` | `Headless` | No mount configured. |

A mount problem never stops sync: the folder keeps syncing headless.

### Initial-sync gate

A folder received from a peer mounts only after its initial sync from that
peer (the *source*):

| CLI | Meaning |
|---|---|
| `waiting for the initial sync from <peer> for … : <reasons>` | Holding the mount. Reasons: `no metadata exchange with the source yet`, `N events remaining at source`, `N requested from source`, `N waiting for parents`, `N not yet activated`, `source has N unactivated events`. |
| `blocked: events from <peer> are quarantined or rejected` | The source is caught up, but some of its events failed validation (`GATE_BLOCKED`, needs you). |
| `initial sync complete` | Satisfied; the mount attaches and the gate is removed. |
| `overridden: mounting before the initial sync finished` | `manage mount-now` was used. |

If the source is unpaired, the gate switches to another peer that shares the
folder.

### Three separate claims

- **Metadata caught up** (per peer): we have read the peer's whole feed and
  nothing from it waits to be activated.
- **Content complete**: every file referenced by shared history is here
  (`content … all N files available`, `history … available`).
- **Acknowledged by peers**: `journal … all acknowledged by peers` means a
  peer has committed every event we created; it is not a backup of the file
  bytes.

### Stall detection

A pair is `stalled` when it has work left, it can make progress (shared, sync
enabled, both folders running, the peer connected), and nothing progressed for
`max(60 s, 20 × sync_interval_ms)` (`TKFS_STALL_AFTER_MS` overrides). The
culprit (the last error's code and object or event) is named.

## Error classes and what to do

Every error has a code (`SCREAMING_CASE`), a class and a scope (daemon,
folder, mount, peer, lane, folder-peer pair, object, event). Errors clear on
the next success in the same scope; the status keeps the last 50 with when
they were resolved.

| Class | Retried | Shown | What to do |
|---|---|---|---|
| `contention` (desktop: busy) | automatically, 50 ms to 1 s; never counted as a failure | only if it lasts 30 s | Nothing. |
| `transient` (temporary) | automatically, with backoff (lanes 1 s → 60 s, objects 30 s → 1 h, events 1 s → 5 min) | after the 3rd consecutive failure | Usually nothing; if it persists, read the message (network, disk). |
| `needs_user_action` (needs action) | no (pairing errors are rechecked every 60 s) | always, with an action | Follow the suggestion below. |
| `incompatible` | rechecked every 10 min or on a change | always | Upgrade the side that runs the older release. |
| `fatal` | no, within its scope only (one event, one object, one folder) | always | Read the message and `worker.log`; see below. |

Common codes:

| Code | Class | What to do |
|---|---|---|
| `PEER_NEEDS_UPGRADE`, `PEER_PROTOCOL_UNSUPPORTED` | incompatible | Upgrade the other host ([runbook](#peer-needs-upgrade)). |
| `INVALID_PAIRING_CODE`, `PAIRING_CODE_CHANGED`, `PAIRING_CODE_NOT_SET` | needs action | Pair again with the host's current code ([runbook](#unauthorized-after-a-code-rotation)). |
| `PEER_INSTALLATION_CHANGED` | needs action | Another installation answers at that address (the host was reinstalled or the address moved): pair again (`manage pair ENDPOINT`) if that is intended. |
| `CANNOT_PAIR_WITH_SELF` | needs action | That address is this daemon. |
| `MOUNT_PATH_MUST_NOT_EXIST` | needs action | Remove the (empty) directory at the mount path or choose another path, then `manage attach` ([runbook](#mount-failed)). |
| `MOUNT_OVERLAPS_DATA`, `MOUNT_OVERLAPS_FOLDER`, `MOUNT_OVERLAPS_STATE` | needs action | Choose a mount path outside the data directory and other folders' mounts (`manage configure-mount` while stopped). |
| `WINFSP_RUNTIME_UNAVAILABLE` | needs action | Install WinFsp, then `manage attach`. |
| `CRASH_LOOP` | needs action | Read `last exit` and `worker.log`, fix the cause, `manage retry` ([runbook](#crash-loop)). |
| `PREVIOUS_WORKER_UNRESPONSIVE` | needs action | An older worker is alive but does not answer: `manage force-stop`, then `manage start`. |
| `STOP_BLOCKED_BY_OPEN_FILES`, `BUSY_VIEW` | needs action / contention | Close the files open in the folder, or `manage force-stop` (unsaved files are kept). |
| `GATE_BLOCKED` | needs action | `manage revalidate`, or `manage mount-now` ([runbook](#initial-sync-gate-blocked)). |
| `ALREADY_RUNNING` | needs action | Another daemon or worker uses this data: stop it first. |
| `NAMESPACE_HISTORY_LIMIT` | needs action | The folder's concurrent history is too large to analyse: resolve its conflicts. |
| `REMOTE_WORKER_STARTING`, `FOLDER_SYNC_PAUSED` | transient (shown as waiting) | The peer's folder is starting or paused; nothing to do here. |
| `CONTENT_UNAVAILABLE`, `OFFLINE_OBJECT_MISSING` | transient | A file's bytes are not here yet; they are downloading (or no connected peer has them). |
| `CORRUPT_OBJECT` | transient | A received copy failed verification; it is downloaded again. |
| `CORRUPT_AT_SOURCE`, `SIZE_MISMATCH`, `OBJECT_SIZE_CHANGED` | fatal (that object) | The sender's copy is damaged; it re-verifies and quarantines it. Fix or restore the file at the source. |
| `EVENT_ID_REUSED`, `FORGED_EVENT_ORIGIN`, `INVALID_EVENT`, `CAUSAL_CYCLE`, `INVALID_CAUSAL_PARENT`, `QUARANTINED_CAUSAL_ANCESTOR`, `PRIVATE_BRANCH_REJECTED`, … | fatal (that event) | Quarantined or rejected: see [quarantined events](#quarantined-or-blocked-events). |
| `STORE_IDENTITY_MISMATCH`, `INVALID_STORE_METADATA`, `STATE_NOT_INITIALIZED`, `ENGINE_POISONED` | fatal (that folder) | The folder's store needs attention: read `worker.log`; `manage retry` restarts it. |
| `ORCHESTRATOR_NOT_RUNNING` | transient | No daemon answers for this configuration: start one. |
| `ORCHESTRATOR_STOPPING` | transient | The daemon is shutting down. |
| `CONNECTION_LOST`, `REQUEST_TIMEOUT` | transient | A management command may or may not have run: check `manage status` before repeating it. |

## Runbooks

### Peer needs upgrade

The peer badge reads **Peer needs upgrade** (`PEER_NEEDS_UPGRADE`): the other
host runs the previous release, or its TLS proxy drops the `X-Tkfs-Sync`
header.

1. Upgrade the other host ([upgrading](#upgrading-from-the-previous-release)).
2. On this host, either wait (the peer is rechecked every 10 minutes) or
   restart this daemon, or pair again (`manage pair ENDPOINT` with the code),
   which reconnects at once.
3. If the other host is already upgraded, check its proxy forwards the
   `X-Tkfs-Sync` header both ways.

### Unauthorized after a code rotation

`manage reset-pairing-code` on a host closes every session that used the old
code within a second. Hosts that dial it show **Unauthorized — re-pair**
(`INVALID_PAIRING_CODE` or `PAIRING_CODE_CHANGED`) and retry every 60 s,
which keeps failing until they have the new code.

1. On the host that was rotated, use the code it printed (it is shown once; if
   it is lost, run `manage reset-pairing-code` again and use the new one for
   every dialer).
2. On each dialing host: `tkfs manage pair ENDPOINT` and paste the code. The
   lanes reconnect at once.

### Quarantined or blocked events

`inbox … N quarantined · M blocked`, a folder `Blocked — N events
quarantined`, or a peer row with `quarantined`/`blocked`: received events
failed validation (`fatal` class, e.g. `INVALID_EVENT`,
`INVALID_CAUSAL_PARENT`), and their descendants are blocked behind them. The
rest of the folder keeps syncing.

1. Read the codes. The status shows the counts; the read-only inspection
   script lists each quarantined event with its error (`quarantined` in its
   report; blocked events name their quarantined ancestor):

   ```sh
   python scripts/inspect_metadata.py --state DATA/states/STATE --output report.json
   ```
2. If the cause was on this side (a disk or database problem now fixed), run
   `tkfs manage revalidate STATE` (or **Revalidate** in the desktop). The
   reply says how many events were re-queued; they are checked again and
   activated if they pass.
3. If they are quarantined again with the same code, the events themselves
   are invalid (events are immutable, so they cannot be repaired): they and
   the events blocked behind them never activate, and the folder stays
   `Blocked` for them while everything else keeps syncing. Find why the
   sending replica produced them (its `worker.log`, the code's meaning in
   [error classes](#error-classes-and-what-to-do)).

**Rejected** events (`N rejected` in a peer row, `· N events rejected` on an
up-to-date folder) were refused before they were staged (for example
`EVENT_ID_REUSED`, `FORGED_EVENT_ORIGIN`, `PRIVATE_BRANCH_REJECTED`). They are
recorded durably, never acknowledged, and `revalidate` does not revive them.

### Initial-sync gate blocked

`Needs attention: initial sync blocked` (`GATE_BLOCKED`): the source is
caught up, but events from it are quarantined, blocked or rejected, so the
received folder does not mount.

- `tkfs manage revalidate STATE` re-checks the quarantined and blocked events
  and reopens the gate; it mounts once they activate.
- If they cannot activate (or events were rejected),
  `tkfs manage mount-now STATE` mounts anyway. Sync continues either way.

### Mount failed

`Mount failed [Retry]` with `MOUNT_PATH_MUST_NOT_EXIST`, `MOUNT_OVERLAPS_*` or
`WINFSP_RUNTIME_UNAVAILABLE`. The folder keeps syncing headless.

1. `MOUNT_PATH_MUST_NOT_EXIST`: something exists at the mount path. A common
   case is an empty directory left behind (for example inside a container
   after a crash). Check that it is empty and not a mount (`mountpoint PATH`
   or `findmnt PATH` on Linux), remove it with a non-recursive `rmdir PATH`,
   then `tkfs manage attach STATE`. A stale mount of this folder's own
   previous worker is cleaned up automatically on Linux; anything else is
   never touched.
2. Wrong path: `manage stop STATE`, `manage configure-mount STATE --mount
   NEW_PATH`, `manage start STATE`.
3. `WINFSP_RUNTIME_UNAVAILABLE`: install WinFsp, then `manage attach STATE`.

Temporary mount errors are retried by themselves (2 s up to 120 s).

### Crash loop

`Crash loop [Retry]` / `failed … CRASH_LOOP`: the worker failed 5 times within
10 minutes and is not restarted until you act. Before that, `restarting in N s
(attempt N)` shows the backoff.

1. Read `last exit` in `manage status` (exit code and fatal error) and the end
   of `DATA/states/<state>/worker.log`. Worker exit codes: 73 store error, 75
   another worker owns the state (`ALREADY_RUNNING`), 78 configuration, 70
   internal error (including an engine that was poisoned, after saving dirty
   files).
2. Fix the cause, then `tkfs manage retry STATE` (desktop: **Retry**).

`WORKER_START_TIMEOUT` means the worker reported no startup progress for 30 s;
its last phase is named.

### Stop blocked

`stop blocked` / `Stopping — files open [Force stop]`: a stop was refused
because files are open in the mount or unsaved data is pending. Close them
(the daemon retries every 2 s), or `tkfs manage force-stop STATE`: unsaved
files go to the folder's pending saves and are committed on its next start.

### Adoption after a daemon crash

Workers are not tied to the daemon's life. If the daemon crashes or is killed,
its workers keep running, mounted and serving files. The next daemon:

1. reads each folder's `worker.json`, checks that the process is alive and is
   the same executable and process instance (pid, start time and path), asks
   it `hello`, and takes it over: `running · adopted`;
2. stops and replaces a worker of a different build (after an upgrade) once it
   has stopped cleanly;
3. marks a worker that is alive but silent for 30 s
   `failed — PREVIOUS_WORKER_UNRESPONSIVE`: `manage force-stop STATE`, then
   `manage start STATE`;
4. asks a worker of the previous release (a `runtime.json` without
   `worker.json`) to stop, and starts the new worker once it has exited
   (`PREVIOUS_WORKER_STOP_TIMEOUT` while it waits).

A worker started by a daemon that has not been contacted for 10 minutes
(`TKFS_UNSUPERVISED_EXIT_S`) stops itself once it is quiet. Where the daemon
is the container's main process, or a systemd unit kills its whole control
group, the workers end with the daemon and are restarted (with store
recovery) instead of adopted; see [Linux](docs/LINUX.md).

### Stalled

`Stalled N min — CODE subject`: the pair is connected and has work, but
nothing progressed for the stall threshold.

1. The culprit names the last error and the object or event. Read the peer
   row's `error` and the folder's errors.
2. `waiting for parents` in the inbox: the events need parents nobody has
   sent: step 5 of the [stuck-replica runbook](#recovering-a-replica-that-was-stuck-on-the-previous-release).
3. Object errors (`CORRUPT_AT_SOURCE`, `NOT_AVAILABLE`): the peer cannot
   serve that file now; another source is used if one has it, otherwise it is
   retried with backoff.
4. Long activation: `activation … starved for …` means a full-replay event
   keeps losing to local writes; it is committed alone after 4 attempts.

### Waiting for the peer's folder

`waiting for the peer's folder`: the other host's copy is stopped, starting,
paused or its worker is not serving. Check that host's `manage status`. Sync
resumes by itself when it serves again.

## Filesystem behaviour

- A mounted folder behaves like a local directory. Content is stored as
  verified, content-addressed objects; history is a causal journal; branches
  and checkpoints are local (`tkfs -C MOUNT branch|checkout|checkpoint …`, see
  the [CLI](docs/CLI.md#filesystem-commands)). Private branches and
  checkpoints never leave the computer; only shared branches sync.
- Concurrent edits become conflicts that are kept and shown (`journal … N
  unresolved conflicts`); nothing is merged automatically. Resolve them in the
  desktop (Conflicts) or with `tkfs resolve` / `tkfs recover`.
- Metadata and content travel separately: a file can be listed, renamed or
  moved before its bytes arrive. Opening a file whose bytes have not arrived
  moves it to the front of the download queue and waits for it for up to 15
  seconds; then the read fails with `CONTENT_UNAVAILABLE` and the download
  continues. A partial download is never shown as a truncated file.
- Closing the desktop leaves the daemon and the mounts running.
  `manage shutdown` stops folders cooperatively and keeps which folders should
  run, so the next start brings them back.
- Not implemented: automatic text merges, symlinks, hard links, native ACL
  replication, garbage collection of old content. Files up to 16 GiB.

## Building

```sh
cargo build --release --locked
cargo test --locked
```

Windows needs the WinFsp SDK and runtime (the desktop is built with Slint).
Linux needs the OpenSSL development libraries and `pkg-config`, and FUSE 3 for
mounts ([Linux](docs/LINUX.md)). `scripts/build.py` builds release binaries
for Windows and Linux (Linux through WSL when run on Windows);
`scripts/package_desktop.py --exe target/release/tkfs.exe` assembles a portable
desktop package. Qualification is described in
[validation](docs/VALIDATION.md).

Licensed under GPL-3.0-or-later; see `LICENSE-GPL-3.0.txt`,
`LICENSE-SLINT-ROYALTY-FREE-2.0.md` and `THIRD-PARTY-NOTICES.md`.
