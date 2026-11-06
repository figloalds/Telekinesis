# Command reference

`tkfs` is one executable:

- the **orchestrator** (`orchestrator`, alias `start`): the daemon that owns
  the catalog of folders, runs one worker per folder, talks to paired peers
  and serves the control endpoint;
- the **workers** it launches (`daemon --state … --supervised`), one per
  folder;
- the **management client** (`manage …`), which talks to the orchestrator;
- the **folder commands** (`status`, `tree`, `mkdir`, …), which talk to one
  folder's worker (or open its store when no worker runs);
- the Windows desktop (`gui`).

Every command line below is checked against the binary by
`tests/docs_cli.rs`: the command must exist, every option it shows must be in
that command's `--help`, and it must give the positional arguments the
command asks for. Every top-level command and every `manage` subcommand must
appear in a block.

```sh
tkfs --help
tkfs manage --help
tkfs manage status --help
```

A folder mounts when it has a mount point: `create`, `receive` and
`configure-mount` take `--mount PATH` (a directory that does not exist yet;
on Windows also a drive letter such as `X:`), and the folder's worker serves
it while it runs. Without one the folder runs headless. `attach`, `detach`,
`mount-now`, `-C DIRECTORY` and branch mounts are not part of this release.

## Global options

They are accepted before or after the command.

| Option | Meaning |
|---|---|
| `-f, --defaults-file CONFIG` | The orchestrator configuration ([CONFIGURATION](CONFIGURATION.md)); default `TKFS_CONFIG` when set, else `orchestrator.toml` next to `tkfs.exe` on Windows and in the current directory elsewhere. `orchestrator` creates it when missing. Every `manage` command finds the running orchestrator through the `data_directory` of this file. |
| `--state DIRECTORY` | A folder's state directory (`DATA/<folder id>`, shown by `manage list`): the folder commands, `daemon` and `init --state` use it. |
| `--runtime STATE/runtime.json` | The same, given as the `runtime.json` of a running worker: its directory is the state. |
| `--request-id UUID`, `--generation NUMBER` | Accepted with folder mutations (`resolve`, `checkpoint`, `branch`, `checkout`, `mkdir`, `rename`, `delete`, `import`, `gc`) and passed to the worker in the request. The worker does not act on them yet, so they currently change nothing. |

Compatibility (Windows): older builds read `orchestrator.toml` from the
current directory. With neither `-f` nor `TKFS_CONFIG`, tkfs now reads the one
next to `tkfs.exe`. When the current directory also holds an
`orchestrator.toml` that is a different file, a command prints
`CONFIG_LOCATION_CHANGED: using …; … is ignored (pass -f to use it)` on stderr
and carries on with the file beside `tkfs.exe`; pass `-f orchestrator.toml` (or
set `TKFS_CONFIG`) to keep using the one in the current directory.

`--json` belongs to `manage` and goes before its subcommand
(`tkfs manage --json status`).

A command that fails prints `CODE: message` on stderr and exits with status
1. The codes, and what to do about them, are listed in the
[README](../README.md#error-classes-and-what-to-do).

## Running the orchestrator

```sh
tkfs init -f orchestrator.toml          # write a new configuration (never overwrites)
tkfs -f orchestrator.toml orchestrator   # run the orchestrator in the foreground
tkfs -f orchestrator.toml start          # the same (alias); this is what containers run
```

- `init -f FILE` writes a format 2 configuration with the defaults of
  [CONFIGURATION](CONFIGURATION.md) and fails with `CONFIG_EXISTS` if the
  file exists. It is optional: `orchestrator` and `start` create a missing
  file with the same defaults, and refuse an invalid one without touching it.
- `orchestrator` runs until `tkfs manage shutdown`, Ctrl-C, closing its
  console, or SIGINT/SIGTERM; every one of them runs the same shutdown
  sequence (see `manage shutdown`). One orchestrator runs per data
  directory: a second one fails with `ALREADY_RUNNING` (exit status 75). It
  writes `DATA/orchestrator.json` (addresses, pid, version, installation) and
  `DATA/orchestrator.token` and removes both when it exits. Its log is
  `DATA/logs/orchestrator.log` (JSON lines, rotated; see
  [logs](CONFIGURATION.md#logs)); its events are also printed on stderr.
- A system clock before 2020-01-01 UTC makes `orchestrator`, `daemon` and
  `manage pair` fail with `CLOCK_INVALID` (exit status 78 for the two
  daemons). Fix the clock (NTP) and start again.
- `gui` is the Windows desktop (elsewhere it fails with
  `DESKTOP_REQUIRES_WINDOWS`; `tkfs` with no command runs it too). It is a
  client of the orchestrator and uses the same `orchestrator.toml` (`-f`, or
  the usual resolution). It starts the orchestrator when none answers and
  attaches to the running one otherwise; closing the window leaves the
  orchestrator and its folders running. When the orchestrator stops, the
  window says so and offers to start it again; it never restarts it by
  itself.

```sh
tkfs gui
```

## Managing folders: `tkfs manage …`

Every `manage` command talks to the running orchestrator over its loopback
control endpoint, presenting its token (`DATA/orchestrator.token`): run them
as the user the orchestrator runs as. A token that cannot be read or does not
match is refused (`ACCESS_DENIED`). With no orchestrator running they fail
with `ORCHESTRATOR_NOT_RUNNING` (or `WS_CONNECT_FAILED` when a crashed
orchestrator left its `orchestrator.json`). A command that changes something
is never sent twice: if the connection is lost after sending, it fails with
`CONNECTION_LOST`; check `manage status` to see whether it happened. Read
only commands retry once.

**Folder arguments** (`STATE` below) accept the full folder id, a unique
prefix of it of at least 4 characters (the `[id]` that `manage status`
shows) or a unique label (`AMBIGUOUS_FOLDER` when more than one folder
matches, `FOLDER_NOT_FOUND` when none does).

### Status

```sh
tkfs manage status
tkfs manage --json status
tkfs manage watch
tkfs manage --json watch
tkfs manage list
tkfs manage inspect STATE
```

- `status` prints the orchestrator line (pid, version, control and peer
  listen addresses; `STOPPING` during shutdown), one line per folder
  (`[id] label  phase  mount|headless  conflicts N`, then `paused` and the
  folder's open error when there is one) and the peers. A sample, from a real
  run (the path and ids are shortened):

```text
orchestrator pid 29636  version 0.1.0  control 127.0.0.1:62548  listen 127.0.0.1:43180
[0f0dc9c0] Projects  Running  headless  conflicts 0
[67382aba] Archive  Failed  headless  conflicts 0  CRASH_LOOP: 5 worker failures within 600 s; last: …
peers: 1
  wss://files.example.com  83f84da9-57d0-46e6-8946-f2fd089f9767
```

- The phase is `Starting`, `Running`, `Stopping`, `Stopped`, `Backoff`
  (restarting after a failure, with a delay that grows to 5 minutes) or
  `Failed` (crash-loop breaker: 5 failures in 10 minutes; `manage retry`).
- `--json status` prints the snapshot as the orchestrator sends it: the
  `daemon` record, every folder with its record, phase, heartbeat, lanes,
  conflicts, error and per-peer sync progress, and the peers.
- `watch` prints `status` every second until Ctrl-C (with `--json`, one
  snapshot per refresh).
- `list` prints `[id] label  state-directory` for each folder in the catalog
  (`--json`: their records). `inspect` prints one folder as JSON: its record,
  phase, heartbeat, lanes and sync progress, and, while its worker runs, the
  worker's own status.
- The sync section of a folder (`sent`, `received`, `last_exchange_ms`,
  `error` per peer) and the `clock` of each entry of `manage peers` (offset
  and round trip to that peer) are in the JSON.

### Folders: create, start, stop

```sh
tkfs manage create Projects
tkfs manage create Archive --headless
tkfs manage create Projects --repo REPO_ID
tkfs manage create Projects --mount /srv/tkfs/Projects
tkfs create Projects                     # the same as manage create
tkfs manage configure-mount STATE --mount X:   # stopped folders only
tkfs manage configure-mount STATE        # no --mount: run headless
tkfs manage start STATE
tkfs manage stop STATE
tkfs manage stop STATE --no-wait
tkfs manage force-stop STATE
tkfs manage retry STATE
```

- `create LABEL` makes a new folder (a new repository, or `--repo ID` for an
  existing repository id), creates its state directory under `DATA/` and
  starts its worker. The label must be unique. With `--mount PATH` the
  worker mounts the folder there (its parent directory is created; the
  mount point itself must not exist: `MOUNT_POINT_EXISTS`, nor be another
  folder's: `MOUNT_IN_USE`; nothing is created then).
  If the folder's store cannot be created it fails with `FOLDER_INIT_FAILED`
  and nothing of it is kept, so the same label and repository can be tried
  again.
  Beyond `maximum_running` folders with a worker it fails with
  `WORKER_LIMIT`. `tkfs create` is the same command. Creating a folder of a
  repository this orchestrator already holds fails with
  `REPO_ALREADY_PRESENT`.
- `configure-mount` sets (`--mount PATH`) or clears (no `--mount`) the
  mount of a **stopped** folder (`FOLDER_RUNNING` otherwise), checked as for
  `create`. The next start mounts there.
- `start` sets the folder to run (persisted) and starts its worker;
  `WORKER_LIMIT` if too many run.
- `stop` records that the folder should stay stopped, then asks the worker
  to stop and waits up to about 13 s. If the worker has files open it
  refuses (`BUSY_VIEW`) and keeps running. `--no-wait` returns at once and
  the orchestrator stops it in the background.
- `force-stop` stops the worker even with files open (it saves what is
  unsaved first), waits up to about 15 s and kills the worker if it still
  hangs.
- `retry` clears the crash-loop breaker of a `Failed` or backing-off folder
  and starts it again at once.

### Sync

```sh
tkfs manage pause STATE
tkfs manage resume STATE
```

`pause` stops syncing the folder with every peer, in both directions; the
folder keeps running and local commands keep working. `resume` restarts it.
Both are persisted.

### Pairing and remote folders

```sh
tkfs manage reset-pairing-code
tkfs manage pair ws://files.example.com:43180
printf '%s\n' "$PAIRING_CODE" | tkfs manage pair wss://files.example.com --connections 6
tkfs manage pair ws://files.example.com:43180 --code PAIRING_CODE
tkfs manage peers
tkfs manage remote-list wss://files.example.com
tkfs manage receive wss://files.example.com REPO_ID Projects
tkfs manage receive wss://files.example.com REPO_ID Archive --headless
tkfs manage unpair INSTALLATION
tkfs manage set-endpoint INSTALLATION ws://newhost:43180
```

- `reset-pairing-code` creates a new pairing code for this orchestrator and
  prints it once (`{"code": …}`); only a salted verifier is stored. It
  revokes every pairing made with the old code: sessions that used it are
  closed within a second and the hosts that dial this one must pair again
  with the new code.
- `pair ENDPOINT` pairs with the orchestrator at ENDPOINT. The other host's
  pairing code is read from the first line of stdin unless `--code` is given
  (stdin keeps it out of the shell history and the process list). The code
  itself never crosses the network: the hosts authenticate with an HMAC
  challenge and, once, enroll a per-peer credential that later sessions use.
  The orchestrator then keeps a pool of connections to the peer:
  `--connections N` (1-16, default `[network] connections`) sets it, one
  control lane plus at least one bulk lane (`1` is raised to 2). `pair`
  tells the peer this host's address, taken from `[network] advertise` or,
  when that is empty, from `listen`. A dial-only host (`listen = ""` and no
  `advertise`) tells it no address: the peer keeps the pairing without an
  endpoint (`peers` shows it inbound, `endpoint` null) and never dials back.
  Pair on one side only: the dialing host drives the exchange, which carries
  events both ways.
- `peers` prints the paired peers: endpoint, installation, direction, lanes
  and their state, inbound sessions, `clock` (the measured offset and
  round trip to that peer, `null` until the first measurement), and
  `requests_served` and `notifies_sent` (what this host answered for that peer
  and hinted to it since this orchestrator started; heartbeats count for
  neither), `features` (what the peer advertised, so `[]` for an older one),
  `profile` (`"active"` or `"passive"`, as that peer declared when it dialed
  this host; `null` with no inbound session, or for an older one that declared nothing) and `two_way_content` (whether
  content moves both ways with that peer: `false` for an older one, which is
  only pulled from). It never shows a code or a credential. Per-folder
  `inspect` gains `pushed` and `last_push_error` for each peer.
- `remote-list ENDPOINT` lists the folders a paired host publishes (`repo`
  and `label`).
- `receive ENDPOINT REPO LABEL` creates a local folder of that repository
  (it must be one the host publishes: `REPO_NOT_PUBLISHED` otherwise) and
  starts syncing it; `--mount` as for `create`. Pair then receive is two-way:
  local changes are noticed within about a second and reach the host as well.
  On a passive host (`network.sync_profile = "passive"`) nothing arrives until
  `sync-now`.
- `unpair INSTALLATION` (the id `peers` shows) asks the other host to forget
  this one (best effort), then forgets the peer and closes its connections.
  If the other host also dials this one, that pairing is its own: unpair it
  there.
- `set-endpoint INSTALLATION ENDPOINT` moves a paired peer to a new address
  (for example when its PC changed address) with the credential it already
  holds: no pairing code, so no other host that dials the peer is revoked. It
  first checks that ENDPOINT answers as that installation and accepts the
  credential, and changes nothing otherwise (`PEER_NOT_AT_ENDPOINT`;
  `PEER_NOT_FOUND` for an unknown installation). The peer's connections are
  reopened at the new address; its sync progress is kept.

`ENDPOINT` is `ws://HOST:PORT` or `wss://HOST[:PORT][/PATH]`. `wss://`
verifies the server certificate and host name with the platform trust store;
use the same endpoint string for `pair`, `remote-list` and `receive`. The peer
listener itself is plain WebSocket: see
[remote access](CONFIGURATION.md#remote-access-through-a-tls-proxy).

### Syncing now

```sh
tkfs manage sync-now
tkfs manage sync-now STATE
tkfs manage sync-now --metadata-only [STATE]
```

`sync-now` syncs one folder, or every folder when none is named, with every
peer this host dials, and waits for that to finish. It works in either profile,
and it is the only way a host with `network.sync_profile = "passive"` asks its
peers for anything: a passive host keeps its connections open and serves its
own readers, but exchanges metadata and pushes only when told to. It:

- makes connections waiting in a backoff redial at once (not those refused for
  a credential or a version);
- exchanges metadata until neither side has more;
- pushes what each peer wants, what its readers wait for first, until a whole
  pass over a peer's listing stores nothing. It does not download: a passive host
  fetches a file when something reads it.

With `--metadata-only` it does the first two steps and not the third: no object
is asked for or pushed, so a file list refreshes on a metered link and no file
moves. `pushed` is `0`, `remaining` is `null` (no listing is read), and it
finishes when the metadata exchange does. A plain `sync-now` later moves the
bytes. While a metadata-only `sync-now` is open for a folder, no push step runs
for it unless another, full `sync-now` is open as well.

It prints `{"folders": [{"id", "exchanged", "pushed", "remaining", "error"}]}`:

- `exchanged`: metadata exchanges that finished;
- `pushed`: objects a peer confirmed are durable;
- `remaining`: once a whole pass over a listing stored nothing, the objects this
  host holds and knows did not get across. What this host does not hold is not
  counted: it is not this host's to send. Short of that it is an estimate, what
  the last page of a listing named and this host did not store. `0` with no error
  means a peer's listing, read from its start, named nothing this host holds and
  has not sent. It is `null` when no listing came back;
- `error`: why the folder did not finish, or `null`. See below.

An error here is reported, not fatal: the command exits 0 and what was done stays
done. The whole command takes at most 175 seconds, and it stops early if the
orchestrator shuts down. A failure of a request starts with the operation that
failed and then its code, as in `objects.store: FATAL_STORE_ERROR: ...`, and an
error of one peer ends with `(peer ENDPOINT)`. The `error` of a folder is:

- the failure of a peer's connection, when its connections stay down for 10
  seconds;
- `REPO_NOT_PUBLISHED`, when no peer took the folder. A peer without the folder
  is no error if another peer took it. A peer that has answered for the folder
  since this orchestrator started gets 10 seconds to publish it again, since its
  folder may only be restarting; any other is told apart at once;
- `PEER_PUSH_UNAVAILABLE`, when a peer is too old to take pushes. Its metadata
  still moved, and `remaining` is `null` if no other peer answered;
- `WORKER_NOT_RUNNING`, when this folder is stopped or paused;
- the code of what a peer refused (`OBJECT_NOT_WANTED`, `INVALID_PEER_OBJECT`), or
  `SYNC_OBJECT_TOO_LARGE` for what no reply can carry, with how many objects, at
  the end of a pass that stored nothing else. They count in `remaining`;
- the last failure, or `SYNC_NOW_TIMEOUT` if nothing was failing, when time ran
  out. A timeout or a lost connection does not end the command: a slow link gets
  asked again, in smaller batches and less and less often (a rest from 1 up to 60
  seconds), until the 175 seconds are up;
- any other failure, at once, since asking again would repeat it.

With no peer to dial there is nothing to do, and it is `null`.

A passive host remembers how far each folder got with each peer, across
reconnects and restarts, so a `sync-now` of an unchanged folder is one small
request however long its history is. It remembers only what is safely on disk on
both sides. It starts over by itself when either side's journal was replaced or
ends before where it got to, and when a peer is too old to say. An active host
keeps none of this and starts over on every connection.

```sh
tkfs manage reset-cursors
```

`reset-cursors` forgets all of that, so the next sync of every folder with every
peer starts over and sends and receives its whole history again. It is for what a
passive host cannot detect, such as a peer restored from an old backup that has
since grown past where it got to. It prints `{}`.

### Shutdown

```sh
tkfs manage shutdown
```

`shutdown` stops the orchestrator. New requests get `ORCHESTRATOR_STOPPING`;
the peer connections close; every folder is stopped in parallel within a
30 s bound. A worker that has not stopped by about 15 s is force-stopped and
one that is still alive at about 25 s is killed (the sequence ends by 30 s); each folder that needed it
is logged as `FORCED_STOP`. The reply lists each folder's outcome:
`stopped`, `not_running`, `killed` or `stranded` (it could not be shown gone
or killed; the reason is in `error`). It never changes whether a folder
should run, so the next start brings the same folders back. SIGINT and
SIGTERM run this same sequence, and the orchestrator exits 0.

## A folder's worker

The orchestrator launches one worker per folder (`daemon --state …
--supervised`). You can also run one yourself on a folder state, without an
orchestrator:

```sh
tkfs init --state /srv/tkfs/standalone
tkfs init --state /srv/tkfs/standalone --repo REPO_ID
tkfs daemon --state /srv/tkfs/standalone
tkfs daemon --state /srv/tkfs/standalone --supervised
```

- `init --state DIR` creates (or opens) a folder store and prints its device,
  repository and state; `--repo` gives it an existing repository id.
- `daemon --state DIR` runs the worker: it serves the folder on a loopback
  session (`runtime.json`, `runtime.token`, `worker.json` and `worker.log` in
  the state directory) for the folder commands below and for an
  orchestrator. A worker never listens for peers and never dials: peers talk
  to the orchestrator. `--mount PATH` mounts the folder there; the
  orchestrator passes it for a folder that has a mount point.
- `--supervised` is what the orchestrator uses: the worker exits quietly
  after a period without contact (`TKFS_UNSUPERVISED_EXIT_S`, default
  600 s). A worker started by hand does not.
- Do not run a worker on a state an orchestrator manages: one worker per
  state (`owner.lock`).

## Folder commands

These act on one folder, named with `--state DIR` or `--runtime
STATE/runtime.json`; without either they fail with `FOLDER_REQUIRED`. While
the folder's worker runs they talk to it (presenting `runtime.token`: run
them as the user the worker runs as); when no worker runs they open the
store directly. They print JSON.

```sh
tkfs --state D:\tkfs\data\STATE status
tkfs --runtime D:\tkfs\data\STATE\runtime.json status
tkfs --state D:\tkfs\data\STATE tree
tkfs --state D:\tkfs\data\STATE events
tkfs --state D:\tkfs\data\STATE conflicts
tkfs --state D:\tkfs\data\STATE conflicts --history
tkfs --state D:\tkfs\data\STATE resolve ENTITY_ID VERSION_ID
tkfs --state D:\tkfs\data\STATE checkpoint
tkfs --state D:\tkfs\data\STATE branch experiment
tkfs --state D:\tkfs\data\STATE branch experiment --snapshot SNAPSHOT_ID
tkfs --state D:\tkfs\data\STATE checkout experiment
tkfs --state D:\tkfs\data\STATE mkdir docs
tkfs --state D:\tkfs\data\STATE import docs/report.pdf C:\Users\me\report.pdf
tkfs --state D:\tkfs\data\STATE cat docs/report.pdf
tkfs --state D:\tkfs\data\STATE rename docs/report.pdf docs/final.pdf --replace
tkfs --state D:\tkfs\data\STATE delete docs/final.pdf
tkfs --state D:\tkfs\data\STATE flush
tkfs --state D:\tkfs\data\STATE gc
tkfs --state D:\tkfs\data\STATE stop
tkfs --state D:\tkfs\data\STATE rpc status
tkfs --state D:\tkfs\data\STATE rpc rename '{"path":"a","target":"b","replace":false}'
```

| Command | Effect |
|---|---|
| `status` | The folder's branch, repository, device, entry and conflict counts and last error. |
| `tree` | The folder's current tree (JSON). |
| `events` | The folder's event journal. |
| `conflicts [--history]` | Unresolved conflicts: entities with competing heads. `--history` includes the retained alternatives. Conflicts never create files in the folder. |
| `resolve ENTITY VERSION` | Resolve a conflict by choosing one of the entity's competing versions (an event with both heads as parents). |
| `checkpoint` | Record a snapshot of the current state; prints its id. |
| `branch NAME [--snapshot ID]`, `checkout BRANCH` | Create a branch (from the current state, or from a snapshot) and select a branch. |
| `mkdir PATH`, `import PATH SOURCE`, `cat PATH`, `rename PATH TARGET [--replace]`, `delete PATH` | Change and read files inside the folder without a mount. `import` copies the local file SOURCE to PATH (its parent folder must exist); `cat` writes the bytes to stdout. |
| `flush`, `gc` | Seal pending writes now; collect unreferenced objects. |
| `stop` | Ask the worker to shut down (refused with `BUSY_VIEW` while files are open). For a folder an orchestrator manages use `tkfs manage stop`, or the orchestrator restarts the worker. |
| `rpc OPERATION [PAYLOAD]` | Call any worker control operation with a JSON payload (default `{}`) and print the JSON reply. |

A `runtime.json`
left behind by a crashed worker makes the commands above fail to connect until a
worker starts on the folder again (it replaces the file). `rpc` never opens
the store itself: without a running worker it fails with
`WORKER_NOT_RUNNING`.

Resolving a conflict on a remote peer and mounting branches arrive with
later plans.
