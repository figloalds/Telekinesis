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

Mounts are not part of this release of the orchestrator: every folder runs
headless. `create` and `receive` accept `--mount PATH`, and the path is
stored with the folder, but nothing mounts it yet; `attach`, `detach`,
`mount-now`, `-C DIRECTORY` and branch mounts arrive with the mounts plan.

## Global options

They are accepted before or after the command.

| Option | Meaning |
|---|---|
| `-f, --defaults-file CONFIG` | The orchestrator configuration ([CONFIGURATION](CONFIGURATION.md)); default `orchestrator.toml` in the current directory. `orchestrator` creates it when missing. Every `manage` command finds the running orchestrator through the `data_directory` of this file. |
| `--state DIRECTORY` | A folder's state directory (`DATA/<folder id>`, shown by `manage list`): the folder commands, `daemon` and `init --state` use it. |
| `--runtime STATE/runtime.json` | The same, given as the `runtime.json` of a running worker: its directory is the state. |
| `--request-id UUID`, `--generation NUMBER` | Accepted with folder mutations (`resolve`, `checkpoint`, `branch`, `checkout`, `mkdir`, `rename`, `delete`, `import`, `gc`) and passed to the worker in the request. The worker does not act on them yet, so they currently change nothing. |

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
  `DESKTOP_REQUIRES_WINDOWS`; `tkfs` with no command runs it too). It is not
  yet a client of the orchestrator: it keeps its own configuration, which it
  rewrites in its own format, so give it a file of its own (`-f`), never the
  `orchestrator.toml` of an orchestrator.

```sh
tkfs gui
tkfs -f desktop.toml gui
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
tkfs manage configure-mount STATE --mount /srv/tkfs/Projects
tkfs manage configure-mount STATE        # no --mount: clear the stored mount
tkfs manage start STATE
tkfs manage stop STATE
tkfs manage stop STATE --no-wait
tkfs manage force-stop STATE
tkfs manage retry STATE
```

- `create LABEL` makes a new folder (a new repository, or `--repo ID` for an
  existing repository id), creates its state directory under `DATA/` and
  starts its worker. The label must be unique. Folders are headless: with
  `--mount PATH` (absolute) the path is only stored for the mounts plan.
  Beyond `maximum_running` folders with a worker it fails with
  `WORKER_LIMIT`. `tkfs create` is the same command. Creating a folder of a
  repository this orchestrator already holds fails with
  `REPO_ALREADY_PRESENT`.
- `configure-mount` sets or, without `--mount`, clears the stored mount of a
  **stopped** folder (`FOLDER_RUNNING` otherwise).
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
  when that is empty, from `listen`; with neither it fails with
  `ADVERTISE_REQUIRED`. Pair on one side only: the dialing host drives the
  exchange, which carries events both ways.
- `peers` prints the paired peers: endpoint, installation, direction, lanes
  and their state, inbound sessions and `clock` (the measured offset and
  round trip to that peer, `null` until the first measurement). It never
  shows a code or a credential.
- `remote-list ENDPOINT` lists the folders a paired host publishes (`repo`
  and `label`).
- `receive ENDPOINT REPO LABEL` creates a local folder of that repository
  (it must be one the host publishes: `REPO_NOT_PUBLISHED` otherwise) and
  starts syncing it; headless like `create`.
- `unpair INSTALLATION` (the id `peers` shows) asks the other host to forget
  this one (best effort), then forgets the peer and closes its connections.
  If the other host also dials this one, that pairing is its own: unpair it
  there.

`ENDPOINT` is `ws://HOST:PORT` or `wss://HOST[:PORT][/PATH]`. `wss://`
verifies the server certificate and host name with the platform trust store;
use the same endpoint string for `pair`, `remote-list` and `receive`. The peer
listener itself is plain WebSocket: see
[remote access](CONFIGURATION.md#remote-access-through-a-tls-proxy).

### Shutdown

```sh
tkfs manage shutdown
```

`shutdown` stops the orchestrator. New requests get `ORCHESTRATOR_STOPPING`;
the peer connections close; every folder is stopped in parallel within a
30 s bound. A worker that has not stopped by about 15 s is force-stopped and
one that is still alive at about 27 s is killed; each folder that needed it
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
  to the orchestrator. `--mount PATH` is accepted for tests and the mounts
  plan; the orchestrator does not pass it.
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
