# Command reference

`tkfs` is one executable: the daemon (`orchestrator`), the desktop (`gui`),
the folder workers the daemon launches (`daemon`), the management client
(`manage …`) and the filesystem commands of one folder (`status`, `branch`,
…). Every command line below is checked against the binary by
`tests/docs_cli.rs`: the command must exist and every option it shows must be
in that command's `--help`.

```sh
tkfs --help
tkfs manage --help
tkfs manage status --help
```

## Global options

They are accepted before or after the command (`--json` after `manage`).

| Option | Meaning |
|---|---|
| `-f, --defaults-file CONFIG` | The daemon configuration (default `orchestrator.toml` in the current directory). Every `manage` command finds the running daemon through the `data_directory` of this file. |
| `-C DIRECTORY` | A folder's mount (or a directory inside it): filesystem commands find the folder's worker through the mount's `.tkfs-runtime.json`. |
| `--runtime STATE/runtime.json` | The same for a folder without a mount: its state directory's `runtime.json`. |
| `--request-id UUID` | Filesystem mutations only: a stable request id, so retrying after an uncertain reply cannot apply the change twice. |
| `--generation NUMBER` | Filesystem mutations only: the store generation the change was prepared against (shown by `status`); a different current generation refuses it with `STALE_VIEW`. |
| `--json` | `manage` commands only: print the daemon's JSON reply instead of text. |

A command that fails prints the error (`CODE: message …`) on stderr and exits
with status 1. The codes, their classes and what to do about them are listed
in the [README](../README.md#error-classes-and-what-to-do).

## Running the daemon and the desktop

```sh
tkfs init -f orchestrator.toml          # write a new configuration (never overwrites)
tkfs -f orchestrator.toml orchestrator   # run the daemon in the foreground
tkfs -f orchestrator.toml start          # the same (alias)
tkfs -f orchestrator.toml gui            # the Windows desktop
```

- `init` writes a format 2 configuration with the defaults of
  [CONFIGURATION](CONFIGURATION.md) and fails if the file exists.
- `orchestrator` runs until `tkfs manage shutdown`, Ctrl-C, closing its
  console, or SIGINT/SIGTERM; every one of them runs the same shutdown
  sequence (see `manage shutdown`). On start it prints its discovery record
  (`{control, network, installation, pid, version, …}`) on stdout and writes
  the same to `DATA/orchestrator.json`, and its control token to
  `DATA/orchestrator.token`. Its log is `DATA/logs/orchestrator.log` (JSON
  lines, rotated; see [logs](CONFIGURATION.md#logs)); its events are also
  printed on stderr.
- `gui` (Windows only; elsewhere it fails with `DESKTOP_WINDOWS_ONLY`) creates
  the configuration if it is missing, starts a daemon if none answers (it
  logs to `DATA/logs/orchestrator.log`) and shows the folders,
  peers and activity. `tkfs` with no arguments opens the desktop with
  `orchestrator.toml` on Windows. Closing the window leaves the daemon and its
  mounts running.

## Managing folders: `tkfs manage …`

Every `manage` command talks to the running daemon over its loopback control
endpoint, presenting the daemon's token (`DATA/orchestrator.token`): run them
as the user the daemon runs as. A token that cannot be read or does not match
is refused at the connection (`ACCESS_DENIED`). With no daemon running they
fail with `ORCHESTRATOR_NOT_RUNNING` (or `WS_CONNECT_FAILED` when a crashed
daemon left its `orchestrator.json`).
A command that changes something is never sent twice: if the connection is
lost after sending, it fails with `CONNECTION_LOST` and says so; check
`manage status` to see whether it happened.

**Folder arguments** (`STATE` below) accept the full state id, a unique
prefix of it of at least 4 characters (the `[id]` that `manage status`
shows), or a unique folder label.

### Status

```sh
tkfs manage status
tkfs manage status --json
tkfs manage watch
tkfs manage watch --json
```

- `status` prints the daemon (version, uptime, installation, control and
  network addresses, its `log` file, its own errors), a one-line overview,
  one block per folder and the peers, then the recent activity. `--json`
  prints the status snapshot (schema 2) as the daemon sends it.
- `watch` follows the same view live until Ctrl-C: on a terminal it redraws
  in place on every change and every 2 s (ages and countdowns); otherwise it
  prints each changed snapshot. With `--json` it prints one snapshot per line.
  When the daemon goes away it says so and reconnects every 2 s, following a
  restarted daemon to its new port.

A folder block, as `manage status` prints it (from a real run; the path and
the peer's address are changed):

```text
Projects [ac526511]
  sync       Up to date · 1 of 1 peer
  lifecycle  running for 4 s · pid 38172
  mount      attached at D:\tkfs\mounts\Projects for 4 s
  store      healthy
  inbox      nothing to activate
  activation running · 15 activated (15 fast, 0 slow) · last 4 s ago
  content    all 7 files available
  journal    15 events · all acknowledged by peers
  peer       wss://files.example.com · outbound · up to date
               metadata caught up · last exchange just now
               download 465.2 KiB/s
               progress last 4 s ago
```

| Row | What it says |
|---|---|
| `sync` | The folder's sync state and summary ([meanings](../README.md#folder-sync-states)). A `!` marks states that need a look (needs attention, stalled, blocked). |
| `lifecycle` | The worker process: `starting for … · <phase> (step N)`, `running for …`, `stop blocked`, `stopping`, `restarting in …`, `failed`; `adopted` when this daemon took over a running worker; `degraded: the engine lock is stalled`; the restart count. |
| `last exit` | Shown while the worker is not running: its exit code and fatal error (or the last line of `worker.log`). |
| `mount` | `attached at …`, `attaching`, `attach failed (attempt N): [class] CODE … · retry in …` or `· waiting for you to fix it`, `lost`, `headless (no mount path)`, `detached on request`, `waiting for the initial sync before mounting at …`. |
| `gate` | Received folders only: the initial-sync gate ([meanings](../README.md#initial-sync-gate)). |
| `intents` | `sync paused` while sync is paused. |
| `store` | Store health, open files, pending saves and unflushed files. |
| `inbox` | Received events not yet activated: `N to activate (ready, waiting for parents, retrying, unclassified)`, plus `quarantined` and `blocked` counts. |
| `activation` | The background activator: running, how many events it committed (fast path or full replay), when last, and `starved for …` if a slow event keeps losing its commit. |
| `content` | Current files available or missing (and their size), downloads in progress, requested files, and how much of the shared history's content is here. |
| `journal` | Committed events, how many are not yet acknowledged by any peer, unresolved conflicts. |
| `peer` | One row per peer that shares the folder: name, direction, state ([meanings](../README.md#per-peer-states)), then its metadata (`to receive`, `to send`, `received, not yet activated`, `pending at the peer`, `blocked`, `quarantined`, `rejected`, `caught up`), download and upload (files, bytes left, rate, ETA, in flight), last progress and its open error. |
| `errors` | Open errors (`! [class] CODE: message · scope · Nx since … · retry in …`), at most 5 (`--json` lists all); when none is open, the last resolved one. |
| `hint` | The command that would help: `retry`, `force-stop`, `resume`, `mount-now`, `revalidate`, `attach`. |

The peers section lists each peer's connection (`online for …`, `backing off
(retry in …)`, `needs upgrade (incompatible protocol)`, `unauthorized: pair
again with the current code`, …), direction, protocol (`tkfs-peer/3`), RTT,
rates, lanes and shared folders.

### Folders: list, inspect, create, start, stop

```sh
tkfs manage list
tkfs manage inspect STATE
tkfs manage create Projects
tkfs manage create Projects --mount D:\Work\Projects
tkfs manage create Archive --headless
tkfs create Projects --mount D:\Work\Projects   # the same as manage create
tkfs manage start STATE
tkfs manage stop STATE
tkfs manage stop STATE --no-wait
tkfs manage force-stop STATE
tkfs manage retry STATE
```

- `list` and `inspect` print JSON in the shape of the previous release
  (`state, label, repo, device, directory, mount, desired_running, running,
  starting, mounted, initial_sync_pending, startup_phase, error,
  publication_error, sync`) plus a typed `status` member: the folder's block
  of the status snapshot, with its open errors.
- `create LABEL` makes a new repository and starts its worker. Without
  `--mount` it mounts at `default_mount_directory/LABEL`; `--headless` makes
  a folder without a mount. The mount path must not exist and must not
  overlap the data directory or another folder. Beyond `maximum_running`
  folders with a worker it fails with `WORKER_LIMIT`. It returns at once
  with `starting: true`.
- `start` sets the folder to run (persisted) and returns at once with
  `starting: true`; use `manage status` or `watch` to see it come up.
- `stop` first records that the folder should stay stopped, then asks the
  worker to stop and waits up to about 13 s. If files are open in the mount
  or unsaved data is pending, the worker refuses (`BUSY_VIEW`): the folder
  shows `stop blocked`, the daemon retries every 2 s, and the reply says
  `stopping: true` with what it waits for. `--no-wait` returns at once.
- `force-stop` stops even with files open: the worker saves every unsaved
  file to its pending saves (they are kept and committed on the next start),
  detaches the mount and exits. It also ends a stop that is blocked and a
  folder stuck behind an unresponsive older worker.
- `retry` restarts a folder that is `failed` (crash loop, start failure) or
  waiting to restart, at once, and retries a mount that waits for you.

### Mounts and the initial sync

```sh
tkfs manage attach STATE
tkfs manage detach STATE
tkfs manage detach STATE --force
tkfs manage mount-now STATE
tkfs manage configure-mount STATE --mount D:\Work\Projects
tkfs manage configure-mount STATE              # no --mount: make it headless
```

- `attach` mounts now: after you fixed a mount error that needs you (an
  existing folder at the mount path, WinFsp missing), after a `detach`, or
  after the mount was lost.
- `detach` unmounts; the folder keeps syncing. It is refused while files are
  open (`BUSY_VIEW`); `--force` saves unsaved files first, then unmounts. A
  detach lasts until `attach` or until the worker starts again.
- `mount-now` overrides the initial-sync gate of a received folder: it mounts
  before the initial sync from its source has finished. The override is
  persisted.
- `configure-mount` changes or clears (headless) the mount path of a
  **stopped** folder (`STOP_FOLDER_BEFORE_CONFIGURING` otherwise).

### Sync and received events

```sh
tkfs manage pause STATE
tkfs manage resume STATE
tkfs manage revalidate STATE
tkfs manage revalidate STATE --id EVENT_ID --id OTHER_EVENT_ID
```

- `pause` stops syncing the folder with every peer, in both directions (a
  peer that asks gets `FOLDER_SYNC_PAUSED` and shows the folder as waiting);
  the mount and local changes keep working. `resume` restarts it. Both are
  persisted.
- `revalidate` puts quarantined and blocked received events (and failed ones
  waiting to retry) back in the queue, so they are checked and activated
  again; `--id` limits it to those events and the ones blocked on them. It
  prints how many were re-queued. It needs a running worker. While the
  initial-sync gate is blocked it also reopens the gate. Rejected events
  (refused before staging) are not re-queued: see the
  [runbook](../README.md#quarantined-or-blocked-events).

### Daemon

```sh
tkfs manage diagnostics
tkfs manage shutdown
```

- `diagnostics` writes a diagnostics bundle and prints the daemon's JSON
  reply, whose `path` is the bundle's directory, `DATA/diagnostics/<time>/`.
  It holds `status.json` (the status snapshot), `activity.json` (the recent
  activity), `folders/<state>.heartbeat.json` (each folder's lifecycle,
  gate, mount and last heartbeat), `catalog.redacted.json` (folders, peers,
  gates; no codes, verifier or credentials), `logs/*.tail` (the end of the
  daemon's log and of each folder's `worker.log`) and `versions.json` (the
  daemon's and workers' versions, protocol and format versions, the
  platform). Pairing codes, per-peer secrets and tokens are removed from
  every file. It reads the daemon's snapshots and files only, so it answers
  while folders are busy or stuck; older bundles are deleted, keeping the
  newest 10.
- `shutdown` stops the daemon: new requests get `ORCHESTRATOR_STOPPING`, the
  peer connections close, every folder is stopped in parallel (at most 30 s),
  and the reply lists each folder's outcome: `stopped`, `left_running` (the
  worker had open files or did not answer: it keeps running, mounted, and the
  next daemon adopts it), `killed` (it was still starting) or `not_running`.
  It never changes whether a folder should run, so the next start brings the
  same folders back.

### Pairing and remote folders

```sh
tkfs manage reset-pairing-code
tkfs manage pair wss://files.example.com
echo PAIRING_CODE | tkfs manage pair wss://files.example.com --connections 6
tkfs manage peers
tkfs manage remote-list wss://files.example.com
tkfs manage receive wss://files.example.com REPO_ID Projects
tkfs manage receive wss://files.example.com REPO_ID Projects --mount D:\Work\Projects
tkfs manage receive wss://files.example.com REPO_ID Archive --headless
tkfs manage unpair wss://files.example.com
```

- `reset-pairing-code` creates a new pairing code for this daemon and prints
  it once (`{"code": …}`); only a salted verifier of it is stored. It also
  revokes the per-peer credential of every host that dials this one.
  Sessions that authenticated with the previous code or one of those
  credentials are closed within a second (`PAIRING_CODE_CHANGED`), and
  their hosts are refused from then on (`PEER_REVOKED`,
  `INVALID_PAIRING_CODE`): every host that dials this one must pair again
  with the new code.
- `pair ENDPOINT` reads the other host's pairing code from stdin (it prompts
  `Pairing code:` on stderr), authenticates once (the code itself never
  crosses the network), enrolls a per-peer credential with the other host
  (later sessions authenticate with it), stores the endpoint, the code and
  the peer's installation, and starts the connection pool.
  `--connections N` (1–16, default `network.connections`) sets the pool: one
  control lane plus at least one bulk lane (`1` is raised to 2). Pairing the same endpoint again
  updates the code and the pool and reconnects at once; pairing another
  endpoint of the same installation replaces the old one. A host running the
  previous release is refused with `PEER_NEEDS_UPGRADE` before the code is
  sent.
- `peers` prints the outbound peers and their lanes in the JSON shape of the
  previous release; it never shows a pairing code.
- `remote-list ENDPOINT` lists the running folders a paired host publishes
  (`repo`, `replica`, `branches`, `label`). The `label` is the folder's name
  on that host (it follows a rename there); the `repo` names the repository.
- `receive ENDPOINT REPO LABEL` creates a local replica of that repository
  (a new device of it) and starts its worker. The folder syncs at once; its
  mount (at `--mount`, or `default_mount_directory/LABEL`) waits for the
  initial sync from that host (the gate), so files are not shown half
  synchronized. `--headless` receives without a mount.
- `unpair ENDPOINT` first asks the other host to revoke this host's
  per-peer credential (`peer.revoke`; best effort, so an unreachable host
  does not block it), then forgets the peer and closes its connections
  within 5 s. The other host closes this host's sessions within a second and
  refuses its credential from then on (`PEER_REVOKED`). If the other host
  also paired with this one (it dials this host too), that pairing is its
  own: unpair it there.

`ENDPOINT` is `ws://HOST:PORT` or `wss://HOST[:PORT][/PATH]` (query strings
are kept). `ws://` defaults to port 80, `wss://` to 443; `wss://` verifies the
server certificate and host name with the platform trust store. Use the same
endpoint string for pairing, listing, receiving and unpairing.

## A standalone worker

The daemon launches one worker per folder (`daemon --state … --supervised`).
You can also run one yourself on a folder state, without a daemon:

```sh
tkfs init --state D:\tkfs\standalone
tkfs init --state D:\tkfs\standalone --repo REPO_ID
tkfs daemon --state D:\tkfs\standalone --mount D:\Work\Standalone
tkfs daemon --state D:\tkfs\standalone
tkfs daemon --state D:\tkfs\standalone --supervised
```

- `init --state DIR` creates (or opens) a folder state and prints its status;
  `--repo` gives it an existing repository id.
- `daemon --state DIR [--mount PATH]` runs a worker on it. A mount failure is
  reported and the worker keeps serving. `--supervised` is what the daemon
  uses: the worker reports `Ready` once the store is recovered, mounts only
  when the daemon asks, and stops itself when no daemon has contacted it for
  10 minutes (`TKFS_UNSUPERVISED_EXIT_S`).
- Do not run a standalone worker on a state the daemon manages: the second
  worker fails with `ALREADY_RUNNING`.

## Filesystem commands

These talk to one folder's worker, found through its mount (`-C`) or its
state directory (`--runtime`), and present the worker's token
(`runtime.token` in its state directory): run them as the user the worker
runs as. They work the same on managed and standalone folders.

```sh
tkfs -C D:\Work\Projects status
tkfs --runtime D:\tkfs\data\states\STATE\runtime.json status
tkfs -C D:\Work\Projects branches
tkfs -C D:\Work\Projects conflicts
tkfs -C D:\Work\Projects state
tkfs -C D:\Work\Projects events
tkfs -C D:\Work\Projects history
tkfs -C D:\Work\Projects branch experiment
tkfs -C D:\Work\Projects checkout experiment
tkfs -C D:\Work\Projects publish shared-cut --current-state-only
tkfs -C D:\Work\Projects checkpoint -m "working version"
tkfs -C D:\Work\Projects restore CHECKPOINT_ID --branch restored
tkfs -C D:\Work\Projects resolve --conflict CONFLICT_ID --revision REVISION_ID
tkfs -C D:\Work\Projects recover ENTITY_ID docs/recovered.txt --conflict CONFLICT_ID
tkfs -C D:\Work\Projects import docs/report.pdf C:\Users\me\report.pdf
tkfs -C D:\Work\Projects mkdir docs/drafts
tkfs -C D:\Work\Projects rename docs/drafts docs/old-drafts --replace
tkfs -C D:\Work\Projects delete docs/old-drafts
tkfs -C D:\Work\Projects cat-object OBJECT_HASH
tkfs -C D:\Work\Projects bucket-export --directory D:\Exports\bucket
tkfs -C D:\Work\Projects stop
tkfs -C D:\Work\Projects stop --force
tkfs -C D:\Work\Projects pending-saves
tkfs -C D:\Work\Projects discard-pending-save ENTITY_ID
```

| Command | Effect |
|---|---|
| `status` | The worker's status: metadata and content counters (`content_complete` and the per-peer feed state `feed_peers`, each with its `caught_up`, cursor and the peer's tip, are separate claims), up to 20 current files with missing bytes, activation, inbox, mount and store health. `caught_up` and `metadata_caught_up` at the top are the previous release's lane observations and stay `false` on this release's peer path; `manage status` shows each folder's sync with each peer. |
| `branches`, `conflicts`, `state`, `events`, `history` | Read the branches, unresolved conflicts, current projection, journal and checkpoints. |
| `branch NAME`, `checkout NAME` | Create a private branch; switch the mount to a branch (refused while files are open, and while an unsaved file of the current branch could not be restored: `RECOVERY_REQUIRED`, see `pending-saves`). |
| `publish NAME --current-state-only` | Publish only the visible state as a new shared branch; private history stays here. |
| `checkpoint -m MESSAGE`, `restore CHECKPOINT --branch NAME` | Record a checkpoint; restore one into a new private branch. |
| `resolve --conflict ID… --revision ID` | Resolve conflicts by choosing a revision. |
| `recover ENTITY TARGET --conflict ID…` | Move a displaced entry of a reviewed conflict to a free path. |
| `import PATH SOURCE`, `mkdir PATH`, `rename PATH TARGET [--replace]`, `delete PATH` | Change files without a mount (headless folders). |
| `cat-object HASH` | Write one stored object's bytes to stdout. |
| `bucket-export --directory DIR` | Copy the shared objects to a local directory outside the mount and the state. |
| `stop [--force]` | Stop a **standalone** worker (refused while busy; `--force` saves unsaved files and detaches). For a managed folder use `tkfs manage stop`, or the daemon restarts it. |
| `pending-saves` | The folder's pending saves: unsaved files kept for a retry (`restored: true`: being saved), and each one that could not be restored with its `error` (its staging file is missing or unreadable, it belongs to another branch). Such a save is retried (1 s up to every 60 s) and reported as `RECOVERY_REQUIRED` in the folder's store health; it does not block Stop or other commands, only a `checkout` while it belongs to the current branch. One of another branch is restored when that branch is checked out, and its file cannot be opened for writing meanwhile. |
| `discard-pending-save ENTITY` | Drop a pending save that cannot be restored (its row and staging file): its unsaved bytes are lost. A restored one is refused (`PENDING_SAVE_RESTORED`). See the README runbook *Unsaved file that cannot be restored*. |
