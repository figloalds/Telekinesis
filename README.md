# Telekinesis

Telekinesis (`tkfs`) keeps versioned folders in sync between computers. Each
folder is a repository of immutable objects with a history (checkpoints,
branches, conflicts kept as alternatives instead of overwritten files). On
Windows a folder can be mounted as a directory or drive letter through
WinFsp, or run **headless** (synced, no mount). Paired computers exchange
changes and download missing file content on demand.

`tkfs` is a single executable that contains:

- the **desktop app** (Windows): what you run by double-clicking `tkfs.exe`;
- the **orchestrator**: a background service, one per data directory, that
  owns your folders, talks to paired computers and runs one **worker** per
  folder;
- the **command line** (`tkfs manage …` and the folder commands), for
  scripting, servers and Linux.

The desktop app and the command line are both clients of the same
orchestrator and use the same configuration file, so you can use either one
(or both at once).

> **Status: mounts are still being qualified.** Windows mounts work but the
> mount-lifecycle blocker
> ([W6-P0-MOUNT-LIFECYCLE](docs/revamp/WAVE_6_MOUNT_LIFECYCLE_BLOCKER.md)) is
> still open, and directory mounts are affected the same way as drive letters.
> Keep mounted testing in an isolated Windows VM, and use **Headless** for
> data you care about. This release does not read stores or journals from
> earlier releases.

## Requirements

- **Windows 10/11 x64** for the desktop app. Mounting needs
  [WinFsp](https://winfsp.dev/) **2.2 Beta5 (2.2.26286) or newer**. Older
  versions are refused with `WINFSP_LOAD_FAILED`. Headless folders don't
  need WinFsp.
- **Linux x64** runs the orchestrator and the command line (no desktop app).
  See [Linux deployment](docs/LINUX.md).
- **Clocks** must be correct (NTP). A clock earlier than 2020 is refused,
  and a peer whose clock is more than 5 s off is logged.

To build from source, see [Building](#building-from-source).

---

## Using the desktop app (Windows)

### 1. Start it

Run `tkfs.exe` (or `tkfs gui`). On the first run it creates these next to
`tkfs.exe`:

| Path | What it holds |
|---|---|
| `orchestrator.toml` | The configuration ([reference](docs/CONFIGURATION.md)) |
| `data\` | Your folders' stores, the catalog, logs |
| `mounts\` | The default place where folders are mounted |

The app then starts the orchestrator in the background, or attaches to the
one that is already running. **Closing the window does not stop your
folders**: the orchestrator keeps running and keeps syncing. Open the app
again at any time to see it.

> Put `tkfs.exe` somewhere writable, not `Program Files`. Otherwise the
> first run fails with `CONFIG_WRITE_FAILED`. To keep the configuration
> elsewhere, set the `TKFS_CONFIG` environment variable to its path.

### 2. Find your way around

- **Left sidebar**:
  - **Folders** and **Activity** switch the main view.
  - **Peers** lists the paired computers (click one for details).
  - **+ Pair with a host** connects to another computer.
  - The **Host** panel shows the orchestrator's state, version, listen
    address and installation id. **Log** opens the orchestrator log and
    **Settings** opens the host settings.
- **Top of the Folders view**: totals for *Folders running*, *Mounted*,
  *Peers online* and *Transfer*.
- **Folder table**: one row per folder, with its sync state, its mount
  (with **Open** to show it in Explorer, or **Change**) and its worker state
  (with **Start** / **Stop**). **Details** opens everything else.

If the orchestrator stops, a banner says so and offers **Start
orchestrator**. The app never restarts it on its own.

### 3. Create a folder

1. Click **New folder**.
2. Type a **Folder name**.
3. Choose the **Mount point**:
   - leave it empty to mount at `mounts\<name>` (the default);
   - type a name to mount under `mounts\`;
   - type a full path (`D:\Work\Projects`) or a drive letter (`X:`) to
     mount there. The path must not exist yet. WinFsp creates it.
   - or tick **Headless** to sync without a mount.
4. Click **Create & mount** (or **Create** when headless).

The folder starts right away. Click **Open** on its row to open it in
Explorer and use it like any other folder.

### 4. Share folders between two computers

Pairing connects two orchestrators. Call the computer that already has the
folder **host A** and the one that should receive it **host B**.

**On host A (the one being connected to):**

1. Make it reachable from the network. By default it listens only on
   `127.0.0.1`. Open `orchestrator.toml` (the path is shown in
   **Settings**) and set:

   ```toml
   [network]
   listen = "0.0.0.0:43180"
   ```

   Then restart the orchestrator: **Settings → Stop orchestrator**, then
   **Start orchestrator** on the banner. Allow port 43180 in the firewall.
   For access over the internet, put a TLS proxy in front of it
   ([remote access](docs/CONFIGURATION.md#remote-access-through-a-tls-proxy)).
2. Open **Settings → Generate a new pairing code**, then **Copy**. The code
   is shown only once. Generating a new code disconnects every host that
   paired with the old one.

**On host B:**

1. Click **Pair with a host**.
2. Enter **Host address** `ws://HOST-A:43180` (or `wss://…` behind a TLS
   proxy) and paste the **pairing code**. Click **Pair**.
3. Host A's folders appear in the list. Select one, choose a mount point or
   **Headless** as when creating a folder, and click **Receive & mount**.

The folder appears on host B and starts downloading. You can follow it in
**Details → Sync** and **Content**.

> **One pairing moves content both ways.** Edits made on host B reach host A
> without pairing in the other direction. Pairing both ways is needed only
> against a peer running an older version, which is pulled from but takes no
> pushes (`manage peers` shows `two_way_content: false` for it).

### 5. Manage a folder (Details)

**Details** on a folder row opens its panel.

**Actions**

| Action | What it does |
|---|---|
| **Start** / **Stop** | Run or stop the folder's worker. Stopping is refused while files in the mount are open: close them first (or use `tkfs manage force-stop` from the command line). |
| **Pause sync** / **Resume sync** | Stop or restart syncing with every peer. The folder itself keeps working. |
| **Change mount point** | Choose a new mount point or switch to headless. The folder must be stopped, and the app offers to stop it. Start it again afterwards. |
| **Open folder** | Open the mount in Explorer. |
| **Open log** | Open this folder's `worker.log`. |
| **Checkpoint** | Record a snapshot of the current state. |
| **Clean up storage** | Remove objects nothing refers to anymore. |
| **Restart** | Stop and start the worker. |

**Tabs**

| Tab | What it shows |
|---|---|
| **Overview** | State, mount, and what the first sync is waiting on |
| **Sync** | Each peer that shares the folder: metadata, events, download/upload, progress, errors |
| **Versions** | **Fork branch** (checkpoint, then branch from it) and **Check out** a branch |
| **Content** | Files whose content is not downloaded yet, with their progress |
| **Conflicts** | Files edited on two computers at once. Preview the competing revisions, then **Use selected revision**, or **Recover at path** to keep one under a new name. The other versions stay in history. |
| **Errors**, **Activity** | This folder's errors and recent events |

### 6. Peers and host settings

- Click a peer in the sidebar to see its connections (lanes), round-trip
  time and shared folders. From there, **Browse folders** receives more
  folders, **Pair again** pairs after its code changed, and **Disconnect**
  unpairs it.
- **Settings** shows the configuration path and listen address, generates
  pairing codes and has **Stop orchestrator**. Stopping it stops every
  folder and disconnects peers until you start it again.

---

## Using the command line

Everything the desktop app does is also available from the command line,
and on Linux the command line is the only interface. Every command takes
`-f orchestrator.toml` (or `TKFS_CONFIG`) to choose the configuration. On
Windows the default is the file next to `tkfs.exe`, so the CLI and the
desktop app share it.

Full reference: [command reference](docs/CLI.md) ·
[configuration](docs/CONFIGURATION.md) · [Linux, systemd and Docker](docs/LINUX.md).

### Run the orchestrator

```sh
tkfs -f orchestrator.toml start      # foreground; creates orchestrator.toml and data/ if missing
tkfs -f orchestrator.toml manage shutdown
```

Ctrl-C, SIGINT and SIGTERM shut it down cleanly too. Run `manage` commands
as the same user as the orchestrator. On Windows you can skip `start`: the
desktop app starts the orchestrator itself.

### Folders

```sh
tkfs manage create Projects                    # headless (no --mount)
tkfs manage create Projects --mount X:         # or --mount D:\Work\Projects, /srv/tkfs/Projects
tkfs manage status                             # one line per folder, plus peers
tkfs manage watch                              # status every second
tkfs manage stop Projects                      # refused (BUSY_VIEW) while files are open
tkfs manage force-stop Projects                # saves pending writes, then stops anyway
tkfs manage start Projects
tkfs manage configure-mount Projects --mount X:   # stopped folders only; no --mount = headless
tkfs manage pause Projects                     # stop syncing; resume restarts it
tkfs manage retry Projects                     # after CRASH_LOOP
```

A folder argument can be its label, its full id, or an id prefix of at
least 4 characters (the `[id]` shown by `manage status`). Unlike the desktop
app, `create` without `--mount` runs the folder headless.

### Pairing

```sh
# Host A: set [network] listen = "0.0.0.0:43180", restart, then:
tkfs manage reset-pairing-code                          # prints the code once
tkfs manage reset-pairing-code --qr                     # also a QR code for a phone

# Host B:
echo "$CODE" | tkfs manage pair ws://hosta:43180         # code read from stdin
tkfs manage remote-list ws://hosta:43180                 # folders host A publishes
tkfs manage receive ws://hosta:43180 REPO_ID Projects --mount X:
tkfs manage peers
tkfs manage unpair INSTALLATION_ID
tkfs manage set-endpoint INSTALLATION_ID ws://newhost:43180   # peer changed address: no code
```

One `pair` moves file content both ways. Pair in both directions only
against a peer running an older version (`two_way_content: false` in
`manage peers`).

A host with `listen = ""` can still pair (it only dials out). The other host
keeps the pairing but never dials back.

### Working inside one folder

Folder commands act on one folder's state directory (`DATA/<folder id>`,
shown by `manage list`) and print JSON:

```sh
tkfs --state data/FOLDER_ID status
tkfs --state data/FOLDER_ID tree
tkfs --state data/FOLDER_ID conflicts
tkfs --state data/FOLDER_ID resolve ENTITY_ID VERSION_ID
tkfs --state data/FOLDER_ID checkpoint
tkfs --state data/FOLDER_ID import docs/report.pdf ./report.pdf
tkfs --state data/FOLDER_ID cat docs/report.pdf > report.pdf
```

See [folder commands](docs/CLI.md#folder-commands) for branches, renames,
deletes, `flush`, `gc` and raw `rpc`.

### Logs and files

| File | Contents |
|---|---|
| `DATA/logs/orchestrator.log` | The orchestrator (JSON lines, rotated) |
| `DATA/<folder id>/worker.log` | One folder's worker |
| `DATA/orchestrator.json`, `DATA/orchestrator.token` | The running orchestrator's address and access token (removed on exit) |

### Limits of this release

- A single file larger than about 8 GiB cannot be replicated
  (`SYNC_OBJECT_TOO_LARGE`).
- Mount points must not exist yet (`MOUNT_POINT_EXISTS`), and two folders
  cannot share one (`MOUNT_IN_USE`).
- At most `maximum_running` folders (default 8) run at once (`WORKER_LIMIT`).
- On Linux, folders run headless (see [Linux](docs/LINUX.md)).

---

## Error classes and what to do

A failed command prints `CODE: message` on stderr and exits with status 1.
The desktop app shows the same codes. The code tells you what to do:

| Code | Meaning | What to do |
|---|---|---|
| `CLOCK_INVALID` | The system clock reads before 2020-01-01 UTC (or a peer's does). The orchestrator, workers and `manage pair` refuse to run. | Fix the clock (NTP), then start again. |
| `CATALOG_INCOMPATIBLE` | The data directory holds a catalog this build cannot read (another schema version, or a `folders.sqlite` of an earlier release). Nothing is migrated or written. | Move the data directory aside and start fresh (`reset-pairing-code`, `pair`, `receive`). |
| `CONFIG_VERSION_2_REQUIRED`, `INVALID_CONFIG` | `orchestrator.toml` is not a format 2 file of this release, or has unknown fields. It is never overwritten. | Fix the file, or remove it to get a fresh one. |
| `DESKTOP_CONFIG_UNSUPPORTED` | The configuration was written by an older desktop build (`format_version = 3` or a `folders` list). It is never overwritten or migrated. | Move it aside; tkfs creates a new `orchestrator.toml`. |
| `CONFIG_WRITE_FAILED` | A missing `orchestrator.toml` cannot be created at its location (read-only or missing directory, for example `Program Files`). | Pass `-f PATH` or set `TKFS_CONFIG` to a writable location. |
| `CONFIG_LOCATION_CHANGED` | A Windows command found neither `-f` nor `TKFS_CONFIG` and an `orchestrator.toml` in the current directory, but uses the one next to `tkfs.exe` (printed as a notice; the command continues). | Pass `-f orchestrator.toml` to use the current directory's file. |
| `ORCHESTRATOR_START_FAILED` | The desktop started an orchestrator that exited with an error (exit status other than 75), or could not be waited for. | Read `DATA/logs/orchestrator.log` for the reason. |
| `ORCHESTRATOR_START_TIMEOUT` | No orchestrator answered within 30 s of the desktop starting one. A start that had to be killed and did not go also reports `STRANDED_PROCESS`. | Read `DATA/logs/orchestrator.log`; end the stranded process if one is named. |
| `INVALID_FOLDER_NAME` | The desktop cannot use a folder's label as a mount directory name (path separators, `<>:"/\|?*` or control characters). | Rename the folder, or choose a mount directory. |
| `LISTEN_FAILED` | The `[network] listen` address cannot be bound. | Free the port or change `listen`. |
| `ALREADY_RUNNING` | Another orchestrator (or worker) holds this data directory (exit status 75). | Use that one, or stop it first. |
| `ORCHESTRATOR_NOT_RUNNING`, `WS_CONNECT_FAILED` | No orchestrator answers for this configuration's data directory. | Start it (`tkfs -f FILE start`); run `manage` as its user. |
| `ACCESS_DENIED` | The orchestrator's or worker's token cannot be read or does not match. | Run as the user the orchestrator runs as. |
| `CONNECTION_LOST` | The connection dropped after a change was sent; it is never resent. | Check `manage status` to see whether it happened. |
| `ORCHESTRATOR_STOPPING` | The orchestrator is shutting down. | Wait for it to exit and start it again. |
| `FORCED_STOP` | Logged: a worker did not stop in time at shutdown and was forced or killed. Committed data is durable. | Nothing; look at `worker.log` if it repeats. |
| `STRANDED_PROCESS` | A worker could not be shown gone or killed within its bound. | Find the process (`worker.json` names its pid) and end it. |
| `ORCHESTRATOR_FAILED`, `ORCHESTRATOR_NOT_READY` | The Android app's embedded orchestrator ended with an error, or did not take requests within 15 s of starting. | Read the error text; restart the app. |
| `WORKER_NOT_READY` | The embedded worker of a folder did not come up within 15 s. | Retry; look at the folder's `manage inspect` phase. |
| `STRANDED_WORKER` | An embedded worker thread (or the embedded orchestrator) did not stop within its bound. | Restart the app process. |
| `INVALID_DOCUMENT_ID` | An app asked the Android provider for a document id that is not `FOLDER_ID:/path`. | Pick the file again in the Files app. |
| `ALREADY_EXISTS` | A rename or move would land on a name that is taken. Names that differ only in case or Unicode form are the same name. | Pick another name. |
| `ROOT_IMMUTABLE` | Something asked to rename or delete a folder's root. | Rename or delete what is inside it. |
| `NATIVE_PANIC` | A call into the Android app's native code hit a bug. The call failed and the app kept running. The panic is in logcat under the tag `tkfs`. | Retry; if it repeats, report it with `adb logcat -s tkfs`. |
| `WORKER_LIMIT` | `maximum_running` folders already have a worker. | Stop one, or raise `maximum_running` and restart. |
| `FOLDER_REQUIRED` | A folder command has neither `--state` nor `--runtime`. | Give `--state DIR` or `--runtime STATE/runtime.json`. |
| `FOLDER_NOT_FOUND`, `AMBIGUOUS_FOLDER` | The folder argument matches no folder, or more than one. | Use more of the id, or the exact label (`manage list`). |
| `FOLDER_RUNNING`, `FOLDER_NOT_RUNNING` | The command needs the folder stopped (`configure-mount`), or running. | `manage stop` / `manage start` it first. |
| `MOUNT_IN_USE` | `create`, `receive` or `configure-mount` names the mount point of another folder. Nothing is created or changed. | Choose another `--mount`, or clear that folder's mount. |
| `MOUNT_POINT_EXISTS` | Something already occupies the mount point (Windows; WinFsp creates the directory itself), possibly an empty leftover. Nothing is created or changed. | Choose a path that does not exist yet, or remove the empty leftover. |
| `MOUNT_FAILED`, `WINFSP_LOAD_FAILED` | The worker could not mount (it exits and the folder backs off). WinFsp older than 2.2.26286 (2026 Beta5) is refused before a volume exists. | Read `worker.log`; install WinFsp 2.2 Beta5 or newer. |
| `FOLDER_INIT_FAILED` | A new folder's store could not be created. Nothing of the folder is kept. | Fix the cause it names (disk space, permissions) and run `create` / `receive` again. |
| `WORKER_NOT_RUNNING` | The folder's worker is not running (`rpc`). | `manage start` the folder. |
| `BUSY_VIEW` | The worker has files open and refuses to stop. | Close them, or `manage force-stop`. |
| `CRASH_LOOP` | The folder's worker failed 5 times within 10 minutes and is not restarted. | Read `worker.log`, fix the cause, `manage retry`. |
| `OP_NOT_ALLOWED` | A peer or control request names an operation that lane or endpoint does not serve. | Use `manage` or the folder commands. |
| `REPO_NOT_PUBLISHED` | The paired host publishes no running, unpaused folder of that repository. | `manage remote-list ENDPOINT`; start or resume the folder there. |
| `OBJECT_NOT_WANTED` | A paired peer pushed an object this host never listed to it (or listed and since evicted or stored). Only that object is refused; the session stays usable. | None: the peer asks `objects.wanted` again. |
| `INVALID_PEER_OBJECT` | A pushed or written object's digest does not match its bytes. Only that object is refused. | None for the operator; the sending peer has a bug or a corrupt pack. |
| `PEER_PUSH_UNAVAILABLE` | `manage sync-now`: a peer running an older version takes no pushes, so nothing of this host's went to it. Metadata still moved, and it can still fetch from this host if it dials it. | Upgrade the peer. |
| `SYNC_NOW_ENDED` | `manage end-bursts` ended this `sync-now` early (the Android transfer job was stopped). Reported per folder; what was done stays done. | Run it again. |
| `SYNC_NOW_TIMEOUT` | `manage sync-now` ran out of time with nothing failing, or a peer's lanes never came up with no failure to report. If something was failing, the reply names that instead. Reported per folder; what was done stays done. | Run it again; check `manage peers`. |
| `REPO_ALREADY_PRESENT` | This orchestrator already has a folder of that repository. | Use that folder. |
| `PAIRING_CODE_CHANGED`, `INVALID_PAIRING_CODE`, `PEER_REVOKED` | The peer's code was reset, the code is wrong, or the peer revoked this host. | Get the current code (`reset-pairing-code` there) and `manage pair` again. |
| `PEER_NOT_AT_ENDPOINT` | `manage set-endpoint`: nothing at that address answers as the installation with its credential (another host, a wrong port, or the peer is down). Nothing was changed. | Check the address and that the peer is running; `manage peers` shows the installation. |
| `PEER_LANES_SKIPPED` | Logged: the connections to a paired peer could not be set up (for example its credential is missing). | Restart the orchestrator; if it repeats, `manage unpair` and `manage pair` again. |
| `CLOCK_OFFSET_LARGE` | Logged: a peer's clock differs from this one by more than 5 s. | Fix the clocks (NTP). |
| `SYNC_PUSH_FAILED` | Logged once per distinct error: pushing objects to a peer failed or timed out. That peer rests briefly and is tried again. | Nothing if it clears. |
| `SYNC_PUSH_REFUSED` | Logged: a peer refused a pushed object (for example `OBJECT_NOT_WANTED`). Only that object is dropped. It is offered again after a backoff (5 s doubling to 5 minutes) if still wanted, and `manage sync-now` reports its code. | None. |
| `SYNC_FETCH_FAILED` | Logged: fetching objects from a peer failed or timed out. On a timeout the folder asks that peer for smaller batches and grows them back as replies arrive. | Nothing if it clears. |
| `SYNC_OBJECT_TOO_LARGE` | Logged: a peer holds an object too large for any reply, the content list (manifest) of a file larger than about 8 GiB. It is not replicated in this release. | Keep such files out of synced folders for now. |
| `DESKTOP_REQUIRES_WINDOWS` | `gui` (or `tkfs` with no command) on a non-Windows host. | Use `manage`. |

---

## Building from source

```powershell
cargo build --release          # target/release/tkfs.exe (tkfs on Linux)
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt --check
```

`scripts/build.py` builds the Windows and Linux runnables, and
`scripts/package_desktop.py` assembles a portable desktop folder.
`scripts/bench.py` is the performance gate ([validation](docs/VALIDATION.md)).

## More documentation

- [Architecture](docs/ARCHITECTURE.md)
- [Command reference](docs/CLI.md)
- [Configuration](docs/CONFIGURATION.md)
- [Linux, containers and systemd](docs/LINUX.md)
- [Validation and benchmark gate](docs/VALIDATION.md)
- [Wave 6 replacement design](docs/revamp/WAVE_6_REPLACEMENT.md) and
  [authority scope](docs/revamp/AUTHORITY_SCOPE.md)

GPL-3.0-or-later. See [LICENSE-GPL-3.0.txt](LICENSE-GPL-3.0.txt), the
[Slint license](LICENSE-SLINT-ROYALTY-FREE-2.0.md) and
[third-party notices](THIRD-PARTY-NOTICES.md).
