# Orchestration restoration on the Wave 6 runtime

Date: 2026-10-28. Branch: `codex/wave6-replacement`. Status: design, awaiting
approval.

## 1. Intent

Telekinesis (`tkfs`) is a self-hosted, zero-cloud, local-first virtual
filesystem that keeps the owner's development environments identical across
their PC, laptop and VPS. It should feel like working on a NAS share, except
every device holds a full copy and keeps working offline; devices sync directly
with each other when they can reach one another.

The Wave 6 replacement (commit `32d8a237c`, 2026-10-23) fixed the write path
and local performance and is kept. It also deleted the orchestration layer that
the product spec depends on, although `docs/revamp/WAVE_6_REPLACEMENT.md`
scoped Wave 6 to data storage, management of stored data and transport only.
Deleted without being in scope: the orchestrator daemon, per-folder worker
supervision and adoption, the `manage` command set, the lane-structured
orchestrator-to-orchestrator and orchestrator-to-worker protocols,
`reset-pairing-code`, SIGINT/SIGTERM shutdown, the `orchestrator.toml`
configuration, `tests/docs_cli.rs`, and most of the pairing catalog. The
Windows GUI's `Desktop` type (`src/desktop.rs`) is the only multi-folder
manager left and it cannot be used headless.

This project restores the product-defined features on top of the new runtime.
The owner's words: "restore the rest of the features within the new
framework".

## 2. Goals

1. `tkfs` is one executable providing: the **orchestrator daemon** (one per data
   directory), the **state-specific worker daemon** (one per folder), the
   **`manage` client** (talks to the orchestrator), and the **filesystem
   commands** of one folder (status, conflicts, resolve, branch, …), found
   through the working directory.
2. Orchestrator ↔ worker communication over multiple lanes, lane 0 reserved for
   metadata and control.
3. Orchestrator ↔ orchestrator communication over multiple lanes, lane 0
   reserved for metadata and control; mutual authentication derived from the
   pairing code; TLS by reverse proxy.
4. Easy pairing: `manage reset-pairing-code` prints a fresh code once; `manage
   pair ENDPOINT` on the other device completes it; `unpair`, `peers`,
   `remote-list`, `receive` as before.
5. Folders and branches run **mounted or headless**; a mount can be attached
   and detached while the folder keeps syncing.
6. **Copy-on-write branches**, each mountable at its own path, **local only**
   (never replicated), merged back into the main branch with conflicts handled
   like sync conflicts.
7. **Open-time freshness**: opening a file whose newer version is known to exist
   on a reachable peer fetches that version first, within a bounded wait.
8. **Clock discipline**: UTC everywhere, a stored clock offset per peer, and a
   hard refusal to run on a machine whose clock is obviously wrong.
9. Conflicts are **only** surfaced and resolved through the CLI and GUI, locally
   or against a remote peer. No sidecar files, ever.
10. Linux/Docker/systemd deployments work headless from the documented
    commands, and the docs are checked against the binary again.

## 3. Non-goals (this round)

- Git-style three-way text merge of branches. A file changed on both sides is a
  two-head conflict; the design must leave room for a smarter merge later.
- Mounting a copy-on-write git worktree. Same: leave room, build nothing.
- A GUI makeover. The GUI becomes a client of the orchestrator and keeps the
  functions it has today (create folder, pair, start/stop, conflicts view);
  nothing more.
- Performance gates. Per the owner's 2026-10-28 instruction and `AGENTS.md`,
  correctness and implementation first, performance last.
- Closing W6-P0-MOUNT-LIFECYCLE, host mounts or cutover before its full
  acceptance. This design must respect the blocker; it does not resolve it.
- Any cloud, relay or account service. Peers talk directly; the VPS is just a
  peer that happens to be always on.

## 4. Anti-goals

Even if everything else works, the project has failed if:

- a `foo (conflicted copy).txt`, `.bak`, `.orig` or any other sidecar appears
  in a mount;
- a conflict is silently resolved by picking a side without the owner's
  decision;
- committed data is lost because a daemon or worker was terminated;
- a worker's termination depends on I/O against or retained handles to its own
  served namespace (see `AGENTS.md`);
- a branch's events leak to a peer;
- a daemon or mount starts on a machine whose clock predates 2020.

## 5. Constraints

- **Runtime kept.** `Engine` (`src/runtime/engine.rs`), the store and object
  model (`src/core.rs`, `src/content/*`), the session layer
  (`src/session/{conn,handshake,io,frame}.rs`, `src/wire.rs`, `src/ws.rs`),
  and the mount adapters (`src/mount.rs`, `native/mount.c`,
  `src/fuse_linux.rs`) stay. The adapters change only where multiple mounts
  per worker require it.
- **W6-P0-MOUNT-LIFECYCLE** (`docs/revamp/WAVE_6_MOUNT_LIFECYCLE_BLOCKER.md`):
  no self-handles; forced termination covered, not only orderly stop; bounded
  shutdown, dump collection and post-kill waits; stranded process is a failure;
  mounted hang repro only in an isolated Windows VM; no new mounts after a
  cleanup timeout. Everything below that spawns, stops or kills workers obeys
  this.
- **Authority scope** (`docs/revamp/AUTHORITY_SCOPE.md`): personal use;
  pairing is the admission boundary; a paired peer is trusted for every
  operation including remote conflict resolution; TLS/WSS or VPN protects the
  channel.
- **Platforms:** Windows (WinFsp, desktop GUI) and Linux (FUSE 3, headless).
  Workers and mounts on both. Orchestrator and `manage` on both.
- **Owner-only secrets:** tokens and the catalog are created `0600`; the data
  directory is the owner's.
- **No new third-party services or crates for IPC**: reuse the existing frame
  protocol and WebSocket code for both peer and loopback lanes.

## 6. Current state (what exists in the tree today)

| Area | Today | Pointer |
|---|---|---|
| CLI | `tkfs [--state DIR] init|daemon|status|…|gui`; one process = one folder; no orchestrator, no `manage`, no signals | `src/main.rs` |
| Folder process | `tkfs daemon [--mount P] [--listen A] [--advertise U]`: serves the store, one optional mount, its own peer listener, a loopback control file (`control.json`) | `src/orchestrator/mod.rs`, `control.rs` |
| Peer sync | per-process: `meta.exchange` feed + batched digest fetch over the session layer | `src/orchestrator/sync.rs` |
| Pairing | per-process catalog in the store DB: verifier, `reset_code`, `pair`, `unpair`, `peers`, HMAC key derivation | `src/orchestrator/catalog.rs` |
| Session layer | HELLO/AUTH handshake, lane roles (`LaneRole::{Control,Bulk,Probe}`), framing, priority queues, backpressure | `src/session/*`, `src/wire.rs` |
| Branches | `branches` table with snapshot; per-branch projection views; `fork`, `checkout` ops; one active branch per process | `src/core.rs`, `control.rs` |
| Conflicts | per-entity heads; `conflicts`, `resolve`, `preview-version` ops; GUI conflicts view | `src/core.rs`, `runtime/engine.rs`, `gui.rs` |
| Mount | one mount per process (`MOUNT_ALREADY_ATTACHED`), attached at startup only | `src/mount.rs`, `src/fuse_linux.rs` |
| Multi-folder | Windows GUI only, via `Desktop` (spawns `tkfs daemon` children, exclusive config lock) | `src/desktop.rs`, `gui.rs` |
| Config | `orchestrator.toml` format 3 written by the desktop, folders listed in it | `src/desktop.rs` |
| Docs | describe the master CLI (`-f`, `orchestrator`, `manage`); no test checks them | `docs/CLI.md`, `LINUX.md`, `CONFIGURATION.md`, `examples/docker-compose.yml`, `deploy/*.service.example` |

Master (pre-cutover) is the reference for everything orchestration-related:
`git show master:docs/ARCHITECTURE.md` (lanes §236-300, control plane §216-232,
adoption §170-179, framing §560-620), `master:docs/CLI.md`,
`master:docs/CONFIGURATION.md`, `master:src/orchestrator/{process,manage,
worker_client}.rs`, `master:src/worker/mod.rs`. Reuse names, codes and limits
from there unless this document says otherwise.

## 7. The design

### 7.1 Processes

**Orchestrator** (`tkfs [-f orchestrator.toml] orchestrator`, alias `start`).
One per data directory, guarded by `DATA/orchestrator.lock`. It:

- creates `orchestrator.toml` with defaults if missing ("auto-init"), refuses
  an invalid one without overwriting it;
- owns the **catalog** (`DATA/folders.sqlite`): folders, their intents
  (desired running, paused, mount gate), branches' mount intents, pairing
  verifier, peer credentials and metadata, clock offsets;
- supervises one **worker** per folder (spawn, adopt, heartbeat, backoff,
  crash-loop breaker, stop ladder; master ARCH §143-179);
- serves the loopback **control endpoint** for `manage` and the GUI
  (`DATA/orchestrator.json` + `DATA/orchestrator.token`);
- dials and accepts **peers** and runs the sync scheduler for every folder;
- handles **SIGINT/SIGTERM** (Linux) and console close/Ctrl-C (Windows) as
  `manage shutdown`: close peers, stop every folder cooperatively, and at the
  30 s bound **force-stop** any worker still busy (detach its mounts, exit
  the worker); committed data is durable and unsealed scratch files are
  re-sealed at the next start, as Wave 6 already guarantees. Exit 0, with a
  `FORCED_STOP` log line per folder it had to force (my call; under Docker
  and systemd there is no next daemon to adopt a left-running worker). The
  per-folder `manage stop STATE` keeps master's behaviour: a worker with
  files open refuses with `BUSY_VIEW` and stays running; `force-stop` is the
  explicit escalation.

**Worker** (`tkfs daemon --state STATE --supervised`). The current
single-folder process, kept, with these changes:

- it **no longer listens for peers** and never dials (master invariant 25).
  Its peer listener and `--listen/--advertise` go away;
- it serves a **loopback multiplexed session** for its orchestrator (§7.2) and
  writes `STATE/runtime.json`, `STATE/runtime.token` (owner-only, written
  after the previous `runtime.json` is removed) and `STATE/worker.json`
  (pid, start time, executable, version) for adoption;
- it prints one Ready line on stdout (`{"ready":true,repo,device,instance,
  pid,address,token,version,"ipc":4,mounts:[…]}`) and logs to
  `STATE/worker.log` with the token redacted;
- it exits when quiet after 10 min without orchestrator contact
  (`TKFS_UNSUPERVISED_EXIT_S`), as on master;
- it refuses to start when the clock is invalid (§7.7);
- it serves **zero or more mounts** at once (§7.5), attached and detached at
  run time.

The old unsupervised `tkfs daemon` (no `--supervised`) remains for tests and
for a hand-run single folder, with the same loopback protocol; the only
difference is the quiet-exit timer.

**Filesystem commands** (`tkfs status|tree|events|conflicts|resolve|
checkpoint|branch|checkout|mkdir|rename|delete|import|cat|flush|gc`) find the
worker through the working directory: `-C DIR` or the current directory inside
a mount → `.tkfs-runtime.json` at the mount root → `runtime.json`; or
`--runtime STATE/runtime.json`. They talk to the worker's control lane
directly (lane 0 RPC with `runtime.token`).

**`manage`** (`tkfs [-f orchestrator.toml] manage …`) talks only to the
orchestrator (`X-Tkfs-Token: <orchestrator.token>`), which forwards
folder-scoped requests to the right worker.

**GUI** (`tkfs gui`, Windows). A control-endpoint client. It starts an
orchestrator if none answers and then uses `manage`-equivalent requests plus
a `desktop-snapshot` aggregate. `Desktop`'s child ownership, exclusive lock
and blocked-state files are removed; its configuration loader becomes the
orchestrator's. Closing the window leaves the orchestrator running.

### 7.2 Lanes

Both links use the existing frame protocol (`src/session/*`): one WebSocket
connection per lane, lane roles `control | bulk | probe`, priority queues
(control first, then response bodies, then data chunks round-robin),
bounded data queue (32 MiB), HEARTBEAT after 5 s idle, dead after 20 s.

**Orchestrator ↔ orchestrator** (`tkfs-peer/4`, header `X-Tkfs-Sync: 4`).
Pool of `max(connections, 2)` lanes per peer. Lane 0 is **control**:
`meta.exchange` (per repo), `catalog.list`, `control.*` (remote conflicts,
§7.8), `clock.probe` (§7.7), `peer.enroll/revoke`. Lanes 1..N-1 are **bulk**:
`objects.*` only; objects sharded by digest across bulk lanes; control never
carries object bytes. Mutual HMAC auth from the paired secret (master ARCH
§568-589). Lane state machine, backoff, `Incompatible`, `Unauthorized`, shared
redial cadence as on master.

**Orchestrator ↔ worker** (`X-Tkfs-Sync: 4-loopback`, no HELLO/AUTH,
`X-Tkfs-Token: <runtime.token>`). The orchestrator opens `1 + bulk_lanes`
connections to the worker's loopback address: lane 0 **control** carries the
JSON RPC (`hello`, `status`, `watch{after_seq}` heartbeat long-poll,
`shutdown`, `conflicts`, `resolve`, `attach`, `detach`, `branch.*`,
and the folder-scoped `meta.exchange` the orchestrator proxies for peers);
lanes 1..N **bulk** carry `objects.want/read/write` between the worker's
pack store and the orchestrator's peer bulk lanes. The worker's `Service`
(`sync.rs`) stays the authority for feed and objects; the orchestrator is a
router that never stores objects itself (my call; keeps one copy of the data
and the worker as the single writer).

**Direction:** the retained session layer lets only the session's client
originate requests. The orchestrator is the client on every lane, loopback
included. Anything the worker needs from the orchestrator (object demand,
status changes, heartbeat) is delivered by completing the parked
`watch{after_seq}` long-poll on lane 0; the orchestrator re-parks it at once.
Demand therefore reaches the orchestrator in one round trip, not on a poll
interval.

Lane counts come from `[network] connections` (peers) and
`[network] worker_lanes` (loopback, default 2, range 1-8) in
`orchestrator.toml`.

### 7.3 Control plane and `manage`

Command set restored from master, STATE given as full id, unique ≥4-char
prefix or unique label:

| Group | Commands |
|---|---|
| Status | `status`, `watch`, `list`, `inspect STATE`, `diagnostics` |
| Folder | `create LABEL [--repo ID] [--mount P \| --headless]`, `configure-mount STATE [--mount P]` (stopped only), `start`, `stop [--no-wait]`, `force-stop`, `retry` |
| Mount | `attach STATE [--branch NAME] [--mount P]`, `detach STATE [--branch NAME] [--force]`, `mount-now STATE` |
| Sync | `pause STATE`, `resume STATE`, `revalidate STATE` |
| Branch | `branch create STATE NAME [--mount P \| --headless]`, `branch list STATE`, `branch merge STATE NAME`, `branch delete STATE NAME [--force]` |
| Conflicts | `conflicts STATE [--peer ENDPOINT]`, `resolve STATE ENTITY VERSION [--peer ENDPOINT]`, `preview STATE VERSION [--peer ENDPOINT]` |
| Pairing | `reset-pairing-code`, `pair ENDPOINT [--connections N]` (code from stdin or `--code`), `unpair INSTALLATION`, `peers`, `remote-list ENDPOINT`, `receive ENDPOINT REPO LABEL [--mount P \| --headless]` |
| Daemon | `shutdown` |

Rules carried over: a mutation is never resent (`CONNECTION_LOST`); read-only
calls retry once; `--json` prints the reply; errors print `CODE: message` and
exit 1; `stop` waits ~13 s, `force-stop` ~15 s; `create` is also available as
top-level `tkfs create`. `--request-id` and `--generation` keep their
meaning for filesystem mutations.

The orchestrator forwards folder-scoped `manage` operations to the worker's
lane 0 and returns its reply unchanged, adding only routing errors.

**Status model (my call):** `status`, `watch`, `list`, `inspect` and the GUI
snapshot report a reduced, Wave-6-native model, not master's hub/status/rules
model (inbox, quarantine, activation and acknowledgement classes have no
counterpart in the new runtime). Per folder: phase (`Starting | Running
{adopted} | Stopping | Stopped | Failed{code} | Backoff{until}`), mounts
(branch, path, state), sync (per peer: cursor sent/received, waiting rows,
last exchange, lane states), unresolved conflicts, clock offset, last error.
`revalidate` retries the folder's `waiting` rows; `mount-now` attaches
regardless of the sync gate; the gate is "cursor at source tip and no waiting
rows". `watch` renders the same snapshot as text, refreshed on change.

### 7.4 Configuration (`orchestrator.toml`, format 2)

Master's schema, plus one field:

| Field | Rule | Default |
|---|---|---|
| `format_version` | must be `2` | — |
| `data_directory` | relative to the file | `"data"` |
| `default_mount_directory` | `<dir>/<label>`, branches `<dir>/<label>@<branch>` | `"mounts"` |
| `maximum_running` | 1-64 | 8 |
| `[network] listen` | `""` = dial only | `"127.0.0.1:43180"` |
| `[network] advertise` | **new**: URL peers should dial (behind a TLS proxy); `""` = derive from `listen` | `""` |
| `[network] connections` | 1-16, effective pool ≥2 | 4 |
| `[network] worker_lanes` | **new**: loopback bulk lanes per worker, 1-8 | 2 |
| `[network] sync_interval_ms` | 50-60000 | 500 |
| `[freshness] open_wait_ms` | **new**: bound for open-time fetch, 0 disables, max 5000 | 250 |

Folders, mounts, headless flags, branches and pairing live in the catalog, not
in the file (master's rule). Unknown fields → `INVALID_CONFIG`.

**No legacy (owner's decision):** nothing is imported. Neither master-era data
directories (`folders.sqlite` with master's schema, master pairings) nor the
current desktop's format 3 `orchestrator.toml` are read. A file whose
`format_version` is not 2 fails with `CONFIG_VERSION_2_REQUIRED`; a data
directory holding a catalog with an unknown schema fails with
`CATALOG_INCOMPATIBLE` and the message names the directory to move aside.
Every machine starts fresh: `reset-pairing-code`, `pair`, `receive`.

### 7.5 Mounts, headless folders and multiple mounts per worker

A folder has a **main branch** and zero or more local branches. Each branch
has at most one **mount intent** (path or none) and a **gate**
(enabled/disabled) in the catalog. "Headless" is simply "no mount intent".

The worker serves N mounts simultaneously, one per `(branch, path)`:

- `src/mount.rs` and `src/fuse_linux.rs` move from the single `MOUNT` static
  to a registry keyed by branch; `attach`/`detach` become run-time control
  ops; each adapter instance holds a **branch-scoped view handle** of the
  Engine instead of using `store.active` (the per-branch projection views in
  `core.rs` already exist; the Engine's lookup/open/write paths gain a branch
  parameter or a `View` wrapper, builder's choice);
- the mount root carries `.tkfs-runtime.json` naming the state, the branch
  and the loopback address, so `tkfs -C` inside a branch mount addresses that
  branch;
- `detach` is refused while files are open on that mount (`BUSY_VIEW`),
  `--force` runs the bounded stop ladder; `attach` fails fast with
  `MOUNT_PATH_MUST_NOT_EXIST` semantics as master documents for Linux
  (directory created by the worker, removed on clean unmount), stale
  `fuse.tkfs` mounts of this folder's own previous worker are cleaned, other
  mounts never touched;
- all W6-P0 invariants apply per mount instance (no self-handles, caching
  with flush-and-purge on cleanup, bounded cleanup).

`checkout` (switching one mount between branches) is **removed**: a mount
shows the branch it was attached for. Pending saves stay bound to a branch.

### 7.6 Copy-on-write branches (local only)

- `branch create STATE NAME` records a new branch whose snapshot is the main
  branch's current tree digest (no copy). Writes in that branch produce events
  with `branch = NAME`; the main branch and peers never see them.
- **Never replicated:** the feed (`meta.exchange`) and object fetch serve only
  main-branch events and the objects they reference; a received event with a
  non-main branch is rejected (`PRIVATE_BRANCH_REJECTED`). Objects written
  only by a branch are not offered to peers until merge makes them
  main-reachable.
- **Merge** (`branch merge STATE NAME`): for every entity changed in the
  branch since its fork point, the branch's head version is brought into main
  with its causal parents intact. If main's head for that entity is still the
  fork-point version (or an ancestor of the branch head), main fast-forwards
  to the branch head. Otherwise main gets a second head and the entity is a
  conflict, listed and resolved exactly as a sync conflict. Deletions and
  renames follow the same rule per entity. The branch stays after merge
  (re-mergeable); `branch delete` removes it, refusing while mounted unless
  `--force`. Events' `id UNIQUE` means the builder decides between re-homing
  events or appending merge events whose parents are the branch versions;
  either way causal parents must be preserved so a later three-way merge can
  be built on the same DAG.
- A branch mount is served by the folder's worker (same store, same pack
  store); there is no second process.
- Nothing here precludes a later git-worktree-backed branch: the branch
  abstraction is "a named root digest plus a local event lane".

### 7.7 Clocks

- All stored and transmitted timestamps are UTC (`*_ms` since epoch); display
  converts.
- **Sanity floor:** `clock::check()` refuses with `CLOCK_INVALID` when the
  system clock reads before 2020-01-01T00:00:00Z. Called at orchestrator
  start, worker start, every `attach`, and on each lane handshake (a peer
  with an invalid clock is refused with the same code). No mount, daemon or
  sync runs on an invalid clock.
- **Peer offsets:** each control-lane handshake and each `clock.probe` (every
  `sync_interval_ms × 60`, and on lane Ready) exchanges send/receive
  timestamps; the dialer computes offset and round-trip like NTP's
  four-timestamp method, keeps a smoothed value per peer in the catalog
  (`peer_meta.clock_offset_ms`, `clock_rtt_ms`, `clock_measured_at`), and
  logs a warning above 5 s. Offsets are applied when **interpreting a peer's
  timestamps** (file times, event times shown as "modified 2 min ago",
  tie-break display order of conflict heads). Version ordering itself stays
  **causal** (explicit parent versions), never clock-based; this is my call
  and the reason the offset is a correction for metadata and UI, not for
  correctness.

### 7.8 Open-time freshness

**What a mount shows (owner's decision):** the version of an entity that the
filesystem exposes (`visible`, in listings, sizes and opens) is the **newest
head whose bytes are fully local**. A newer head whose objects have not
arrived is known to the projection (it is the head for conflict and sync
purposes) but not shown until its bytes are present. This replaces today's
behaviour where `LocalContent::ensure` fails with `OBJECT_MISSING`. A write
made against the older shown version while a newer head exists produces a
two-head conflict, resolved like any other; nothing is merged silently.

When a mount opens a file:

1. The worker checks its projection: is there a head newer than the shown
   version whose bytes are not local? (Metadata arrives continuously on lane
   0, so this is a local lookup.)
2. If so, the worker records **demand** for the missing objects
   (`Engine::demand`) and completes the orchestrator's parked `watch`
   long-poll immediately with that demand (the worker never dials; the
   orchestrator is always the session client, as the retained session layer
   requires). The orchestrator fetches the objects on its peer bulk lanes,
   prioritised ahead of background prefetch, and writes them to the worker
   over the loopback bulk lanes (`objects.write`). The opener waits on a
   condition variable at most `open_wait_ms`.
3. If the bytes arrive in time, the shown version advances and the open sees
   the newer version. If not (peer unreachable, slow), the open proceeds with
   the version shown so far; the fetch continues and the shown version
   advances when the bytes land, invalidating the cached entry so the next
   open sees it.

Nothing is fetched speculatively at open time beyond the entity being
opened; background prefetch (`DigestFetch::prefetch`) continues as today,
moved to the orchestrator side of the bulk lanes.

### 7.9 Conflicts, local and remote

- Two heads for an entity = one conflict. Listed by `tkfs conflicts`, `manage
  conflicts STATE` and the GUI; previewed by `preview-version`; resolved by
  `resolve ENTITY VERSION`, which appends an event with both heads as parents.
  The filesystem shows the deterministic `visible` version until then and
  never writes a sidecar.
- **Remote:** `manage conflicts STATE --peer ENDPOINT` (and `resolve`,
  `preview`) sends `control.conflicts|resolve|preview` to that peer's
  orchestrator on lane 0, which forwards to its worker and returns the reply.
  The resolution is an ordinary event in the peer's main branch and replicates
  back like any other. A paired peer is authorised for this (authority scope).
- Branch-merge conflicts are the same records with `branch = main` and are
  listed in the same views.

### 7.10 Pairing

Master's protocol, kept verbatim: 64-hex code, salted SHA-256 verifier only
stored, HELLO/HELLO_ACK/AUTH/AUTH_OK with mutual HMAC transcript, `peer.enroll`
with a 32-byte secret, `peer-hmac-v1` for later sessions, `reset-pairing-code`
rotates the verifier, revokes every credential and sends
`GOAWAY{PAIRING_CODE_CHANGED}`. The existing `catalog.rs` functions
(`reset_code`, `pair`, `unpair`, `paired_key`, `probe`) move from the store DB
to the orchestrator's catalog unchanged in behaviour. `pair` accepts the code
on stdin (default) or `--code`.

### 7.11 Linux, Docker, systemd

- `examples/docker-compose.yml`: image installs `ca-certificates libssl3t64`
  (plus `fuse3` and the FUSE device/cap/AppArmor block only when mounting,
  commented by default); command is `exec ./tkfs -f orchestrator.toml start`
  (auto-init); folders are added with
  `docker compose exec tkfs-daemon ./tkfs manage create NAME --headless`;
  `stop_grace_period: 60s`; SIGTERM is handled by the orchestrator itself, no
  shell trap.
- `deploy/tkfs-orchestrator.service.example`: `ExecStart … orchestrator`,
  `ExecStop … manage shutdown`, `TimeoutStopSec=60`, `KillMode` note as on
  master.
- `docs/LINUX.md`, `docs/CLI.md`, `docs/CONFIGURATION.md`,
  `docs/ARCHITECTURE.md`, `README.md` updated; `tests/docs_cli.rs` restored so
  every command line in the docs is checked against `--help`.

### 7.12 Error codes

Reuse master's codes where the situation is the same (`ALREADY_RUNNING`,
`ACCESS_DENIED`, `ORCHESTRATOR_NOT_RUNNING`, `CONNECTION_LOST`,
`FOLDER_NOT_FOUND`, `MOUNT_PATH_MUST_NOT_EXIST`, `BUSY_VIEW`,
`PRIVATE_BRANCH_REJECTED`, `PREVIOUS_WORKER_UNRESPONSIVE`, `CRASH_LOOP`,
`STALE_VIEW`, …). New: `CLOCK_INVALID`, `BRANCH_MOUNTED` (delete without
`--force`), `BRANCH_NOT_FOUND`, `PEER_UNREACHABLE` (remote conflict ops),
`CATALOG_INCOMPATIBLE`, `FRESHNESS_TIMEOUT` and `FORCED_STOP` (both logged,
not returned to the opener/caller). All codes documented in `README.md`'s
error-class table.

## 8. Left to the builder

- Module layout for the orchestrator (suggested: `src/orchestrator/{hub,
  process,worker_client,manage,config}.rs` as on master, adapted) and for the
  worker-side control handlers.
- Catalog schema details beyond the fields named here; SQLite as today.
- Exact Engine API shape for branch-scoped views (parameter vs. wrapper type).
- Whether branch merge re-homes events or appends merge events (§7.6).
- Heartbeat/long-poll parameters, backoff curves, pool sizes: take master's
  numbers unless a measured reason says otherwise.
- Test strategy, with these required: forced-termination lifecycle tests per
  the blocker; a two-orchestrator sync test over loopback with ≥2 bulk lanes
  proving control never carries object bytes; branch isolation (no branch
  event leaves the process); merge fast-forward and merge conflict; clock
  floor refusal; open-time freshness with a reachable and an unreachable
  peer; `docs_cli` restored.
- Order of work. Suggested phases: (1) worker loopback lanes + control RPC +
  runtime files; (2) orchestrator process, config, catalog, `manage`, signals;
  (3) peer lanes moved to the orchestrator, pairing, `reset-pairing-code`;
  (4) clocks; (5) multi-mount, attach/detach, headless; (6) shown-version
  semantics and open-time freshness; (7) branches and merge; (8) remote
  conflicts; (9) GUI as client; (10) docs, compose, systemd, `docs_cli`.

## 9. Decisions recorded from the conversation (owner's words, 2026-10-28)

- Wave 6 was scoped to storage, management and transport; orchestration
  removal is a regression to restore.
- The experience is "a NAS, but local-first"; only committed and synchronized
  versions appear on the next host.
- Opening a file with a newer remote version fetches it first.
- UTC; stored clock offsets, rsync/NTP style; clock before ~2020 ⇒ refuse
  mounting and running any daemon.
- Conflicts are resolved in UI or CLI; no "copy of the backup (2).bak" files.
- Querying and resolving conflicts on a remote peer: yes, as a small detour.
- Branches are mounted copy-on-write slices so builds don't see partial edits
  from other applications; merge conflicts use the sync-conflict mechanism;
  git-style merge later.
- Branches are local only, not synchronized.
- Master's CLI design "looked solid"; reinventing is allowed where useful.
- GUI: keep current functions; makeover later.

## 10. Calls I made (check these while reading)

1. The orchestrator routes objects between peers and workers but never stores
   them (§7.2).
2. Causal version ordering; clock offsets correct timestamps and display only
   (§7.7).
3. `open_wait_ms` default 250 ms; local version opens on timeout (§7.8).
4. `checkout` removed; one mount = one branch (§7.5).
5. Branch mount default path `<mounts>/<label>@<branch>` (§7.4).
6. `[network] advertise`, `[network] worker_lanes`, `[freshness]` added to
   master's schema (§7.4).
7. Branches survive merge until deleted (§7.6).
8. Clock probe cadence `sync_interval_ms × 60`, warn above 5 s (§7.7).
9. `tkfs-peer/4` / `ipc: 4` protocol version bump (§7.2).
10. Worker signals object demand by completing the parked `watch` long-poll;
    the orchestrator stays the only session client (§7.2, §7.8).
11. Orchestrator `shutdown` force-stops still-busy workers at the 30 s bound;
    `manage stop STATE` keeps `BUSY_VIEW` (§7.1).
12. Reduced Wave-6-native status model for `status/watch/list/inspect` and the
    GUI snapshot (§7.3).

Owner's answers to the reviewer's questions (2026-10-28): a mount shows the
newest head whose bytes are local, with fetch continuing in the background
(§7.8); no legacy data anywhere, greenfield, so nothing is imported (§7.4).
