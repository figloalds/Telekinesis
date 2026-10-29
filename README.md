# Telekinesis

Versioned folders backed by immutable objects and a host-filesystem working
layer. WinFsp on Windows and FUSE on Linux expose the folder; paired sessions
replicate entity events and hydrate missing content by digest.

**Windows mount lifecycle blocker (OPEN, P0):** the no-self-handle fix and
repeated isolated lifecycle matrices pass; full acceptance remains open. See
[W6-P0-MOUNT-LIFECYCLE](docs/revamp/WAVE_6_MOUNT_LIFECYCLE_BLOCKER.md) for the
qualified WinFsp dependency and evidence. Per the user's 2026-10-28 decision,
finish Wave 6 correctness and implementation before the final performance gates.
Keep mounted repro/quickstart/benchmarks in an isolated Windows VM until full
acceptance. Directory mounts require the same isolation as drive-letter mounts.

This checkout implements the greenfield [Wave 6 replacement](docs/revamp/WAVE_6_REPLACEMENT.md).
Host mounted qualification and cutover remain blocked by the demand above. Historical
stores and the previous write journal are not supported by this format.

```powershell
cargo build --release
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt --check
```

## Quick start

`tkfs` is one executable: the orchestrator (one per data directory), the
worker it runs for each folder, and the `manage` client. Folders run headless
for now; mounts, branches, remote conflict resolution and the desktop as an
orchestrator client follow in later plans.

```sh
# Terminal 1: creates orchestrator.toml and data/ beside it on first start.
tkfs -f orchestrator.toml start

# Terminal 2, as the same user.
tkfs -f orchestrator.toml manage create Projects --headless
tkfs -f orchestrator.toml manage status

# Pair two hosts: host A prints a code, host B pairs with it and receives a folder.
tkfs -f orchestrator.toml manage reset-pairing-code                     # host A
echo "$CODE" | tkfs -f orchestrator.toml manage pair ws://hosta:43180   # host B
tkfs -f orchestrator.toml manage remote-list ws://hosta:43180            # host B
tkfs -f orchestrator.toml manage receive ws://hosta:43180 REPO_ID Projects   # host B

tkfs -f orchestrator.toml manage shutdown
```

The default listener is `127.0.0.1:43180`, reachable from the same computer
only: before host B can pair, host A must set `[network] listen =
"0.0.0.0:43180"` (or a specific interface) in its `orchestrator.toml` and
restart (see [Linux deployment](docs/LINUX.md#quick-start)).

See the [command reference](docs/CLI.md), [configuration](docs/CONFIGURATION.md)
and [Linux deployment](docs/LINUX.md) (signals, systemd, Docker).

A host with no listener (`[network] listen = ""`) can still pair: it dials
the other host and tells it no address, so that host keeps the pairing
without an endpoint and never dials back. Objects move in batches the
receiving side shrinks on a slow link (see
[object transfer](docs/CONFIGURATION.md#object-transfer)); a single file
larger than about 8 GiB cannot be replicated in this release.

## Error classes and what to do

A failed command prints `CODE: message` on stderr and exits with status 1.
The code tells what to do:

| Code | Meaning | What to do |
|---|---|---|
| `CLOCK_INVALID` | The system clock reads before 2020-01-01 UTC (or a peer's does). The orchestrator, workers and `manage pair` refuse to run. | Fix the clock (NTP), then start again. |
| `CATALOG_INCOMPATIBLE` | The data directory holds a catalog this build cannot read (another schema version, or a `folders.sqlite` of an earlier release). Nothing is migrated or written. | Move the data directory aside and start fresh (`reset-pairing-code`, `pair`, `receive`). |
| `CONFIG_VERSION_2_REQUIRED`, `INVALID_CONFIG` | `orchestrator.toml` is not a format 2 file of this release, or has unknown fields. It is never overwritten. | Fix the file, or remove it to get a fresh one. |
| `LISTEN_FAILED` | The `[network] listen` address cannot be bound. | Free the port or change `listen`. |
| `ALREADY_RUNNING` | Another orchestrator (or worker) holds this data directory (exit status 75). | Use that one, or stop it first. |
| `ORCHESTRATOR_NOT_RUNNING`, `WS_CONNECT_FAILED` | No orchestrator answers for this configuration's data directory. | Start it (`tkfs -f FILE start`); run `manage` as its user. |
| `ACCESS_DENIED` | The orchestrator's or worker's token cannot be read or does not match. | Run as the user the orchestrator runs as. |
| `CONNECTION_LOST` | The connection dropped after a change was sent; it is never resent. | Check `manage status` to see whether it happened. |
| `ORCHESTRATOR_STOPPING` | The orchestrator is shutting down. | Wait for it to exit and start it again. |
| `FORCED_STOP` | Logged: a worker did not stop in time at shutdown and was forced or killed. Committed data is durable. | Nothing; look at `worker.log` if it repeats. |
| `STRANDED_PROCESS` | A worker could not be shown gone or killed within its bound. | Find the process (`worker.json` names its pid) and end it. |
| `WORKER_LIMIT` | `maximum_running` folders already have a worker. | Stop one, or raise `maximum_running` and restart. |
| `FOLDER_REQUIRED` | A folder command has neither `--state` nor `--runtime`. | Give `--state DIR` or `--runtime STATE/runtime.json`. |
| `FOLDER_NOT_FOUND`, `AMBIGUOUS_FOLDER` | The folder argument matches no folder, or more than one. | Use more of the id, or the exact label (`manage list`). |
| `FOLDER_RUNNING`, `FOLDER_NOT_RUNNING` | The command needs the folder stopped (`configure-mount`), or running. | `manage stop` / `manage start` it first. |
| `MOUNT_UNSUPPORTED` | `create`, `receive` or `configure-mount` was given `--mount`: orchestrator-managed folders run headless in this release. Nothing is created or changed. | Create the folder headless (`--headless`, or no `--mount`). |
| `FOLDER_INIT_FAILED` | A new folder's store could not be created. Nothing of the folder is kept. | Fix the cause it names (disk space, permissions) and run `create` / `receive` again. |
| `WORKER_NOT_RUNNING` | The folder's worker is not running (`rpc`). | `manage start` the folder. |
| `BUSY_VIEW` | The worker has files open and refuses to stop. | Close them, or `manage force-stop`. |
| `CRASH_LOOP` | The folder's worker failed 5 times within 10 minutes and is not restarted. | Read `worker.log`, fix the cause, `manage retry`. |
| `OP_NOT_ALLOWED` | A peer or control request names an operation that lane or endpoint does not serve. | Use `manage` or the folder commands. |
| `REPO_NOT_PUBLISHED` | The paired host publishes no running, unpaused folder of that repository. | `manage remote-list ENDPOINT`; start or resume the folder there. |
| `REPO_ALREADY_PRESENT` | This orchestrator already has a folder of that repository. | Use that folder. |
| `PAIRING_CODE_CHANGED`, `INVALID_PAIRING_CODE`, `PEER_REVOKED` | The peer's code was reset, the code is wrong, or the peer revoked this host. | Get the current code (`reset-pairing-code` there) and `manage pair` again. |
| `PEER_LANES_SKIPPED` | Logged: the connections to a paired peer could not be set up (for example its credential is missing). | Restart the orchestrator; if it repeats, `manage unpair` and `manage pair` again. |
| `CLOCK_OFFSET_LARGE` | Logged: a peer's clock differs from this one by more than 5 s. | Fix the clocks (NTP). |
| `SYNC_FETCH_FAILED` | Logged: fetching objects from a peer failed or timed out. On a timeout the folder asks that peer for smaller batches and grows them back as replies arrive. | Nothing if it clears. |
| `SYNC_OBJECT_TOO_LARGE` | Logged: a peer holds an object too large for any reply, the content list (manifest) of a file larger than about 8 GiB. It is not replicated in this release. | Keep such files out of synced folders for now. |
| `DESKTOP_REQUIRES_WINDOWS` | `gui` (or `tkfs` with no command) on a non-Windows host. | Use `manage`. |

[Architecture](docs/ARCHITECTURE.md), [configuration](docs/CONFIGURATION.md),
[validation](docs/VALIDATION.md), and [authority scope](docs/revamp/AUTHORITY_SCOPE.md).

GPL-3.0-or-later. See the repository license and Slint notices.
