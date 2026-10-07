# Orchestrator management commands

This is the Windows owner-local management CLI. See
[orchestrator TOML anatomy](ORCHESTRATOR-CONFIG.md) for every configuration field
and [the main command reference](CLI.md) for standalone filesystem controls.

In the directory containing `orchestrator.toml`, use:

```powershell
tkfs orchestrator
tkfs manage hello
tkfs manage list
```

`orchestrator` and `manage` default `--defaults-file` to `./orchestrator.toml` in
the shell's current working directory. Relative data/mount paths inside that file
remain relative to its own directory. There is no parent-directory search,
global fallback, daemon auto-selection or automatic configuration creation.
Missing/invalid files fail before a daemon is contacted or launched.

An explicit path wins; `-f` is the short alias:

```powershell
tkfs orchestrator -f "other directory/orchestrator.toml"
tkfs manage --defaults-file "other directory/orchestrator.toml" list
```

Existing commands with explicit `--defaults-file` keep working. Local mutations
still need their expected `--generation` and support exact `--request-id` retry.
The GUI keeps resolving its configuration beside the executable; this convenience
does not change GUI startup, shell profiles or running daemon configuration.
The O1 supervisor remains Windows-only until its separate Linux portability work.

## Actions and arguments

```text
tkfs orchestrator [-f <CONFIG_PATH>]
tkfs [--request-id <UUID>] [--generation <N>] manage [-f <CONFIG_PATH>] <ACTION>
```

| Action | Arguments | Expected generation |
|---|---|---|
| `hello` | None; installation/API/build identity and readiness handshake | Not required |
| `list` | None; catalog generation and states | Not required |
| `inspect <STATE_ID>` | Stable managed state ID; state/worker observation | Not required |
| `operation <OPERATION_UUID>` | Inspect an existing request's pending/completed receipt | Not required |
| `create <LABEL> [--mount <ABSOLUTE_PATH>]` | New managed state and worker; no mount argument means headless | Current `catalog_generation` from `list` |
| `start <STATE_ID>` | Set desired-running and reconcile/start the worker | State's current `management_generation` from `list`/`inspect` |
| `stop <STATE_ID>` | Set desired-stopped; busy contexts can leave the request pending | State's current `management_generation` |
| `shutdown` | Cooperative supervisor/worker shutdown, preserving desired-running states | Current `catalog_generation` |

Mutating actions require `--generation`; missing it fails with
`MANAGEMENT_GENERATION_REQUIRED`. Labels must be nonempty after trimming and at
most 256 bytes. State IDs differ from repository UUIDs, replica/device UUIDs and
branch IDs. Use the returned `state_id` for start/stop/inspect.

For a mounted create, the parent must exist and the target must be absolute,
unoccupied and outside the native data root. Overlapping managed mount targets
are refused. `default_mount_directory` is a GUI convenience; omitting `--mount`
from this CLI still creates a headless worker.

## A mutation and its exact retry

In PowerShell, with the supervisor running separately:

```powershell
$catalog = tkfs manage -f ./orchestrator.toml list | ConvertFrom-Json
$operation = [guid]::NewGuid().ToString()
tkfs --request-id $operation --generation $catalog.catalog_generation manage -f ./orchestrator.toml create Project
tkfs manage -f ./orchestrator.toml operation $operation
# If delivery was uncertain or the operation is pending, retry exactly:
tkfs --request-id $operation --generation $catalog.catalog_generation manage -f ./orchestrator.toml create Project
```

Without an explicit request ID, the CLI generates one and prints it with the
expected generation to stderr before sending. A timeout means delivery is
uncertain. Keep the same UUID, arguments and original expected generation on
retry, even if the catalog/state has advanced. Completed receipts replay before
current-generation checks; changed payload under the same UUID is refused.
A terminal stale-generation failure requires a refresh and a new request ID.

Shutdown/stop can refuse busy files, directory handles, cwd references or memory
mappings. Release those contexts, inspect the operation and retry the same
request. Closing a management CLI does not stop anything. Closing the desktop
leaves workers/supervisor running; `shutdown` ends them cooperatively, while
`stop <STATE_ID>` changes only that state's desired-running setting.
