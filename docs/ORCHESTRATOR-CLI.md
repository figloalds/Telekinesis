# Orchestrator management commands

This is the Windows/Linux owner-local management CLI. See
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
The GUI defaults beside the executable and also offers explicit configuration
selection; neither changes shell profiles or a running daemon's configuration.
The supervisor uses same-owner named pipes on Windows and Unix sockets on Linux.

## Actions and arguments

```text
tkfs orchestrator [-f <CONFIG_PATH>]
tkfs [--request-id <UUID>] [--generation <N>] manage [-f <CONFIG_PATH>] <ACTION>
```

| Action | Arguments | Expected generation |
|---|---|---|
| `hello` | None; installation/API/build identity and readiness handshake | Not required |
| `reset-pairing-code` | Print exactly 200 decimal digits once; rotate future code enrollment only | Forbidden; no request ID or replay |
| `list` | None; catalog generation and states | Not required |
| `inspect <STATE_ID>` | Stable managed state ID; state/worker observation | Not required |
| `operation <OPERATION_UUID>` | Inspect an existing request's pending/completed receipt | Not required |
| `create <LABEL> [--mount <ABSOLUTE_PATH>]` | New managed state and worker; no mount argument means headless | Current `catalog_generation` from `list` |
| `prepare-replica <LABEL> --repo <UUID>` | New headless replica of an existing logical repo; returns fresh state/device IDs | Current catalog generation |
| `attach-mount <STATE_ID> --mount <ABSOLUTE_PATH>` | Reserve first mount for a stopped headless state; separate Start activates it | Current state generation |
| `background-sync-status` | Desired binding, actual service state and all-enabled-grants scope | Not required |
| `configure-background-sync --config <PATH>` | Bind preprovisioned outbound WSS config and enable supervision | Current catalog generation |
| `configure-background-sync --disable` | Remove desired binding and cancel/drain the service | Current catalog generation |
| `start <STATE_ID>` | Set desired-running and reconcile/start the worker | State's current `management_generation` from `list`/`inspect` |
| `stop <STATE_ID>` | Set desired-stopped; busy contexts can leave the request pending | State's current `management_generation` |
| `shutdown` | Cooperative supervisor/worker shutdown, preserving desired-running states | Current `catalog_generation` |

Mutating actions require `--generation`; missing it fails with
`MANAGEMENT_GENERATION_REQUIRED`. Labels must be nonempty after trimming and at
most 256 bytes. State IDs differ from repository UUIDs, replica/device UUIDs and
branch IDs. Use the returned `state_id` for start/stop/inspect.

Prepare journals allocated IDs before filesystem creation; exact retries preserve
them and duplicate managed repo UUIDs are refused. Attach holds the stopped state
lock through catalog commit. The desktop additionally supplies selected shared
branch/generation proof, revalidated with causal history and content objects under
that lock. CLI attachment is an explicit owner reservation without that wizard
proof. Neither attach nor prepare grants network authority.

Background configuration records only immutable public binding metadata. Missing
or changed configuration/credentials fail closed; disable still works when the
config file is missing. The service survives GUI closure and supervisor restart.
An uncertain drain retains registry ownership and leaves Shutdown pending so
workers cannot restart; it requires explicit process recovery, not blind retry.

For a mounted create, the parent must exist and the target must be absolute,
unoccupied and outside the native data root. Overlapping managed mount targets
are refused. `default_mount_directory` is a GUI convenience; omitting `--mount`
from this CLI still creates a headless worker.

## Pairing code reset

With the selected supervisor running, execute
`tkfs manage -f ./orchestrator.toml reset-pairing-code`. Transfer the printed code
privately. The command emits only the code once; it never writes a replay receipt.
Do not redirect it to a persistent log. If delivery fails, explicitly reset again.
`--request-id` and `--generation` are rejected for this special action.

The protected sibling `orchestrator.toml.pairing/verifier.sqlite` stores a salted
verifier bound to the actual initialized installation and config/data paths.
It does not store the code. A receiving pairing service must explicitly select
this config through `orchestrator_config` in its own TOML. Reset does not start a
listener, grant data access, revoke existing peers, stop sync, or invalidate
already submitted candidates awaiting local approval. Legacy v1 invitations are
independent and remain subject to their original expiry/one-use policy.
See [desktop evolution](DESKTOP-EVOLUTION.md) for the migration/security boundary.

## A normal mutation and its exact retry

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
