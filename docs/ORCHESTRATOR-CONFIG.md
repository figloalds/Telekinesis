# Orchestrator TOML anatomy

`orchestrator.toml` configures the Windows O1 local supervisor and its desktop
client. The accepted schema is defined by `Config`, `Control`, `Network` and
`Workers` in `src/orchestrator.rs`; desktop bootstrap is in `src/desktop.rs`.
It does not configure standalone `daemon` PSK networking or the separate
`pairing.toml` service.

## Find and load the file

`tkfs orchestrator` and `tkfs manage ...` default to `orchestrator.toml` in the
calling shell's current directory. `-f <PATH>`/`--defaults-file <PATH>` selects
another file. There is no parent search, global fallback or automatic CLI config
creation. The GUI always uses `orchestrator.toml` beside its actual executable;
its first-run flow can create that file.

The file must exist and parse before the supervisor/client can proceed. Relative
`data_directory` and `default_mount_directory` values resolve against the
configuration file's directory, independently of the process cwd.

## Minimal valid configuration

Save this as `orchestrator.toml` at the repository root for a disposable local
setup. Use native local storage and a fresh, otherwise empty data directory.

```toml
format_version = 1
data_directory = 'test-runs\managed-data'

[control]
transport = 'named-pipe'
name = 'telekinesis-local'
```

Omitting `[network]` disables networking. Omitting `[workers]` selects
`restart_policy = 'on-failure'` and `maximum_running = 8`.

```powershell
tkfs orchestrator -f ./orchestrator.toml
# In another shell:
tkfs manage -f ./orchestrator.toml hello
tkfs manage -f ./orchestrator.toml list
```

## Expanded example

```toml
format_version = 1
data_directory = 'test-runs\managed-data'
default_mount_directory = 'test-runs\Projects'
# Optional: installation_id = '<existing-or-new-installation-UUID>'

[control]
transport = 'named-pipe'
name = 'telekinesis-local'

[network]
enabled = false
# Optional: listen = '127.0.0.1:0'  # validated only; opens no listener

[workers]
restart_policy = 'on-failure'
maximum_running = 8
```

TOML single-quoted strings are literal: backslashes in Windows paths do not need
escaping. In double-quoted TOML strings, escape backslashes or use forward slashes.
The example UUID comment is a placeholder; do not enable it without a real UUID.

## Root fields

| Field | Type | Required/default | Meaning/validation |
|---|---|---|---|
| `format_version` | Integer | Required | Only `1` is accepted; other values fail with `UNSUPPORTED_CONFIG_VERSION`. |
| `data_directory` | Path string | Required | Nonempty native data root. Relative paths resolve beside the config; stores/catalog are kept here. Empty values fail with `INVALID_DATA_DIRECTORY`. |
| `installation_id` | UUID string | Optional | Fresh catalogs use this ID, or generate one when omitted. When supplied for an existing catalog it must match that catalog's identity. It is not a credential; do not change it to relabel an existing installation. |
| `default_mount_directory` | Path string | Optional | Parent folder used by the desktop's new-project flow. Relative values resolve beside the config. It does not mount existing states or supply `manage create`'s `--mount` argument automatically. |
| `control` | Table | Required | Owner-local Windows management endpoint. |
| `network` | Table | Optional; disabled | Reserved O1 network fields described below. |
| `workers` | Table | Optional; defaults below | Worker restart/running policy. |

The loader rejects unknown fields at the root and within all three tables.
Do not add service, pairing, peer-key or arbitrary project entries to this file.

## `[control]`

Both fields are required whenever the config is loaded.

| Field | Type | Accepted value |
|---|---|---|
| `transport` | String | Exactly `'named-pipe'`; otherwise `UNSUPPORTED_CONTROL_TRANSPORT`. |
| `name` | String | 1..100 ASCII letters, digits, hyphens or underscores; otherwise `INVALID_PIPE_NAME`. Supply the name, not a full pipe path. |

The same Windows user/SID must own and manage the catalog. Choose a distinct pipe
name for each concurrently running installation. The GUI creates a unique name
during bootstrap. A pipe name or installation UUID is not peer authentication.

## `[network]`

| Field | Type | Default/accepted value |
|---|---|---|
| `enabled` | Boolean | Defaults to `false`, including when the table is present. `true` fails with `O1_NETWORK_DISABLED`. |
| `listen` | Socket-address string | Optional; if supplied it must parse as numeric IPv4/IPv6 plus port, otherwise `INVALID_LISTEN_ADDRESS`. It does not enable or bind a listener in O1. |

Use the separate [pairing configuration](PAIRING.md) and [WSS guide](PAIRING-WSS.md)
for published-data networking. Putting their fields here is rejected.

## `[workers]`

| Field | Type | Accepted value/default when table omitted |
|---|---|---|
| `restart_policy` | String | Exactly `'on-failure'`. |
| `maximum_running` | Integer | 1..64 inclusive; default `8`. Limits concurrently running workers, including headless ones. |

If `[workers]` is included, **both fields must be provided**; individual fields
have no omitted-value defaults. Unsupported policy or out-of-range limits fail
with `INVALID_WORKER_POLICY`. Worker desired states belong in the registry, not
this table. The supervisor retries failures with bounded backoff and honors the
running limit; permanently invalid/unverified states remain unavailable.

## Data ownership, startup and edits

One supervisor owns each data root. Initial startup accepts a proven fresh root
or interrupted bootstrap; later startup requires the established registry.
Missing, empty, truncated or unrecognized catalogs are refused rather than
silently initialized. Existing standalone stores are not automatically adopted.
The registry includes catalog identity, state IDs, mount reservations, desired
running states and retry receipts. It creates `states/`, `staging/` and `logs/`
below the data root. Keep mounts and application files separate from that root.

The GUI's first-run flow requires a new/empty data root and a separate mount
parent, then writes absolute paths, an installation UUID and unique pipe name.
It pins the connected catalog in `.tkfs-ui-identity.json` beside the executable.
Legacy configs without `installation_id` remain accepted; without
`default_mount_directory`, creating a project in the GUI needs an explicit path.

Configuration is loaded at startup/client connection; there is no live reload
API. A running supervisor keeps its loaded configuration until restarted.
Changing the data root or installation identity is not a migration mechanism.
Use the matching original config/catalog for reconnect and restore; do not point
a pinned GUI at an empty replacement. See [management commands](ORCHESTRATOR-CLI.md)
for cooperative stop/shutdown and retry handling.
