# TKFS command and argument reference

This reference describes the CLI defined in `src/main.rs`. Commands use the same
`tkfs` executable as the Windows desktop, supervisor and managed workers. Use
`tkfs.exe` on Windows or `tkfs` on Linux; examples assume it is on PATH. From a
checkout, substitute `./target/debug/tkfs.exe` or `./target/debug/tkfs`.

```text
tkfs [GLOBAL OPTIONS] <COMMAND> [COMMAND OPTIONS]
tkfs --help
tkfs <command> --help
tkfs --version
```

On Windows, no arguments opens the desktop; `tkfs gui` does the same. Linux
requires a command and has no desktop. `orchestrator` and `manage` are
Windows-only. `stop` is available only in Linux builds.

## Global arguments and project selection

| Argument | Meaning and default |
|---|---|
| `-C <DIRECTORY>` | Resolve a mounted project from this directory; defaults to the caller's cwd. This selects discovery, not the shell's working directory. |
| `--runtime <PATH>` | Read a runtime discovery file directly, usually `<state>/runtime.json`; takes precedence over `-C`/cwd. |
| `--request-id <UUID>` | Reuse the identity of a semantic mutation when retrying; otherwise a fresh ID is generated. Keep the complete payload, including generation, identical. |
| `--generation <N>` | Optional runtime view-generation check; required for mutating `manage` actions. The two generation domains differ, as described below. |
| `-h`, `--help` | Display help for the current command. |
| `-V`, `--version` | Display the executable version at the top level. |

These global arguments can appear before or after a subcommand. Runtime commands
discover `.tkfs-runtime.json` by checking the selected directory and its ancestors.
Without a discovery file they fail with `PROJECT_NOT_FOUND`. The synthetic file is
readable by exact path but omitted from mounted directory listings.

`init`, `daemon`, `gui`, `orchestrator`, `manage` and `pairing` use their own
state/configuration arguments rather than mounted-project discovery. Global
runtime/view arguments do not configure their state directories or transports.

## Initialize and run a standalone state

| Command | Arguments and behavior |
|---|---|
| `init --state <PATH> [--repo <REPOSITORY_UUID>]` | Initialize native local state under an exclusive owner lock. Omit `--repo` for a new repository; use an existing repository UUID for a separate replica with its own device identity. |
| `daemon --state <PATH> [--mount <PATH>]` | Run an already initialized state in the foreground. Without `--mount`, run headless with local control. |
| `stop` | Linux: stop the selected standalone daemon after a quiet-view check and unmount if mounted. Windows managed workers use `manage stop` instead. |

The state directory must be outside the mounted view. A mount's parent must exist
and its target must not already exist. Keep state on native local storage. A
second daemon cannot own the same state simultaneously.

The standalone daemon also accepts a legacy PSK replication group:

| Argument | Requirement |
|---|---|
| `--listen <IP:PORT>` | Local TCP listener; requires both peer arguments below. |
| `--peer <IP:PORT>` | Remote endpoint; requires `--listen`. |
| `--peer-device <DEVICE_UUID>` | Remote replica's exact device UUID; requires `--listen` and must differ from the local device. |
| `TKFS_PEER_KEY` environment variable | Required when using that group: a 32-byte secret encoded as 64 hexadecimal characters. It is not persisted by the daemon. |

Supply all three network arguments together. This is separate from the
persistent [pairing service](PAIRING.md) and [WSS transport](PAIRING-WSS.md), which
use approved device certificates and repository grants.

## Inspect the selected runtime

Prefix these commands with `--runtime <state>/runtime.json` or `-C <mount>`.

| Command | Output/purpose |
|---|---|
| `status` | Branch, view generation, durability/backlog, handles, health and last known catchup. During serialized work it may return a sampled observation with `busy=true` and `stale=true`. |
| `branches` | Branch descriptors and stable IDs; private/shared names may overlap. |
| `state` | Canonical projection of the selected branch. |
| `events` | Local semantic journal. |
| `conflicts` | Retained competing alternatives and review state. |
| `history` | Local checkpoint IDs, branches, snapshot objects and messages. |
| `cat-object <SHA256>` | Raw immutable-object bytes; buffered object reading is limited to 16 MiB. |
| `sync` | Explicit exchange using the standalone daemon's configured PSK peer; fails with `PEER_NOT_CONFIGURED` when absent. For the separate paired service, use `pairing sync`. |

Successful inspection/mutation output is JSON, except raw bytes from `cat-object`.
Failures are printed as JSON to stderr and exit with code 1. Catchup is the last
observed exchange, not a distributed barrier.

## Branches, checkpoints and reviewed conflicts

| Command syntax | Behavior |
|---|---|
| `branch <NAME>` | Create a new private branch from the current visible state; does not select it. |
| `checkout <NAME_OR_ID>` | Select a branch, using an unambiguous name or stable ID. Mounted checkout remounts at the same path after fencing and busy checks. |
| `publish <NAME> --current-state-only` | Required explicit flag; create a new shared branch from visible state without private ancestry, hidden entries or conflict alternatives. Does not select it. |
| `checkpoint -m <MESSAGE>` | `--message` is the long alias; save a coherent local snapshot of the selected branch. |
| `restore <CHECKPOINT_ID> --branch <FRESH_NAME>` | Copy the snapshot into a new private branch; does not select it or overwrite the current branch. |
| `resolve --conflict <ID>... --revision <REVISION_ID>` | Resolve exactly the reviewed conflict IDs using the chosen revision; the parser requires `--revision`, and the runtime also requires at least one conflict ID. |
| `recover <ENTRY_ID> <FREE_LOGICAL_PATH> --conflict <ID>...` | Relocate a reviewed displaced entry while preserving its identity/bytes; requires one or more exact conflict IDs, a visible parent and a free target. Deleted entries need existence resolution first. |

Review alternatives before resolving. Reading the canonical winner does not
resolve a conflict. Checkpoints/history remain local. Publication becomes a
network transfer only when a transport and corresponding grants are configured.

## File/namespace administration and export

| Command syntax | Arguments |
|---|---|
| `import <LOGICAL_PATH> <NATIVE_SOURCE_FILE>` | Read a native source file and publish its bytes at the logical path; bounded buffered import, not a streaming bulk-copy command. |
| `mkdir <LOGICAL_PATH>` | Create a directory in the selected branch. |
| `rename <LOGICAL_PATH> <TARGET_LOGICAL_PATH> [--replace]` | Move/rename; `--replace` explicitly allows replacement under runtime constraints. |
| `delete <LOGICAL_PATH>` | Delete the selected entry through the runtime. |
| `bucket-export --directory <NATIVE_PATH>` | Export verified shared objects and a manifest to a native test directory outside state and mounts. No cloud upload is performed. |

Logical paths name entries inside the selected branch; native paths name files or
directories on the host. Runtime mutations can refuse `BUSY_VIEW` while file or
directory contexts, cwd references, watchers, mappings or dirty staging remain.
Run checkout/control from a shell outside the mount. Durable flushed edits stay
with their original branch; checkout does not discard them.

For runtime controls, `--generation` refers to the selected state's view
generation, exposed by `status`. An identical completed mutation retries from its
stored receipt; changing payload under the same request ID is refused. For
management, use catalog/state management generations instead.

The standalone `sync`, `bucket-export` and Linux `stop` dispatch paths do not
apply the optional view-generation check. A sampled busy `status` observation
also bypasses that check; inspect `stale` before using it as a current view.

## Supervisor, desktop and pairing modes

| Mode | Configuration/reference |
|---|---|
| `gui` | Windows desktop; `orchestrator.toml` beside the actual executable. No config-path argument. |
| `orchestrator [-f <PATH>]` | Windows foreground supervisor; defaults to cwd `orchestrator.toml`. See [TOML anatomy](ORCHESTRATOR-CONFIG.md). |
| `manage [-f <PATH>] <ACTION>` | Windows owner-local management client; same config default. See [management commands](ORCHESTRATOR-CLI.md). |
| `pairing [-f <PATH>] <ACTION>` | Portable published-data service/management; defaults to cwd `pairing.toml`. See [pairing configuration and enrollment](PAIRING.md). |

`-f` is the alias for `--defaults-file` on the last three modes. It is not a
global flag or a way to select GUI configuration.

Pairing actions are listed here for argument lookup; follow the operating guide
for credential provisioning and enrollment order:

| Pairing action | Arguments |
|---|---|
| `new-id` | Print a fresh public installation UUID; does not need an existing config. |
| `credential-init` | Windows: generate/store a CurrentUser DPAPI-protected identity. |
| `credential-provision --encrypted-output <PATH>` | Linux: provision through `systemd-creds`; requires a systemd credential config. |
| `run`, `status` | Run the separate service or inspect credential/registry state. |
| `invite [--lifetime-seconds <N>]` | Default lifetime is 300 seconds; prints a secret invitation for private transfer. |
| `join` | Read the secret invitation from masked or piped stdin. |
| `approve <INVITATION_ID> --installation <UUID> --fingerprint <VALUE>` | Approve the exact pending identity. |
| `revoke <PEER_INSTALLATION_ID>` | Persist peer revocation. |
| `grant <PEER_INSTALLATION_ID> --repo <UUID> --runtime <PATH> --remote-replica <UUID> [--disable]` | Configure or disable a published repository sync; `--runtime` here identifies the granted local worker. |
| `remote-list <PEER_INSTALLATION_ID>` | List authorized published repositories on the peer. |
| `sync <PEER_INSTALLATION_ID> --repo <UUID>` | Exchange a configured repository grant. |

The hidden `managed-worker` mode is supervisor-internal. The hidden Windows
`gui-test --report <PATH>` mode is acceptance tooling; use the repository desktop
harness rather than treating it as a normal desktop startup option.
