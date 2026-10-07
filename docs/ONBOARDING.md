# Local onboarding and managed projects

Windows uses WinFsp; Linux uses FUSE. Both platforms share configuration, catalog,
owner-local management IPC and worker supervision. Linux remains headless; the
desktop application is Windows-only.

## First project

Choose a writable application folder. On Linux, use a native Linux filesystem,
including inside Ubuntu WSL2; keep managed state outside `/mnt/c`. Have the TKFS
binary on PATH (or use its full path). From the chosen folder:

```text
tkfs init
tkfs start
```

`start` runs the supervisor in the foreground and keeps that terminal occupied.
Open a second terminal in the same folder:

```text
tkfs create MyProject
```

The new project's shared `main` branch is mounted at `Projects/MyProject`.
Open and edit files there normally. Windows requires installed WinFsp. Linux
requires accessible `/dev/fuse` and installed `fusermount3`.

Bare `init` creates `orchestrator.toml`, `.tkfs/data` and `Projects`. Defaults are
format version 1, a pinned installation UUID, an owner-local control endpoint,
networking disabled, restart policy `on-failure`, and at most eight running workers.
Paths in the default config are relative to its folder. Rerunning `init` validates
and preserves existing configuration and identity. It refuses malformed configs,
nonempty unregistered data, identity changes and missing registered catalogs.
Config publication is atomic and never replaces an existing config.

Keep the adjacent `orchestrator.tkfs-bootstrap.json` pin and config lock file.
The pin records the effective data root and whether a catalog has been registered.
Copying configuration does not migrate an installation; restore or migrate the
complete installation deliberately rather than deleting its safety pin.
CLI project receipts live in the private data root and contain requests, not
pairing credentials.

`create` saves its operation UUID and expected catalog generation before sending.
Repeat the same command after a lost response or process crash to replay that
request and recover the same project. A definitive terminal stale-generation
receipt permits a generation refresh. After correcting an obstruction, rerunning
can replace a terminal rejected request only when its receipt proves no state
intent was committed. Busy or uncertain operations keep the original request.
Repeating a completed create returns its original result; use managed
start to restart a project that was subsequently stopped.

## Selecting a configuration

`init`, `start`, `create`, `orchestrator` and `manage` accept `-f` / `--defaults-file`.
The CLI otherwise uses `orchestrator.toml` in the current directory:

```text
tkfs start -f /path/to/orchestrator.toml
tkfs create MyProject -f /path/to/orchestrator.toml
```

On Windows, `tkfs gui -f C:\path\orchestrator.toml` opens that installation. Without
`-f`, the desktop still uses `orchestrator.toml` next to the executable. Desktop and
CLI use the same bootstrap implementation. Before dispatching management actions,
clients verify the installation UUID and canonical data root in the owner's Hello.

## Inspection and shutdown

```text
tkfs manage list
tkfs manage inspect STATE_UUID
tkfs --generation CATALOG_GENERATION --request-id OPERATION_UUID manage shutdown
```

Use the catalog generation from `list` for create/shutdown; use the state's
management generation from `inspect` for start/stop. Raw `manage` retains explicit
concurrency and receipt controls. Save its printed operation UUID and generation
when retrying. Shutdown preflights all workers; close open files, directory iterators
and shells inside mounts before retrying a busy shutdown.

Restarting the foreground supervisor reattaches surviving workers only after an
authenticated instance/identity handshake. A dead managed Linux FUSE worker can be
replaced only after the state lock is free. Recovery detaches only an unresponsive
mount at the recorded path with that worker's exact FUSE source ID; it refuses
unrelated or responding mounts and never uses forced or lazy unmounts. Linux uses
private sockets, SO_PEERCRED on both ends, checked flock inodes and pidfds rather
than stored-PID signalling. Runtime discovery records the actual socket address,
including short private endpoints for deeply nested native state paths.

## Standalone and networking workflows

`tkfs init --state PATH [--repo UUID]` and `tkfs daemon --state PATH [--mount PATH]`
remain available. Standalone and managed workers contend for the same state lock.
Linux rejects unsafe existing state directories or ownership-lock aliases instead
of changing their permissions. Branch and checkout commands still use `-C MOUNT`
or `--runtime STATE/runtime.json`; raw managed create without a mount stays headless.

This onboarding does not start pairing or install a service. See [PAIRING.md](PAIRING.md)
for the separate TLS/WSS/loopback-WS data service. No systemd unit, Windows service,
firewall rule, credential or network configuration is installed.

## Local verification

```text
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked --test orchestrator
cargo test --locked --test onboarding_process -- --include-ignored
```

The process suite uses disposable installations and foreground processes. Ignored
cases require WinFsp or working Linux FUSE and exercise actual writes, rename/delete,
private branches, busy retries, worker reattachment, concurrent creates, publication
and delivery crashes, and Linux disconnected-mount recovery. These checks qualify
local operation; service installation, multi-user deployment, power-loss durability
and WAN behavior remain outside this qualification.
