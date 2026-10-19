# Linux

The daemon, the workers and the CLI run on Linux; the desktop is Windows
only (on Linux, follow the status with `tkfs manage watch`).

## Building and runtime dependencies

```sh
sudo apt install build-essential pkg-config libssl-dev   # Debian/Ubuntu build
cargo build --release --locked
```

Outbound `wss://` uses OpenSSL and the distribution's CA bundle
(`ca-certificates`). Mounts need FUSE 3 (`fuse3`, `fusermount3`) and
`/dev/fuse`; headless folders need neither. TKFS does not install
certificates or change trust stores. It creates its token files
(`orchestrator.token`, each state's `runtime.token`) and the catalog
(`folders.sqlite`, with its `-wal`/`-shm` files) with mode `0600`; the rest of
the data directory follows your umask, so keep it `0700` if other users share
the machine (see
[local access](CONFIGURATION.md#pairing-credentials-and-local-access)).

```sh
tkfs init -f ./orchestrator.toml
tkfs -f ./orchestrator.toml orchestrator
# In another terminal:
tkfs -f ./orchestrator.toml manage create Project            # FUSE mount at mounts/Project
tkfs -f ./orchestrator.toml manage create Archive --headless
tkfs -f ./orchestrator.toml manage watch
```

## Mounts

- A mount path must not exist when the folder mounts: the worker creates the
  directory and removes it after a clean unmount. An empty directory left at
  the path (a killed container, a crash) makes the mount fail with
  `MOUNT_PATH_MUST_NOT_EXIST`; the folder keeps syncing headless. Check the
  directory is empty and not a mount (`mountpoint PATH`), remove it with
  `rmdir PATH`, then `tkfs manage attach STATE`.
- A stale mount of this folder's own previous worker (type `fuse.tkfs`, no
  longer answering, proven by the previous `worker.json`) is unmounted and
  removed automatically before the next attach. Any other mount at the path is
  never touched.
- FUSE's kernel permission checks and the mount helper's policy apply as the
  system configures them. Portable permission bits and timestamps are
  filesystem metadata; symlinks, hard links, devices, ownership and privileged
  mode bits are not implemented. Names are case-insensitive, as on Windows.

## Signals and services

The daemon handles SIGINT and SIGTERM like `manage shutdown`: it closes its
peer connections, stops every folder (graceful, up to 30 s; a folder with
files open or a worker that does not answer is left running), and exits 0.
A worker started by hand (`tkfs daemon`) also stops on SIGINT/SIGTERM; with
files open it refuses (`BUSY_VIEW`, logged): send the signal again once they
are closed.

Management commands read the daemon's token (`DATA/orchestrator.token`,
owner-only), so run them as the daemon's user (`sudo -u USER tkfs …`).

For a systemd service, adapt `deploy/tkfs-orchestrator.service.example`
(`ExecStop` runs as the service's user, so it can read the token):

```ini
ExecStart=/usr/local/bin/tkfs -f /etc/tkfs/orchestrator.toml orchestrator
ExecStop=/usr/local/bin/tkfs -f /etc/tkfs/orchestrator.toml manage shutdown
TimeoutStopSec=60
```

Workers are separate processes that may outlive the daemon, and a restarted
daemon adopts the ones still running (see the
[adoption runbook](../README.md#adoption-after-a-daemon-crash)). systemd's
default `KillMode=control-group` ends every process of the unit when the
service stops or its main process dies, workers included: they are then
restarted with store recovery instead of adopted. `ExecStop` gives them a
cooperative stop first, and committed data is durable, but a worker that is
killed while files are open in its mount can lose changes not saved yet. If
you want workers, and the files open in their mounts, to survive a daemon
crash or a service restart, set `KillMode=process` (only the daemon is
signalled; `mixed` still kills the rest of the group at the end of a stop)
and keep `ExecStop`, which stops every quiet worker cooperatively.

## Containers

`examples/docker-compose.yml` runs the daemon as the container's main process
(`exec ./tkfs start`) with the binary, configuration and data in the
bind-mounted directory, host networking, `/dev/fuse` and `SYS_ADMIN` for
mounts. In a container every worker ends with the daemon, so there is no
adoption: a restarted container restarts its folders.

- Stop or restart it with time for the daemon's own shutdown:
  `docker compose stop -t 60` (Docker's default is 10 s, then SIGKILL).
- Run management commands inside it, as the daemon's user (they read its
  token): `docker compose exec tkfs-daemon ./tkfs manage status`.
- Mounts live in the container's mount namespace; to see the files on the
  host, use a headless folder plus another tool, or configure mount
  propagation yourself.
- Upgrading from the previous release: see the
  [README](../README.md#step-by-step-a-desktop-and-a-server-behind-a-tls-proxy).

## Tests on Linux

```sh
CARGO_TARGET_DIR=$HOME/tkfs-target/linux cargo test --locked --test orchestrator --test session_core --test worker_lifecycle --test mount_lifecycle
bash scripts/qualify.sh --wave 3
```

FUSE tests skip when `/dev/fuse` is missing. [Validation](VALIDATION.md)
describes the qualification runs.
