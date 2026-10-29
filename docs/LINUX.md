# Linux

The orchestrator, the workers and the CLI run on Linux; the desktop is
Windows only (on Linux, follow the status with `tkfs manage watch`). Folders
run headless: FUSE mounts of orchestrator-managed folders arrive with a later
plan, so containers and services need neither `/dev/fuse` nor `fuse3` yet.

## Building and runtime dependencies

```sh
sudo apt install build-essential pkg-config libssl-dev   # Debian/Ubuntu build
cargo build --release --locked
```

Outbound `wss://` uses OpenSSL and the distribution's CA bundle
(`ca-certificates`). TKFS does not install certificates or change trust
stores. It creates its token files (`orchestrator.token`, each state's
`runtime.token`) and the catalog (`folders.sqlite`, with its `-wal`/`-shm`
files) with mode `0600`; the rest of the data directory follows your umask,
so keep it `0700` if other users share the machine (see
[local access](CONFIGURATION.md#pairing-credentials-and-local-access)).

## Quick start

```sh
tkfs -f ./orchestrator.toml start
# In another terminal, as the same user:
tkfs -f ./orchestrator.toml manage create Project --headless
tkfs -f ./orchestrator.toml manage create Archive --headless
tkfs -f ./orchestrator.toml manage watch
```

`start` creates `orchestrator.toml` (with the defaults of
[CONFIGURATION](CONFIGURATION.md)) when it is missing and `data/` beside it.
To sync with another host, set `[network] listen` (and `advertise` behind a
proxy), then pair: `manage reset-pairing-code` on one host prints a code,
`manage pair ENDPOINT` on the other reads it from stdin, and `manage
receive` creates a folder from what the first host publishes
([CLI](CLI.md#pairing-and-remote-folders)).

## Signals and shutdown

The orchestrator handles SIGINT and SIGTERM exactly like `manage shutdown`:
it closes its peer connections and stops every folder in parallel within a
30 s bound. A worker that has not stopped by about 15 s (for example one with
files open) is force-stopped, and one still alive at about 27 s is killed;
each forced folder is logged as `FORCED_STOP`. Committed data is durable and
unsealed scratch files are re-sealed at the next start. The orchestrator then
exits 0. A worker started by hand (`tkfs daemon`) also stops on SIGINT and
SIGTERM; with files open it refuses (`BUSY_VIEW`, logged), so send the signal
again once they are closed.

Management commands read the orchestrator's token
(`DATA/orchestrator.token`, owner-only), so run them as the orchestrator's
user (`sudo -u USER tkfs ...`).

## Adoption after a crash

Workers are separate processes. If the orchestrator dies (a crash, `kill -9`,
a service restart that leaves the group alone), its workers keep running.
The next orchestrator on the same data directory reads each folder's
`worker.json` and **adopts** a worker only when the process matches: same pid,
same start time and same executable image. A worker that matches is
reused without restarting it; when nothing matches (a stale file, a recycled
pid, a different binary) the folder starts a fresh worker. The
supervisor of a folder that fails repeatedly backs off (up to 5 minutes)
and, after 5 failures within 10 minutes, stops restarting it
(`CRASH_LOOP`; `manage retry` clears it). `manage status` shows the phase of
each folder.

## systemd

Adapt `deploy/tkfs-orchestrator.service.example` (`ExecStop` runs as the
service's user, so it can read the token):

```ini
ExecStart=/usr/local/bin/tkfs -f /etc/tkfs/orchestrator.toml orchestrator
ExecStop=/usr/local/bin/tkfs -f /etc/tkfs/orchestrator.toml manage shutdown
TimeoutStopSec=60
```

systemd's default `KillMode=control-group` ends every process of the unit
when the service stops or its main process dies, workers included: they are
then restarted with store recovery instead of adopted. `ExecStop` gives them
a cooperative stop first, and committed data is durable, but a worker that is
killed while files are open can lose changes not saved yet. If you want
workers to survive an orchestrator crash or a service restart and be adopted,
set `KillMode=process` (only the orchestrator is signalled; `mixed` still
kills the rest of the group at the end of a stop) and keep `ExecStop`, which
stops every quiet worker cooperatively.

## Containers

`examples/docker-compose.yml` runs the orchestrator as the container's main
process (`exec ./tkfs -f orchestrator.toml start`), with the binary,
configuration and data in the bind-mounted directory and host networking.
Folders run headless; the mounting block (`/dev/fuse`, `SYS_ADMIN`,
AppArmor) is commented out and unused until mounts return.

- `exec` makes the orchestrator the container's PID 1, so `docker compose
  stop` sends it SIGTERM and it shuts down by itself (the bounded 30 s sequence above,
  with `FORCED_STOP` logged for any folder it had to force). The file asks
  Docker to wait 60 s (`stop_grace_period`) before SIGKILL; there is no shell
  trap.
- In a container every worker ends with the orchestrator, so there is no
  adoption: a restarted container restarts its folders.
- Add and watch folders from inside the container, as the orchestrator's user
  (they read its token):

```sh
docker compose exec tkfs-daemon ./tkfs manage create Project --headless
docker compose exec tkfs-daemon ./tkfs manage status
```

- The peer listener defaults to `127.0.0.1:43180`. Set `[network] listen` to
  `0.0.0.0:43180` in `orchestrator.toml` (and `advertise` to the address
  peers should use, `wss://` behind your TLS proxy) to accept remote peers;
  the container uses host networking.
- Upgrading from the previous release: nothing is migrated. Start with a new
  data directory and pair again (`reset-pairing-code`, `pair`, `receive`); a
  catalog this build cannot read fails with `CATALOG_INCOMPATIBLE`.

## Tests on Linux

```sh
CARGO_TARGET_DIR=$HOME/tkfs-target/linux cargo test --locked
```

[Validation](VALIDATION.md) describes the qualification runs.
