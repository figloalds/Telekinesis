# Command reference

Global options: `-f/--defaults-file CONFIG` (default `orchestrator.toml`),
`-C DIRECTORY`, `--runtime STATE/runtime.json`, `--request-id UUID`, and
`--generation NUMBER`. The last two apply to filesystem operations; use a
stable request ID when retrying a worker mutation after an uncertain response.
Management lifecycle actions do not require a catalog generation.

| Command | Effect |
|---|---|
| `init [-f CONFIG]` | Write a new version 2 config. |
| `init --state DIR [--repo REPO]` | Initialize standalone filesystem state. |
| `orchestrator` / `start` | Run the daemon in the foreground. |
| `gui` | Windows desktop; initializes a missing config and starts its daemon. |
| `create LABEL [--mount PATH] [--headless]` | Create a managed filesystem and start its worker. |
| `daemon --state DIR [--mount PATH]` | Run a standalone local worker. |
| `manage list` | Folder catalog, running state and errors. |
| `manage inspect STATE_ID` | Inspect one managed folder. |
| `manage create LABEL [--mount PATH] [--headless]` | Same as top-level create. |
| `manage stop STATE_ID` | Cooperatively stop and clear desired-running. |
| `manage configure-mount STATE_ID [--mount PATH]` | Change a stopped folder's mount; omit path for headless. |
| `manage start STATE_ID` | Persist desired-running and start a worker. |
| `manage shutdown` | Stop workers and daemon, preserving desired-running. |
| `manage reset-pairing-code` | Return a new receiving code and persist its salted hash. |
| `manage pair ENDPOINT [--connections N]` | Read the remote code from stdin; persist and start the connection pool. |
| `manage peers` | Pool/lane status, without stored pairing codes. |
| `manage unpair ENDPOINT` | Remove a local outbound pool; does not revoke a remote holder of your code. |
| `manage remote-list ENDPOINT` | List paired server's running shared repositories. |
| `manage receive ENDPOINT REPO LABEL [--mount PATH] [--headless]` | Create a same-repository local replica and begin sync. |

`ENDPOINT` accepts `ws://HOST:PORT` or `wss://HOST[:PORT][/PATH]`, including query
strings. WS defaults to port 80; WSS defaults to 443 and uses platform certificate
and hostname verification. Use the same endpoint string for pairing, listing,
receiving and unpairing. Both schemes use the remote pairing code; there is no
certificate enrollment or approval step.

`create`, `manage receive`, and `manage start` initiate asynchronous startup.
Their response can have `starting: true` and `running: false`. Poll
`manage inspect STATE_ID` until `running: true` before using the mount or
`runtime.json`; `startup_phase` identifies the current recovery phase. Startup
allows 30 seconds without progress, renewed by work on the recovery thread.
`manage stop` also cancels a folder that is still starting.

`manage peers` separates lane connection errors from folder sync errors. Each
peer reports `connections` (effective total), `configured_connections`, and
`reserved_connections` (one). Lane 0 has `role: "control"`; other lanes have
`role: "bulk"`. `--connections 1` creates two connections to preserve the reserve.
Both endpoints must support reserved lanes; upgrade the remote daemon if pairing
reports `RESERVED_LANES_UNSUPPORTED`.

Each lane's `repositories` map is keyed by repository ID and reports `caught_up`,
`last_success_ms`, `error`, `consecutive_failures`, and `retry_at_ms` (Unix
milliseconds). A connected lane can have a failing folder while other folders
continue syncing. Failed folders retry with exponential delays from 1 to 30
seconds; a successful exchange clears the error and retry state. These status
fields describe the last exchange, not a distributed synchronization barrier.

Remote discovery omits workers whose summaries cannot be read. Their local
`manage inspect` / `manage list` results expose `publication_error`, which clears
after a successful discovery pass.

Filesystem commands work with `-C MOUNT` or `--runtime STATE/runtime.json`:
`status`, `branches`, `conflicts`, `state`, `events`, `history`, `branch NAME`,
`checkout NAME`, `publish NAME --current-state-only`, `checkpoint -m MESSAGE`,
`restore CHECKPOINT --branch NAME`, `resolve --conflict ID... --revision ID`,
`recover ENTITY TARGET --conflict ID...`, `import PATH SOURCE`, `mkdir PATH`,
`rename PATH TARGET [--replace]`, `delete PATH`, `cat-object HASH`,
`bucket-export --directory DIR`, and `stop`.

Use `manage stop` for managed workers so the supervisor does not restart them.
Direct worker `stop` is intended for standalone workers. Network sync is automatic
in the daemon; direct worker peer keys/listeners and the old `pairing` service
command are removed. The runtime API still refuses checkout/stop when the mount
has open handles or unflushed writes.

## Partial content availability

Worker `status` distinguishes `metadata_caught_up`, `content_complete`, per-peer
observations and `content` counters. `content.items` contains up to 20 current
paths with missing bytes, their expected sizes, durable partial lengths and the
latest transfer error. `caught_up` retains metadata-roundtrip semantics and does
not certify a remote content backup. Peer observations expire after 15 seconds.

Reads prioritize missing content over background objects and may report content
unavailable after a bounded wait. The folder and its metadata remain accessible.
All connected peers must support protocol 6 for independent metadata/content sync.
