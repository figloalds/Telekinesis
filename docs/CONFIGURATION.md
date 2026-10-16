# Configuration

## The configuration file

`tkfs init -f PATH` writes a new file with the defaults (it never overwrites
one). Relative paths in it resolve against the file's directory. The daemon
reads it at start: restart the daemon after editing it. Folder mounts,
pairings and intents are not in this file: they are changed with
`tkfs manage …` and persist in the catalog at once.

```toml
format_version = 2
data_directory = "data"
default_mount_directory = "mounts"
maximum_running = 8

[network]
listen = "127.0.0.1:43180"
connections = 4
sync_interval_ms = 500
```

| Field | Meaning |
|---|---|
| `format_version` | Must be `2` (`CONFIG_VERSION_2_REQUIRED` otherwise). Unknown fields are rejected (`INVALID_CONFIG`). The previous release used the same format: an existing file keeps working. |
| `data_directory` | The catalog (`folders.sqlite`) and every folder's state directory. |
| `default_mount_directory` | Where `create` and `receive` mount a folder when no `--mount` is given (`<this>/<label>`). |
| `maximum_running` | Folders that may have a worker at once, 1–64 (default 8). Beyond it, starting a folder fails with `WORKER_LIMIT`. A running worker found at startup is always adopted, even beyond the limit. |
| `network.listen` | The peer listener, plain WebSocket. Default `127.0.0.1:43180` (this computer only, for a TLS proxy on the same host). `"0.0.0.0:43180"` listens on every IPv4 interface; `""` disables it (the daemon only dials). |
| `network.connections` | The default pool per peer, 1–16 (default 4): lane 0 carries metadata, the other lanes file content, so the pool is at least 2. `manage pair --connections N` overrides it per peer. More lanes help many files move at once; one file always moves on one lane. |
| `network.sync_interval_ms` | The idle **anti-entropy** interval, 50–60,000 ms (default 500): a folder that has not exchanged metadata with a peer for this long does so again. Changes do not wait for it: a local change, a remote change (its catalog tip) or newly staged events start an exchange at once, and content moves continuously while there is any. Connections stay up whatever the value (heartbeats every 5 s of silence), so a long interval costs nothing but a slower safety net. The stall threshold is `max(60 s, 20 × sync_interval_ms)`. |

## Environment variables

Set them in the daemon's environment; workers inherit it. All are optional.

| Variable | Read by | Meaning |
|---|---|---|
| `TKFS_STALL_AFTER_MS` | daemon | Overrides the stall threshold (milliseconds, positive). |
| `TKFS_UNSUPERVISED_EXIT_S` | worker | A worker started by a daemon stops itself (when quiet) after this many seconds without contact from any daemon; default 600, `0` disables it. |
| `TKFS_REMOTE_FASTPATH` | worker | `0` restricts the incremental commit of creations and namespace changes to this replica's own events, and disables it on a branch with any conflict row, as the previous release did (a kill switch; slower). Any other value, or unset: the fast paths serve remote events too. |
| `TKFS_CONFLICT_MODE` | worker | How commits derive conflicts: `append` (default) derives only what the new event adds when that is provably exact, else in full; `full` always derives in full (the reference, slower); `verify` runs both and fails the commit with `CONFLICT_MODE_MISMATCH` on any difference (for testing). An unknown value selects `full`. |
| `TKFS_FAULT_POINT`, `TKFS_FAULT_ACTION`, `TKFS_FAULT_SCOPE`, `TKFS_FAULT_MS`, `TKFS_FAULT_COUNT` | both | Fault injection for the qualification scenarios (`hang`, `fail`, `exit`, `panic` at a named point). Never set them in production. |
| `TKFS_FAULT` | worker | The older fault hook of the test suites (exit 86 at a named point). Never set it in production. |
| `TKFS_GUI_RENDER`, `TKFS_GUI_RENDER_SNAPSHOT`, `TKFS_GUI_RENDER_VIEW`, `TKFS_GUI_RENDER_SIZE` | desktop | Render one desktop frame to a BMP file without a window (from a saved status snapshot or the running daemon), for documentation and checks; see [validation](VALIDATION.md#desktop-manual-checklist). |
| `TKFS_BUILD_ID` | compile time | The build id in the version string (`0.1.0+<id>`); without it, a digest of the executable's size and time. A daemon replaces adopted workers whose version differs. |

## Files

```text
orchestrator.toml
data/                            data_directory
  folders.sqlite                 catalog: folders, intents, gates, peers, pairing verifier
  orchestrator.lock              one daemon per data directory
  orchestrator.json              the running daemon: control and network addresses, pid, version
  logs/orchestrator.log          output of a daemon the desktop started
  states/<state id>/             one folder
    metadata.sqlite (-wal, -shm) journal, projection, inbox, feed and object indexes
    objects/                     verified content objects (SHA-256 names)
    incoming-objects/            partial downloads (spools)
    staging/                     write buffers of open files; kept for saves still pending
    owner.lock                   one worker per state
    runtime.json                 the running worker's loopback address
    worker.json                  the running worker's identity (pid, start time, executable, version)
    worker.log                   the worker's startup lines and errors
mounts/<label>/                  default mount points
```

`runtime.json` and `worker.json` exist only while a worker runs (a crashed
worker leaves them; the next daemon checks them before adopting or replacing
it). `orchestrator.json` exists only while a daemon runs.

## Logs

- **Daemon**: JSON lines on stderr, `{"at_ms":…,"level":…,"code":…,…}`, at
  most 20 lines per code and minute (then one `suppressed` count). A daemon
  started by the desktop appends its output to `DATA/logs/orchestrator.log`;
  a daemon you start yourself logs where its stderr goes (redirect it, or let
  the service manager keep it).
- **Workers**: `DATA/states/<state>/worker.log`, appended at every start: the
  startup lines, the Ready line, and the worker's JSON log lines. The last
  lines of it are shown as a folder's `last exit` when no fatal line explains
  an exit.
- In this build neither file is rotated: watch their size on long-running
  hosts.
- `manage status` and the desktop show the status and the recent activity
  (500 entries) without reading logs.

## Pairing credentials and local access

- A host's **pairing code** is 32 random bytes (64 hex characters), printed
  once by `manage reset-pairing-code`. The host stores a salted SHA-256
  verifier of it; the verifier is as sensitive as the code (it is the
  authentication key).
- A **dialing** host stores the remote host's code in its catalog
  (`folders.sqlite`), so it can reconnect.
- The code never crosses the network: sessions authenticate with an
  HMAC-SHA256 challenge in both directions, so WS and WSS carry the same
  proof; WSS adds transport encryption.
- Rotating the code (`reset-pairing-code`) closes every session that used the
  old one within a second; dialers must pair again.
- **Local control**: the daemon's management endpoint and the workers'
  endpoints listen on loopback only. In this build they require no token:
  any process of a local user can manage the daemon. Protect the data
  directory and the configuration with the operating system's permissions
  (Linux: owner-only `0700`/`0600` if other users share the machine; Windows:
  keep them in your user profile). TKFS never changes permissions itself.

## Remote access through a TLS proxy

The daemon's listener is plain WebSocket. For access over the internet, keep
it on loopback and put a TLS reverse proxy in front of it; dialers then use
`wss://host[:port][/path]`. The dialer verifies the certificate and host name
with the platform trust store (a private CA must be trusted by that
platform); there is no certificate pinning or bypass.

The proxy must:

- forward WebSocket upgrades (HTTP/1.1 `Upgrade` and `Connection` headers);
- pass the `X-Tkfs-Sync` header through in both directions (the request and
  the `101` answer). A proxy that drops it makes an upgraded server look like
  the previous release: the dialer reports `PEER_NEEDS_UPGRADE`;
- not time out an idle connection in less than about 20 s (sessions send a
  heartbeat after 5 s of silence; 60–120 s is a comfortable read timeout);
- allow messages up to 1 MiB plus a small header (one frame; larger bodies
  are split into frames of at most 1 MiB).

nginx forwards custom headers both ways by default. An example in front of
the default listener:

```nginx
location / {
    proxy_pass http://127.0.0.1:43180;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_read_timeout 120s;
    proxy_send_timeout 120s;
}
```

Paths and query strings of the endpoint are kept in the upgrade request, so a
path-based route (`wss://host/tkfs`) works. One pairing is enough for both
directions: the server does not need to dial the desktop.

## Backups

Stop the daemon (`manage shutdown`; check that no worker is left running),
then copy the whole data directory. Restore it at the same path: the catalog
records the absolute paths of the folders' state directories and mounts, and
no command changes a state directory's path (`manage configure-mount`
changes a stopped folder's mount path).
