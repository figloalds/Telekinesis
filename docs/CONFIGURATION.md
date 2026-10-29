# Configuration

## The configuration file

`orchestrator.toml` configures the orchestrator. `tkfs init -f PATH` writes a
new file with the defaults and never overwrites one; `tkfs -f PATH
orchestrator` (or `start`) creates a missing file the same way and refuses an
invalid one without touching it. Relative paths in the file resolve against
the file's directory. The orchestrator reads it at start: restart it after
editing. Folders, their mount paths, pairings and intents are not in this
file: they are changed with `tkfs manage …` and persist in the catalog at
once.

The file's path is `-f PATH` when given, else the `TKFS_CONFIG` environment
variable when set and not empty, else `orchestrator.toml` next to `tkfs.exe`
on Windows and in the current directory elsewhere. A relative `-f` or
`TKFS_CONFIG` resolves against the current directory. The desktop app and
every command use the same resolution.

```toml
format_version = 2
data_directory = "data"
default_mount_directory = "mounts"
maximum_running = 8

[network]
listen = "127.0.0.1:43180"
advertise = ""
connections = 4
worker_lanes = 2
sync_interval_ms = 500

[freshness]
open_wait_ms = 250
```

| Field | Meaning |
|---|---|
| `format_version` | Must be `2` (`CONFIG_VERSION_2_REQUIRED` otherwise). Unknown fields are refused (`INVALID_CONFIG`). The Windows desktop reads and writes this same file (a configuration written by an older desktop build is refused with `DESKTOP_CONFIG_UNSUPPORTED`): see [CLI](CLI.md#running-the-orchestrator). |
| `data_directory` | The catalog (`folders.sqlite`) and every folder's state directory. |
| `default_mount_directory` | Where a folder mounts by default (`<this>/<label>`). The desktop builds its default mount points from it (`INVALID_FOLDER_NAME` when a label cannot be one directory name); the worker creates this directory when it mounts the first folder under it. Headless folders ignore it. |
| `maximum_running` | Folders that may have a worker at once, 1-64 (default 8). Beyond it, starting a folder fails with `WORKER_LIMIT`; `INVALID_WORKER_LIMIT` for a value outside the range. At start the orchestrator starts or adopts at most this many folders; the others log `WORKER_LIMIT` and stay stopped until one stops. |
| `network.listen` | The peer listener, plain WebSocket. Default `127.0.0.1:43180` (this computer only, for a TLS proxy on the same host). `"0.0.0.0:43180"` listens on every IPv4 interface; `""` disables it (the orchestrator only dials). A port that cannot be bound fails the start with `LISTEN_FAILED`. |
| `network.advertise` | The URL a peer should use to reach this orchestrator, for example the `wss://` address of your TLS proxy. `""` (default) derives `ws://` plus the listen address. With neither a listener nor an advertise address the host is dial-only: `manage pair` still works and tells the peer no address, so the peer keeps the pairing without an endpoint and never dials back. |
| `network.connections` | The default pool per peer, 1-16 (default 4): lane 0 carries metadata, the other lanes file content, so the pool is at least 2. `manage pair --connections N` overrides it per peer (`INVALID_CONNECTION_COUNT` outside 1-16). |
| `network.worker_lanes` | Loopback connections from the orchestrator to each worker, 1-8 (default 2): one control lane plus the rest for object transfer (`INVALID_WORKER_LANES`). |
| `network.sync_interval_ms` | How often each folder exchanges metadata with each peer, 50-60,000 ms (default 500) (`INVALID_SYNC_INTERVAL`). The measured clock offset of each peer is refreshed every 60 intervals. |
| `freshness.open_wait_ms` | How long opening a file waits for a newer remote version, 0-5,000 ms (default 250; `INVALID_OPEN_WAIT`). Validated and stored for the mounts plan; nothing opens files through the orchestrator yet. |

## Data directory and catalog

One orchestrator runs per data directory. Its catalog (`folders.sqlite`)
holds the folders and their intents, the pairing verifier, the credentials of
paired peers and each peer's measured clock offset. A data directory whose
catalog has a schema this build does not know fails with
`CATALOG_INCOMPATIBLE`, and the message names the directory: move it aside
(or restore a backup) and start again. Stores and catalogs of earlier
releases are not read or migrated: a `folders.sqlite` of an earlier release
(tables but no schema version) is refused the same way and never stamped as
this release's.

## Object transfer

File content moves between orchestrators as objects (64 KiB chunks and, for
larger files, a manifest listing their chunks) over the bulk lanes; there is
nothing to configure. The receiving side asks for manifests in batches of at
most 4 and for chunks in batches of at most 32. Each request may take
10 s plus 1 s per 256 KiB asked (a chunk counts 64 KiB, a manifest 1 MiB),
at most 120 s. A request that times out halves the batch that folder asks of
that peer (down to one object; `SYNC_BATCH_SHRUNK` in the log); each answered
request grows it back by a quarter. The sending side never fails a request for
its size: it answers with what fits (8 MiB of object bytes, and a reply body
within the 16 MiB session bound) and the rest is asked again at once.

Limit of this release: an object whose encoding alone exceeds the reply bound
is never sent (`SYNC_OBJECT_TOO_LARGE` on the receiving side). Object bytes
travel as JSON number arrays (about 3.6 bytes per byte), so this is a
manifest larger than about 4 MiB, that is a single file larger than about
8 GiB: such a file cannot be replicated until binary bodies arrive.

## Environment variables

Set them in the orchestrator's environment; workers inherit it. All are
optional.

| Variable | Read by | Meaning |
|---|---|---|
| `TKFS_UNSUPERVISED_EXIT_S` | worker | A worker started with `--supervised` stops itself after this many seconds without contact from an orchestrator; default 600. |
| `TKFS_LOG_ROTATE_BYTES`, `TKFS_LOG_ROTATE_FILES` | orchestrator | The size at which `DATA/logs/orchestrator.log` is rotated (bytes, at least 256; default 10 MiB) and how many files are kept (1-100, default 5). |
| `TKFS_FAULT_POINT`, `TKFS_FAULT_ACTION`, `TKFS_FAULT_SCOPE`, `TKFS_FAULT_MS`, `TKFS_FAULT_COUNT` | both | Fault injection for the qualification scenarios (`hang`, `fail`, `exit`, `panic` at a named point). Never set them in production. |

## Files

```text
orchestrator.toml
data/                            data_directory
  folders.sqlite                 catalog: folders, intents, peers, pairing
                                 verifier, per-peer credentials, clock offsets
                                 (owner-only; with -wal/-shm files)
  orchestrator.lock              one orchestrator per data directory
  orchestrator.json              the running orchestrator: control and listen
                                 addresses, advertise, installation, pid,
                                 version, started_ms, data_directory
  orchestrator.token             the running orchestrator's control token
                                 (owner-only)
  logs/orchestrator.log          the orchestrator's log (JSON lines; rotated to .1 ...)
  <folder id>/                   one folder (its state directory)
    metadata.sqlite (-wal, -shm) journal, projection and indexes
    objects/                     content packs and their index
    scratch/                     write buffers of open files
    owner.lock                   one worker per state
    runtime.json                 the running worker's loopback address
    runtime.token                the running worker's token (owner-only)
    worker.json                  the running worker's identity (pid, start time,
                                 executable, version), used for adoption
    worker.log                   the worker's startup lines and errors
                                 (rotated at start past 10 MiB: worker.log.1 ...)
```

`orchestrator.json` and `orchestrator.token` exist only while an
orchestrator runs: it removes them when it exits and writes new ones when it
starts. `runtime.json`, `runtime.token` and `worker.json` exist only while a
worker runs; a crashed worker leaves them, and the next orchestrator checks
`worker.json` (pid, start time and executable image must all match) before
adopting or replacing the worker. `orchestrator.toml` lives beside the data
directory, not inside it.

## Logs

- **Orchestrator**: `DATA/logs/orchestrator.log`, one JSON object per line,
  `{"at_ms":...,"level":...,"code":...,...}`, at most 20 lines per code and
  minute (then one `suppressed` count). It is rotated at 10 MiB and 5 files
  are kept (`orchestrator.log`, then `.1` to `.4`, the oldest dropped);
  `TKFS_LOG_ROTATE_BYTES` and `TKFS_LOG_ROTATE_FILES` change that. When run
  from a terminal or a service manager it also prints its events on stderr.
  The desktop starts its orchestrator with stdout and stderr appended to this
  same file: the orchestrator captures them into it as `stdout`/`stderr`
  events, so the file is rotated like any other and each event is written once.
  Notable events: `ORCHESTRATOR_STARTED`, `ORCHESTRATOR_STOPPING`,
  `FORCED_STOP` (a worker had to be forced or killed at shutdown),
  `PEER_LANES_SKIPPED` (the connections to a paired peer could not be set up), `CLOCK_OFFSET_LARGE` (a peer's clock differs by
  more than 5 s) and `ORCHESTRATOR_STOPPED`.
- **Workers**: `DATA/<folder id>/worker.log`: the startup lines, the Ready
  line (its token redacted) and the worker's log lines, appended while it
  runs. It is rotated when a worker starts and the file is over 10 MiB (five
  files). The last lines are kept as a folder's error when the worker exits
  without a fatal line.
- No log line carries a pairing code, a per-peer secret or a token: fields
  with such names are written `<redacted>`.

## Clocks

All stored and transmitted timestamps are UTC milliseconds. A system clock
that reads before 2020-01-01T00:00:00Z is refused with `CLOCK_INVALID` by the
orchestrator (exit status 78), by every worker, by `manage pair` and on every
peer connection: fix the clock (NTP) and start again. Each control lane
measures the clock offset and round trip to its peer (NTP-style, smoothed)
and keeps it in the catalog; `manage peers` shows it as `clock`. Versions are
ordered by their causal parents, never by clock, so the offset only corrects
how a peer's times are read. An offset above 5 s is logged as
`CLOCK_OFFSET_LARGE`.

## Pairing credentials and local access

- A host's **pairing code** is 32 random bytes (64 hex characters), printed
  once by `manage reset-pairing-code`. The host stores a salted SHA-256
  verifier of it; the verifier is as sensitive as the code (it is the
  authentication key).
- **Per-peer credentials.** `manage pair` authenticates once with the code;
  over that authenticated session the dialing host then enrolls a fresh
  random 32-byte secret with the accepting host (`peer.enroll`), and both
  keep the key derived from it in `folders.sqlite`. Later sessions
  authenticate with that key (scheme `peer-hmac-v1`), so each pairing has its
  own credential that can be revoked alone. A missing or revoked credential
  is refused with `PEER_REVOKED`.
- The pairing code never crosses the network: sessions authenticate with an
  HMAC-SHA256 challenge in both directions, so WS and WSS carry the same
  proof; WSS adds transport encryption.
- **Revocation.** `manage unpair` first asks the other host to revoke this
  host's credential (best effort: an unreachable host does not block the
  unpair); that host closes this installation's sessions within a second.
  Rotating the code (`reset-pairing-code`) closes every session that used
  the old code and revokes the credentials of the hosts that dial this one;
  they must pair again with the new code.
- **Local access.** The orchestrator's control endpoint and the workers'
  endpoints listen on loopback only, and each requires a token at the
  WebSocket upgrade (header `X-Tkfs-Token`); a connection without the right
  one is refused before any request is read (`ACCESS_DENIED`):
  - the orchestrator's token is `DATA/orchestrator.token`; `tkfs manage ...`
    reads it;
  - each worker has its own token in `STATE/runtime.token`; the orchestrator
    gets it from the worker's Ready line and the folder commands (`tkfs
    --state ...`, `tkfs --runtime ...`) read the file.
- **Owner-only files.** The token files and `folders.sqlite` are created
  owner-only: mode `0600` on Linux; on Windows they inherit the ACL of the
  data directory, which in your user profile allows only you (plus SYSTEM
  and Administrators). Whoever can read them can manage the orchestrator, act
  as this host towards its peers, or dial its peers: keep the data directory
  in your profile on Windows, and `0700` on Linux if other users share the
  machine. Run `tkfs manage ...` as the user the orchestrator runs as.

## Remote access through a TLS proxy

The peer listener is plain WebSocket. For access over the internet, keep it
on loopback and put a TLS reverse proxy in front of it; dialers then use
`wss://host[:port][/path]`, and the proxy's public URL goes in
`network.advertise` so a peer pairing with this host learns the right
address. The dialer verifies the certificate and host name with the platform
trust store (a private CA must be trusted by that platform); there is no
certificate pinning or bypass.

The proxy must:

- forward WebSocket upgrades (HTTP/1.1 `Upgrade` and `Connection` headers);
- pass the `X-Tkfs-Sync` header through in both directions (the request and
  the `101` answer);
- not time out an idle connection in less than about 20 s (sessions send a
  heartbeat after 5 s of silence; 60-120 s is a comfortable read timeout);
- allow large WebSocket messages (the transport accepts up to 64 MiB per message).

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
path-based route (`wss://host/tkfs`) works.

## Backups

Stop the orchestrator (`manage shutdown`; check that no worker is left
running), then copy the whole data directory. Restore it at the same path:
the catalog records the absolute paths of the folders' state directories and
mounts. The copy holds the pairing verifier and the per-peer credentials
(`folders.sqlite`): protect it like the original. The token files need no
backup; each orchestrator and worker writes a new one when it starts.
