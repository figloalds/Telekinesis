# Telekinesis

Mount folders, manage them with an orchestrator, and sync shared work through
persistent WebSocket connections between orchestrators.

## Architecture

```text
mounted folder <-> worker -- orchestrator <== WS pool ==> orchestrator -- worker <-> mounted folder
```

A worker owns one filesystem state and optionally a WinFsp (Windows) or FUSE
(Linux) mount. It handles durable saves, branches, checkpoints and conflicts.
Workers expose local loopback WebSocket control; they never connect to remote
workers. The orchestrator starts/stops workers, configures mount paths, persists
its folder catalog, and proxies replication between matching repositories.

Networking runs inside the orchestrator daemon. WebSocket is the only transport,
with four persistent connections per configured peer by default. Lane 0 is
reserved for discovery, events, and replication state; file content uses the
other three lanes. Repositories are distributed across the bulk lanes, while
the reserved lane visits all repositories. Requests carry local changes and
responses carry remote changes, so an outbound-only orchestrator can both send
and receive. Multiple peers and relayed replica origins are supported.

Pairing uses one randomly generated code. The receiving orchestrator stores a
salted SHA-256 verifier in its catalog, not the code. A dialing orchestrator
retains the supplied remote code in its ordinary catalog so it can reconnect.
There are no OS credential providers, device certificates, approval ceremonies,
repository grants, application ACL checks or permission repair. The process
uses whatever filesystem access the OS already gives it.

TKFS accepts outbound `ws://` and `wss://` connections in the desktop and CLI.
Secure connections verify the server certificate and hostname using the platform
trust store. The daemon listener remains plain WS; expose it through an
operator-managed TLS reverse proxy for remote `wss://` access. Listener
certificates, access rules and service accounts are operator configuration.
TKFS does not generate or install certificates, enroll devices, or change the
system trust store. WSS uses the same pairing-code exchange as WS.

## Build

```sh
cargo build --locked
cargo test --locked
python scripts/orchestrator_e2e.py --exe target/debug/tkfs.exe
```

The CLI harness above uses the Windows executable; select the Linux executable
on Linux. Windows needs the installed WinFsp SDK/runtime. Linux builds need the
OpenSSL development libraries and pkg-config; see the [Linux guide](docs/LINUX.md).
Linux mounts need `fusermount3`
and `/dev/fuse`; headless workers need neither. `scripts/build.py` builds release
runnables for Windows and Linux, using WSL for Linux when launched on Windows.
The Windows desktop opens with `tkfs gui` or no arguments. To assemble a portable
package, run `python scripts/package_desktop.py --exe target/debug/tkfs.exe`.

The desktop dashboard shows searchable virtual folders, mount status, connected
orchestrators and local host status. Use **New folder** to create and mount a
folder, **Change path** to stop it and edit its mount point, and **Connect remote**
to pair, browse and receive remote folders. **Host settings** contains the local
pairing code and daemon shutdown controls. Counts refresh every five seconds;
headless workers are not counted as mounted folders. **Sync / conflicts** opens a
folder panel with per-peer metadata observations, partial content availability,
paged conflict alternatives, bounded text previews and explicit resolution or
recovery at a new path. The panel refreshes status every five seconds without
replacing an ongoing conflict review. Stale reviews are rejected and can be
refreshed. Storage usage is marked as
unavailable because the management API does not report it. Folder deletion and
quotas from the visual prototype are not implemented by the daemon.

## Mount and manage a folder

```sh
tkfs init -f orchestrator.toml
tkfs -f orchestrator.toml orchestrator
```

In another terminal:

```sh
tkfs -f orchestrator.toml create MyProject
tkfs -f orchestrator.toml manage list
```

`create` starts a worker and mounts it under the configured mount parent. Its
JSON result contains the state ID, repository ID, backing directory and mount
path. Use `--mount PATH` for a particular mount path or `--headless` for a worker
without a mount. Copy existing project files into the mount to import them; TKFS
does not watch an existing native folder.

```sh
tkfs -f orchestrator.toml manage stop STATE_ID
tkfs -f orchestrator.toml manage configure-mount STATE_ID --mount NEW_PATH
tkfs -f orchestrator.toml manage start STATE_ID
tkfs -f orchestrator.toml manage shutdown
```

Closing the desktop leaves the daemon running. Daemon shutdown stops workers
cooperatively and retains their desired-running state for the next start. Stopping
one folder changes its desired state. Open handles or pending writes can prevent
an orderly stop. Failed worker starts remain visible in `manage list` with their
log path; the daemon retries desired-running workers.

Folder startup is asynchronous. Create, receive and start return with
`starting: true`; poll `manage inspect STATE_ID` for `running: true` before
accessing the mount or worker API. The desktop displays **Starting** and allows
cancellation. `startup_phase` reports the latest recovery work.

Remounts restore a checksummed local snapshot and replay events after its durable
cursor. Snapshots are saved transactionally during operation, so crash recovery
does not depend on a clean dismount. Older states need one full reconstruction;
incompatible or damaged snapshots and replay requiring historical conflict analysis
fall back to reconstruction automatically. Content verification and pending-save
recovery still run.

Workers have 30 seconds to report startup progress. Real recovery work renews
this allowance, so large journal replays can finish without a fixed total time
limit. If progress stops for 30 seconds, the daemon terminates the worker and
reports the last phase in `WORKER_START_TIMEOUT`. Other folders and management
requests remain available during recovery.

## Connect two orchestrators

1. On A, configure `[network].listen` to the desired interface and start the daemon.
2. On A, run `tkfs -f A.toml manage reset-pairing-code` and copy the returned code.
3. On B, run `tkfs -f B.toml manage pair ws://A_HOST:43180`, then paste the code.
4. On B, list A's folders and receive one:

```sh
tkfs -f B.toml manage remote-list ws://A_HOST:43180
tkfs -f B.toml manage receive ws://A_HOST:43180 REPO_ID MyProject
tkfs -f B.toml manage peers
```

The daemon creates a new local replica, mounts it, and syncs it automatically.
Use `--headless` on `receive` when no mount is needed. Existing matching
repositories sync automatically; B needs no inbound listener and A needs no
outbound pairing entry. Only running workers appear in the remote folder list.
A paired orchestrator can discover all running folders and sync shared branches.
Private branches and checkpoint history remain local.

`manage pair ... --connections 6` selects a total pool size, including one
reserved connection. A setting of 1 is promoted to 2 so file transfers never
borrow the reserved lane. Both daemons must support lane negotiation; older
servers report `RESERVED_LANES_UNSUPPORTED` and need upgrading.
Pairing again updates the
saved code/pool. `manage unpair ENDPOINT` removes the local outbound connection.
`manage reset-pairing-code` replaces the receiving verifier and invalidates
existing inbound sessions on their next request; reconnecting clients need the
new code. Reset does not retain or recover the previous code.

## Filesystem behavior

The existing causal journal, SQLite durability, verified content objects,
streaming staging, branch isolation, conflict retention and recovery remain.
These checks prevent data loss and corruption. They do not impose OS ownership
or credential policy. Private branches remain a filesystem feature.

```sh
tkfs -C MOUNT status
tkfs -C MOUNT branch experiment
tkfs -C MOUNT checkout experiment
tkfs -C MOUNT checkpoint -m "working version"
tkfs -C MOUNT checkout main
```

This remains an experimental filesystem for regular-file projects. Automatic
text merging, symlinks, hardlinks, native ACL replication and garbage collection
are not implemented. Streaming files are bounded at 16 GiB; buffered APIs at
16 MiB. Sync transfers bounded resumable pages, with eventual per-peer progress
rather than a global distributed barrier.

Metadata and content now converge independently. Valid causal metadata can be
listed, renamed, inspected and relayed while file bytes are missing. File sizes
travel with metadata; `metadata_caught_up` and `content_complete` are separate.
A metadata acknowledgement is not a content backup receipt. Background transfer
continues after metadata acknowledgement, including historical shared objects.

Reads and conflict previews prioritize an absent object's download through the
orchestrator mesh. Reads wait up to 15 seconds for availability, then report an
unavailable-content error while keeping the request queued. Only verified whole
objects are exposed; a partial download never becomes an empty or truncated
file. Other folders and metadata operations remain available. Explicit truncate
to zero can replace a missing file without downloading its old contents.
Missing content survives restart; corrupt existing objects are quarantined and
can be downloaded again. Private branches have no automatic remote source.

This change uses **peer protocol 6**. Upgrade both ends of each connection;
protocol 5 peers cannot participate. Existing local journals are retained, and
available legacy object sizes/provenance are indexed during first startup.
SHA-256 object identities remain unchanged.

## Configuration and upgrade

The new configuration is **format version 2**. Existing filesystem state remains
readable, but old O1 catalogs, desktop journals, pairing configurations and
credentials are not migrated or loaded. Use a new v2 configuration and catalog.
Existing state can be opened with `tkfs daemon --state PATH --mount PATH` as a
standalone worker. Filesystem contents are not deleted or automatically migrated.

See [configuration](docs/CONFIGURATION.md), [CLI](docs/CLI.md),
[architecture](docs/ARCHITECTURE.md), [Linux](docs/LINUX.md), and
[validation](docs/VALIDATION.md). Old design documents and transport-specific
acceptance scripts were retired with their implementation; Git retains them.
