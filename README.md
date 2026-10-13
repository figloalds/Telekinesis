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
other three lanes. Objects within each repository are distributed across the bulk lanes, while
the reserved lane visits all repositories. Requests carry local changes and
responses carry remote changes, so an outbound-only orchestrator can both send
and receive. Multiple peers and relayed replica origins are supported.

Each peer lane also has its own persistent connection to the local worker;
control requests do not queue behind a bulk socket. Active content pages yield
for 10 ms between rounds; advancing metadata also continues without an idle pause.
`sync_interval_ms` controls idle polling. Each object has one serial lane owner,
while different objects in the same repository transfer concurrently. Bulk I/O
uses separate database connections and per-object locks outside the metadata
engine. Control activation yields after at most 32 event commits or a 50 ms
target (an individual atomic event can take longer). Peer status includes
per-exchange object/wire byte counts, cumulative transferred bytes, and
local offer, remote exchange, and local apply timings to distinguish worker cost
from remote round-trip cost (which includes remote processing).

New endpoints negotiate `tkfs-binary-gzip-v1` on both worker IPC and peer
WebSockets. Object payloads cross as raw binary attachments; metadata stays JSON.
Messages use gzip level 1 when a compression probe and final size comparison
show savings, otherwise raw binary. Compressed and expanded sizes are bounded,
and SHA-256 verification still applies to the original file bytes. Older
endpoints retain JSON and the previous single-bulk-lane-per-repository behavior;
upgrade both ends to use parallel bulk protocol 1 and compression together.

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

The daemon creates a new local replica and starts its worker headlessly. For a
mounted `receive`, it waits for a successful metadata round trip with the selected
peer: accepted event inventories must match and both incoming queues must be
empty. It then restarts the worker with the requested mount. `manage inspect`
reports `initial_sync_pending`, `startup_phase`, `mounted`, and per-peer `sync`
progress/errors while it waits. The wait survives daemon restarts; an unavailable
peer or incomplete causal history leaves the mount unpublished. File bytes may
still download after mounting. Later remounts use the durable local journal and
remain available offline.
Use `--headless` on `receive` when no mount is needed. Existing matching
repositories sync automatically; B needs no inbound listener and A needs no
outbound pairing entry. Only running workers appear in the remote folder list.
A paired orchestrator can discover all running folders and sync shared branches.
Private branches and checkpoint history remain local.

To investigate a folder that is not converging, first run `manage peers` and
`manage inspect STATE_ID`. A connected socket does not mean replication is
healthy: inspect each lane's **repository** error, failure count, and
`metadata_progress` (local/remote event counts, differences and pending counts).
Use `tkfs --runtime BACKING_STATE/runtime.json status` for worker health and
content availability. Objects are content hashes, not directory entries; counts
can differ because of duplicate content, old revisions, and partial downloads.

For a deeper comparison, the read-only diagnostic script reads a consistent
SQLite transaction, including its WAL, without opening or rebuilding the store:

```sh
python scripts/inspect_metadata.py --state BACKING_STATE --output local.json
# Run the same command on the other machine and retrieve its remote.json.
python scripts/inspect_metadata.py --compare local.json remote.json
```

Compare accepted shared events, payload hashes, pending prerequisites, active
branch and projection hashes. `missing_parents_absent_on_both` means neither
sampled replica has those required events, even in its inbox; locate an older
replica or backup to recover them. Local startup snapshots are disposable journal
caches, not an authoritative substitute for missing remote history. Do not clear
queues or fabricate acknowledgements to make the counts match. Legacy references
with missing bytes and size descriptors replicate as metadata with unknown size;
status reports `size: null` until a descriptor or verified content is recovered.
Unknown-size files cannot provide accurate filesystem attributes until then.

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
