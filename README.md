# Telekinesis (TKFS)

A causal filesystem with Windows WinFsp and Linux headless FUSE mounts, backed
by a Rust runtime, immutable SHA-256 objects, local SQLite and authenticated peer
replication.
Each branch **is** an independently writable worktree. The shared `main` branch
propagates saves and namespace changes automatically; `branch` creates a private
local branch. Checkpoints are coherent project snapshots, separate from saves.

This is an experimental filesystem for small regular-file projects. The exact
tested scope and remaining acceptance gaps are in [TKFS-PLAN.md](docs/TKFS-PLAN.md).
The prior design is archived in
[TKFS-PLAN-2026-10-02-LEGACY.md](docs/TKFS-PLAN-2026-10-02-LEGACY.md).

## Implemented today

| Area | Implemented behavior | Scope |
|---|---|---|
| Storage and recovery | Verified immutable objects, SQLite journal, causal revisions/conflicts, disk-backed staging and retained failed saves | Native local state; 16 GiB streaming ceiling; buffered object APIs remain limited to 16 MiB |
| Worktrees | Private branches, shared branches, fenced checkout, explicit current-state publication, checkpoints and restore | One selected branch per runtime; checkpoints/history stay local |
| Windows | Real WinFsp mounts, basic attributes/four timestamps, notifications, directory scan cursors and portable Slint desktop | x64; installed WinFsp SDK/runtime required |
| Linux | Headless CLI/daemon, real FUSE mounts, owner-authenticated Unix control, POSIX permission bits and graceful stop | No desktop or O1 supervisor port; case-insensitive portable names |
| Local management | O1 supervisor, independent workers, durable operation receipts, start/stop/recovery and running limits | Windows, same-user named pipes; networking disabled in O1 |
| Persistent pairing | TLS 1.3 device identities, owner-approved enrollment, repository/replica grants, revocation and restart retry | Separate service on Windows/Linux; published data only |
| WSS | One enrollment/sync listener, outbound-only homes, persistent pools and bounded resumable pages | One active paired peer and two replica origins per repository |
| Object backend | Explicit export of verified shared objects and manifests to a native directory | Local test adapter; no cloud provider or garbage collection |

Shared saves propagate when replication is explicitly configured. A shared branch
in a local managed project alone does not establish a network connection. Automatic
text merging, writable simultaneous branch mounts, multi-peer relay and service
installation remain outside the implemented scope.

## Repository layout and reading guide

```text
src/             Rust core, runtime, platform adapters, desktop and transports
native/          Windows WinFsp bridge, local control and DLL loader
ui/              Slint desktop components
tests/           Rust integration and regression tests
scripts/         Acceptance harnesses, packaging and development helpers
deploy/          Example Linux systemd units
docs/            Platform guides, operating instructions, plans and validation notes
test-evidence/   Saved JSON reports, provenance and isolated research harnesses
test-runs/       Ignored disposable states, mounts, screenshots and logs
```

Start with the [documentation index](docs/README.md) and
[test evidence index](test-evidence/README.md). Platform/network guides are
[Linux headless/FUSE](docs/LINUX.md), [persistent pairing](docs/PAIRING.md),
[one-port WSS](docs/PAIRING-WSS.md) and [orchestrator CLI](docs/ORCHESTRATOR-CLI.md).
License texts and [third-party notices](THIRD-PARTY-NOTICES.md) remain at the root.
Saved reports record their original binaries, commits and fixture locations;
they are historical evidence, not a claim that every check ran against this checkout.

## Local orchestrator and later distribution

[ORCHESTRATOR-PLAN.md](docs/ORCHESTRATOR-PLAN.md) records the implemented local O1
slice and later product direction. `orchestrator` supervises independent worker
processes from one TOML defaults file and an owner-protected SQLite registry.
`manage` uses a separate same-user Windows named pipe for create/list/inspect,
start/stop, operation lookup and orderly supervisor shutdown. Managed O1 workers
have no peer replication; existing mounted store commands remain compatible.
Adoption, mount reassignment/trash/restore, O1 integration of pairing/catalog
contracts and service installation are deferred. Persistent pairing is implemented
as a separate portable service, described below. The portable Slint desktop uses
the same executable and management API.

## Portable desktop application

Build and launch the first Windows desktop slice:

```powershell
cargo build --offline --target-dir target/slint-ui
python scripts/package_desktop.py
.\target\desktop-portable\tkfs.exe
```

No arguments opens the UI; `tkfs gui` does the same. Existing CLI commands,
`orchestrator`, and the private `managed-worker` mode dispatch before any UI
initialization. One shipped executable runs separate GUI, supervisor, and worker
processes. WinFsp remains an installed prerequisite for mounts.

The app locates `orchestrator.toml` beside its actual executable, independent of
cwd. First run asks for a new/empty native data directory and a separate parent
for mounted project folders, with a running-project limit. It creates a versioned
config with a unique installation UUID and pipe name under an exclusive lock and
publishes it atomically. Concurrent first-run clients reread the winning config.
The application folder must be writable; for Program Files or a read-only folder,
move the portable application to a writable folder. There is no silent fallback
to another configuration or data root.

Existing configurations are validated, and the client checks API version,
installation identity, effective data root, build version, and capabilities before
attaching. After the first connection an adjacent `.tkfs-ui-identity.json` pins the
catalog identity; a missing pinned catalog is refused. Established missing,
empty, truncated or unrecognized catalogs are never initialized again. Only a
fresh empty directory or a valid initial-bootstrap marker allows creation;
interrupted first startup retains its installation identity. Legacy O1 configs
remain accepted. A legacy config without
`default_mount_directory` requires an explicit new-project mount path.

The UI supports listing projects, create-and-mount, selected local status,
project start/stop, opening a mounted folder in Explorer, operation inspection
and exact retry, explicit supervisor shutdown/reconnect, and About/attribution. All startup
and RPC work runs off the Slint event loop. Standard controls have keyboard focus
and accessible labels; layouts resize and use Windows DPI scaling. Closing the
UI leaves the supervisor and workers alive. Shutdown preserves desired-running
states for the next launch; stop changes one project's desired state.

Custom panels and standard controls share Slint's system palette, so light and
dark themes remain readable without changing Windows settings. Project lifecycle
buttons distinguish start and stop; Start is available for stopped projects.
Reconnect supervisor remains available after an interrupted connection. Completed
request details are under Activity; pending requests and their retry buttons stay
visible. Window close remains separate from supervisor shutdown.

Branches & snapshots shows the current branch and private/shared state. Supported
actions are new private branch, switch to a selected branch, publish the current
visible state into a NEW shared branch (requiring explicit checkbox confirmation),
save a checkpoint, and restore a checkpoint into a NEW private branch. Creating,
publishing, and restoring do not switch the current branch. Managed projects have
no configured peer; Shared does not mean uploaded. Branch deletion, rename,
merging, pairing, and conflict resolution are not exposed by this slice.

Branch requests are saved separately in `.tkfs-ui-branch-operation.json` before
delivery, with installation/state/repository/device identity, original UUID,
payload and view generation. Retry uses the original request even after a lost
checkout response. Dialogs capture their project and active branch; a changed
project selection or view is refused. Open handles and unsaved data remain subject
to runtime busy checks. Cancel dismisses an unsent dialog; there is no in-flight
abort API. Branch RPC stays on the controller worker, without shell commands or
direct store access.

Mutation UUIDs, exact payloads, and expected generations are persisted before
send in `.tkfs-ui-operation.json`. A timeout is uncertain delivery, not cancellation.
Pending operations are inspected/retried using the same request. Terminal stale
generation failures require refreshing and issuing a new action. There are no
percentage progress or cancellation APIs. Managed workers have no peer configured;
the UI does not expose networking, adoption, trash, mount reassignment, or services.

Slint is pinned to 1.17.0 with the winit Windows backend, software renderer, and
accessibility support. Slint uses the Royalty-free Desktop, Mobile, and Web
Applications License 2.0 with the official AboutSlint widget in the accessible
About screen. The repository declares GPL-3.0-or-later in Cargo.toml. The
portable package contains attribution, license texts, registry notices/inventory,
and the corresponding application source archive. See
[THIRD-PARTY-NOTICES.md](THIRD-PARTY-NOTICES.md).
This is an experimental local package, not an installer or release qualification.

```powershell
python scripts/desktop_e2e.py
python scripts/desktop_e2e.py --exe target/desktop-branches/debug/tkfs.exe --theme light --branches --report test-runs/branches-light.json
python scripts/desktop_e2e.py --exe target/desktop-branches/debug/tkfs.exe --theme dark --branches --report test-runs/branches-dark.json
python scripts/catalog_startup_e2e.py
python scripts/orchestrator_e2e.py --exe target/slint-ui/debug/tkfs.exe --report test-evidence/DESKTOP-ORCHESTRATOR-VALIDATION.json
```

`gui-test --report <path>` is an explicitly selected acceptance mode for an
isolated copied executable. It opens the real Windows Slint window, exercises
component callbacks and toolkit keyboard events, and captures rendered BMP
screenshots. It is not physical OS mouse/keyboard automation. The harness checks
first run, two real independent mounts, busy stop/exact retry, durable data after
start, GUI close/reopen, and cooperative shutdown; fixtures and screenshots remain
under `test-runs/desktop-<UUID>`. Evidence is in
[DESKTOP-VALIDATION.json](test-evidence/DESKTOP-VALIDATION.json). Native
folder picker interaction and general screen-reader behavior require manual QA.

## Build and test on Windows

Prerequisites: x64 Windows, Rust/MSVC and Windows SDK, Python 3 for the acceptance
harnesses, and an installed WinFsp runtime **and SDK**. This implementation
discovered WinFsp 2025, DLL version
`2.1.25156.ddca7bd`, at `C:\Program Files (x86)\WinFsp`. It did not install or
change the driver. Rust `1.96.0` and the available MSVC toolchain built the code.
`WINFSP_DIR` can select a different SDK directory. The current native link target
is x64; other Windows architectures are not supported by this build script.

Run in PowerShell from this repository:

```powershell
cargo build --offline
cargo test --offline
cargo fmt --check
cargo clippy --offline --all-targets -- -D warnings
python scripts\e2e.py
```

These `--offline` commands require cached dependencies. On a fresh machine use
`cargo build --locked` first; Cargo.lock pins the dependency set. Harnesses write
summary reports to `test-evidence/` by default and retain disposable mount/state
fixtures in ignored `test-runs/`. Use `--report <path>` to preserve a separate run.
Packaging accepts `--exe <path>` and `--output <folder>` for an isolated build.

No WinFsp `PATH` setup is needed for the new executable or test binaries. WinFsp
is delay-loaded only for mounts, using an absolute path from `WINFSP_DIR` when
explicitly set, otherwise the installer registry and conventional installation
directory. A bad override fails with `WINFSP_RUNTIME_UNAVAILABLE` and the attempted
path. It never changes machine PATH or installs a driver. Older already-built
executables retain their original DLL lookup; use a newly built executable.

For an isolated build while another daemon holds the default executable:

```powershell
cargo build --offline --target-dir target/o1
python scripts\orchestrator_e2e.py --exe .\target\o1\debug\tkfs.exe
```

## Foreground local management

Create `orchestrator.toml` in the repository (choose a fresh data directory):

```toml
format_version = 1
data_directory = 'test-runs\managed-data'
[control]
transport = 'named-pipe'
name = 'telekinesis-local'
[network]
enabled = false
[workers]
restart_policy = 'on-failure'
maximum_running = 8
```

Relative data paths resolve against the config file. Unknown fields/versions and
enabled networking are refused before workers launch. One supervisor owns each
registry; the data root must be fresh or an existing O1 registry. The foreground
supervisor owns its data root and restricts its ACL to the current Windows user.
Keep it running in one shell:

```powershell
.\target\o1\debug\tkfs.exe orchestrator --defaults-file .\orchestrator.toml
```

Use a second shell for management. Mount parents must exist and targets must be
absolute, unoccupied and outside the data root. A create without `--mount` starts
a headless worker. Mutations require the observed catalog generation for create
or shutdown, and the state's `management_generation` for start/stop.

```powershell
$tkfs = '.\target\o1\debug\tkfs.exe'
& $tkfs manage --defaults-file .\orchestrator.toml list
$request = [guid]::NewGuid().ToString()
$mount = Join-Path (Get-Location).Path 'test-runs\ManagedProject'
& $tkfs --request-id $request --generation 0 manage --defaults-file .\orchestrator.toml create Project --mount $mount
& $tkfs manage --defaults-file .\orchestrator.toml list
# Copy state_id and management_generation from list/inspect:
& $tkfs manage --defaults-file .\orchestrator.toml inspect '<state_id>'
& $tkfs --request-id ([guid]::NewGuid().ToString()) --generation 0 manage --defaults-file .\orchestrator.toml stop '<state_id>'
& $tkfs --request-id ([guid]::NewGuid().ToString()) --generation 1 manage --defaults-file .\orchestrator.toml start '<state_id>'
& $tkfs manage --defaults-file .\orchestrator.toml operation $request
# For this one-created-state example, catalog generation is now 1:
& $tkfs --request-id ([guid]::NewGuid().ToString()) --generation 1 manage --defaults-file .\orchestrator.toml shutdown
```

After a timeout, retry the exact action, operation UUID and expected generation.
Completed receipts replay before checking current generation; changing parameters
under the same UUID is refused. Automatically generated UUIDs are printed to
stderr before the request. Busy/unavailable errors retain queryable pending
intent; terminal validation errors are recorded. Automatic worker retries use
bounded backoff and the configured running limit. A pending shutdown requires
an exact retry and inhibits startup recovery, automatic restarts and pending
create/start retries while shutdown is incomplete. Windows sharing/lock/busy
I/O violations remain pending and resumable; invalid markers/identities remain
terminal. A temporarily blocked staging installation retains its allocated
identities and mount reservation across retries and restarts.

An unexpected supervisor exit leaves workers mounted. Restart authenticates the
instance/state/repo/device/mount before reattaching. An unverified occupied store
is reported unavailable and never killed or replaced. Graceful shutdown refuses
open handles/mappings, unmounts, verifies worker exit and preserves desired states
for the next supervisor launch. Closing a management CLI has no lifecycle effect.
The worker's lifecycle secret is transient local process authentication in an
owner-protected `worker.json`, not a machine pairing credential. There is no
service/logoff/reboot or power-loss qualification in O1.

The isolated acceptance harness checks real mounts without `WINFSP_DIR`/WinFsp
PATH, lifecycle/ownership, five creation crash boundaries, retries, missing
metadata, authorization, mappings, worker limits and partial startup failures.
Evidence: [ORCHESTRATOR-VALIDATION.json](test-evidence/ORCHESTRATOR-VALIDATION.json) and
[ORCHESTRATOR-COMPATIBILITY.json](test-evidence/ORCHESTRATOR-COMPATIBILITY.json).

The offline commands use the dependencies already cached on the implementation
machine. On a fresh development machine, fetch Cargo dependencies normally before
using `--offline`. Cargo.lock pins them. Python 3.12 runs the mount acceptance
harness without third-party Python dependencies.

The E2E harness launches **two local runtime processes and two real WinFsp
mounts**. It generates an ephemeral 256-bit pairing key, binds only to loopback,
and terminates its own processes in `finally`. Fixtures and stdout/stderr logs
remain under `test-runs/<run UUID>/`. [VALIDATION.json](test-evidence/VALIDATION.json) records
the actual run directory and checks. This is not real two-computer validation.

## Single-computer use

Use an existing parent directory and a mount path that does **not** already
exist. Keep the state directory on native local storage, outside the mount.

```powershell
New-Item -ItemType Directory -Force .\test-runs | Out-Null
.\target\debug\tkfs.exe init --state .\test-runs\my-device
.\target\debug\tkfs.exe daemon --state .\test-runs\my-device --mount .\test-runs\Project
# Leave the daemon running; use another shell for the following commands.
.\target\debug\tkfs.exe -C .\test-runs\Project status
Set-Content .\test-runs\Project\hello.txt 'Hello TKFS'
.\target\debug\tkfs.exe --runtime .\test-runs\my-device\runtime.json branch experiment
.\target\debug\tkfs.exe --runtime .\test-runs\my-device\runtime.json checkout experiment
Set-Content .\test-runs\Project\hello.txt 'Private branch edit'
.\target\debug\tkfs.exe --runtime .\test-runs\my-device\runtime.json checkpoint -m 'Reviewable snapshot'
.\target\debug\tkfs.exe --runtime .\test-runs\my-device\runtime.json checkout main
```

`-C` or cwd discovers the project through the synthetic, read-only
`.tkfs-runtime.json` file at the mount root. The control file is omitted from every
directory listing, so a fresh mount is empty to Git and other listing-based
checks. It remains readable by exact path, including `stat`/`GetFileAttributes`,
under the existing mount ACL; this is enumeration omission, not a security
boundary or removal of the reserved namespace. The name remains reserved at all
directory levels. Standard `.`/`..` directory entries let Windows open a search
on an otherwise empty mount; normal listings omit them.

For checkout, keep the calling shell
outside the mount and use `--runtime`. Open file/directory contexts, watchers,
cwd references and memory mappings can cause `BUSY_VIEW`. Dirty edits already
flushed into a branch stay there when switching; no checkpoint is needed merely
to retain them. Checkout verifies target content, fences new opens, and remounts
at the same path to discard kernel caches. A remount failure reports an
unavailable view; restart the runtime using its retained durable selection.

The CLI is a runtime RPC client; it does not open SQLite or mutate objects.
`import <logical-path> <native-source-file>` is available for bounded fixtures.
`mkdir`, `rename`, `delete`, `state`, `events`, `branches`, `conflicts`, and
`cat-object <hash>` provide inspection and basic administration. Output is JSON,
except raw bytes from `cat-object`. `--generation N` rejects a stale checkout
context. `--request-id <UUID>` makes semantic mutations retry-safe: identical
payloads return the stored result; different payloads under that UUID fail.
Receipts and mutations commit atomically. `history` lists local checkpoint IDs,
branches, objects and messages. `restore <checkpoint-ID> --branch <fresh-name>`
copies its canonical snapshot into a new private branch, preserving file IDs;
use `checkout` to select it. Restore leaves current edits and the original
checkpoint intact. Checkpoint labels/history are not replicated between devices.

| Commands | Purpose/default |
|---|---|
| `init`, `daemon` | Native device state; mount and paired peer are explicitly configured |
| `branch`, `branches`, `checkout` | New branch is private; select by stable ID or unambiguous name |
| `status`, `sync` | Local durability/publication/catchup; one-second peer retry by default |
| `checkpoint`, `history`, `restore` | Local coherent snapshots; restore into a fresh private branch |
| `conflicts`, `resolve`, `recover` | Inspect retained alternatives and explicitly review exact conflict IDs |
| `publish --current-state-only` | Fresh shared branch from visible state only |
| `bucket-export --directory` | Explicit export of shared closure to a native local test directory |
| `state`, `events`, `cat-object` | Inspect canonical projection, journal, and immutable bytes |
| `import`, `mkdir`, `rename`, `delete` | Basic administration through the runtime |

## Linux headless/FUSE

Linux builds reuse the core, SQLite/CAS, branch/privacy and replication code,
with the kernel FUSE adapter in `src/fuse_linux.rs`. They do not compile Slint.
Use a native Linux checkout/state directory, a C compiler for bundled SQLite,
an available `/dev/fuse` and the installed `fusermount3` helper. Qualification
used Ubuntu 24.04.4 x86_64 under WSL2 with Rust 1.88 and FUSE 3.14.

```bash
cargo build --locked
cargo test --locked
./target/debug/tkfs init --state /native/path/new-state
./target/debug/tkfs daemon --state /native/path/new-state --mount /native/path/new-mount
# In another shell outside the mount:
./target/debug/tkfs --runtime /native/path/new-state/runtime.json status
./target/debug/tkfs --runtime /native/path/new-state/runtime.json stop
python3 scripts/linux_fuse_e2e.py
```

The mount target must not exist and its parent must exist. Local control checks
Unix socket ownership and peer UID; an exclusive state lock prevents a second
owner. Filesystem permissions 0000..0777, including executable bits, survive
saves, branches, checkpoints and replication. UID/GID are the daemon user's;
ownership changes, privileged modes, links, ACLs/xattrs and special files are
unsupported. Names remain case-insensitive with Windows restrictions.

Checkout and stop require a quiet view, including no open handles or cwd inside
the mount. Inotify, arbitrary mapped writes/shared mmap, cross-platform mounted
replication and installed no-login systemd operation remain unqualified. See
[the complete platform contract](docs/LINUX.md) and
[saved Linux report](test-evidence/LINUX-VALIDATION.json). Fresh Linux harness
fixtures go to ignored `test-runs/linux-evidence/` unless `--evidence` overrides it.

## Persistent pairing and published-data sync

`tkfs pairing` is a separate portable service using TLS 1.3 and distinct persistent
installation credentials. Windows credentials use CurrentUser DPAPI; Linux reads
explicitly provisioned systemd credentials. The owner enrolls a peer with an
expiring single-use invitation, approves its exact installation/certificate
fingerprint, and grants a repository with explicit local/remote replica bindings.
Approval alone grants no repository access. Private branches and private-only
objects remain local; remote peers cannot dispatch local management or mount
operations.

The CLI provides `new-id`, `credential-init` (Windows), `credential-provision`
(Linux), `run`, `status`, `invite`, `join`, `approve`, `revoke`, `grant`,
`remote-list` and `sync`. Select the config with
`tkfs pairing --defaults-file <pairing.toml> <command>`. Follow the complete
[credential, configuration and enrollment flow](docs/PAIRING.md) before running
the service; workers must be started independently. Persisted grants resume
with bounded retry after service restart, and each data operation rechecks
authorization/revocation. Unsafe private-storage ownership/ACLs fail closed.

The [WSS transport](docs/PAIRING-WSS.md) adds one inbound enrollment/sync port
and an outbound-only home configuration that binds no TCP listener. Direct TKFS
TLS termination authenticates approved certificates; control and bulk pools have
separate quotas, absolute deadlines and bounded resumable transfer pages. Raw TLS
remains the default compatibility mode. The current WSS URLs require numeric IPs
and explicit ports; DNS discovery, proxy trust and relay-only VPS operation are
not implemented. One active paired peer and two replica origins per repository
remain enforced. O1 does not supervise the transport service.

Saved [pairing](test-evidence/PAIRING-VALIDATION.json) and
[WSS](test-evidence/WSS-VALIDATION.json) reports cover disposable loopback peers,
real local worker bridges, privacy, revocation, reconnect and process restarts.
They do not qualify public WAN/VPS deployment, unattended boot or arbitrary
multi-peer topologies. Example units are in `deploy/`; they are not installed
automatically.

## Two explicitly configured devices

This section documents the legacy, explicitly configured PSK transport. For
persistent approved-device identities and repository grants, use the
[pairing service](docs/PAIRING.md) or [WSS guide](docs/PAIRING-WSS.md).

These steps configure the two devices. Ten live checks passed on
FIGLOALDS/FELYPE at commit `303fec8`; the original physical report was saved in
`test-runs/two-machine-448ed3f6-948d-4d90-b8b7-9b1cdbd82744/TWO-MACHINE-VALIDATION.json`
(an ignored local artifact, absent from a fresh checkout). The
[validation notes](docs/VALIDATION.md#physical-findings-and-acknowledgement-correction)
record the results and acknowledgement finding. The correction below is validated
locally, and has not been tested on two physical computers. The user configured the
firewall exception; agents did not change network settings, drivers or credentials.

1. Build/copy the executable onto each x64 Windows machine; verify the existing
   WinFsp runtime/SDK. New builds locate the installed DLL without PATH changes.
2. On A, `tkfs init --state <A-native-state>`. Record its repository UUID and
   device UUID from the JSON. On B, `tkfs init --state <B-native-state> --repo
   <A-repository-UUID>`. B must have its own device UUID and SQLite, not a copy of
   A's metadata database.
3. Configure one reachable TCP endpoint on each device, using an already
   authorized LAN/VPN/network path. For example, A listens on `10.0.0.10:7410`
   and B listens on `10.0.0.11:7410`. Do not change a firewall or expose a public
   listener without separate authorization.
4. Generate a fresh 32-byte random secret and convey it out of band to both
   devices. Set `$env:TKFS_PEER_KEY` to its 64 hexadecimal characters in each
   daemon's launching shell. Do not commit it, include it in arguments, or save
   it in the state directory. The E2E harness demonstrates this without printing
   the key. Restarting after a fresh pairing requires setting the secret again.
5. Launch each daemon with the other device's exact UUID:

```powershell
# A; run only after authorizing the intended network endpoints.
tkfs daemon --state <A-native-state> --mount <A-project-path> `
  --listen 10.0.0.10:7410 --peer 10.0.0.11:7410 --peer-device <B-device-UUID>
# B
tkfs daemon --state <B-native-state> --mount <B-project-path> `
  --listen 10.0.0.11:7410 --peer 10.0.0.10:7410 --peer-device <A-device-UUID>
```

The protocol uses AES-256-GCM authenticated encryption, random 96-bit nonces,
versioned bounded frames and authenticated sender/receiver UUIDs. Responses are
bound to their request UUID. This is a two-trusted-device pairing protocol, not
multi-user permissions, PKI, independent device signing, forward secrecy or
revocation. It is real TCP transport with real SQLite/object durability; no
remote bucket or centralized authority is implemented or required.

The sender transfers only events rooted in shared branches, and only the exact
object hashes referenced by those events. The current peer format is **5**;
metadata and objects are paginated
and objects under the frame bound, including a single atomic publication whose
objects need several pages. Incomplete events and verified received objects are
durably staged; metadata becomes visible and acknowledged only after all required
content and valid same-branch causal prerequisites are available. Missing parents
stay in the durable inbox, outside committed inventories/outboxes/local dependency
frontiers. A later parent with mismatched entity/register quarantines the child
and dependent descendants, while the valid parent and unrelated branches progress.
Startup resumes inbox activation and moves legacy incomplete journal entries back
to that inbox; their payloads/bytes remain available. A known hash in private CAS is insufficient authorization:
an inbound shared reference requires previously shared bytes or verified inbound
bytes. Invalid events are quarantined without blocking unrelated valid events.
Inventories, durable outboxes, peer
acknowledgements and idempotent event replay provide reconnect catchup. Inventory
caches reduce retransmission and are scoped to the configured peer UUID. Accepted
event inventories are durable receipts: both request and response paths atomically
reconcile the two-device outbox with the peer's current inventory. Withdrawn events
are queued again. Object inventories, incomplete manifests, quarantine and private
events cannot acknowledge shared metadata. Explicit acknowledgements must appear
in the sender's accepted inventory. A
one-second retry loop and `sync` perform exchanges. Objects arrive and verify
before metadata can become visible. `status` separates unflushed local staging,
local durable state, queued/acknowledged publication and last observed catchup.
Catchup also requires matching current shared inventories, zero outgoing/incoming
backlog, no pending causal events, no unflushed staging and healthy local storage.
Failed explicit or background exchanges invalidate the cached roundtrip result.
Status exposes pending and quarantined counts. Catchup is knowledge at a roundtrip, not a global distributed
barrier. Branch identity is its UUID: private and shared names may overlap, and
concurrent shared publications with one name remain distinct branches. An
ambiguous name fails clearly; use the ID returned by `branches`.

When an existing daemon locks the default executable, validate a separate build
without stopping it: `cargo build --offline --target-dir target/ack-fix`, then
`python scripts/e2e.py --exe target/ack-fix/debug/tkfs.exe --report test-evidence/ACK-FIX-VALIDATION.json`.
The [local correction report](test-evidence/ACK-FIX-VALIDATION.json) preserves the prior physical
and original local reports; exact commands/results are in [VALIDATION.md](docs/VALIDATION.md).

To reproduce the acceptance checks on two machines: save/rename on mounted
`main` at A and reopen at B; stop both daemons, restart without peer arguments,
save different bytes at the same path, restart to verify local persistence,
then restart with pairing arguments and compare `state` and `conflicts`. Repeat
private-branch canaries and A-B-A checkout on each machine. Use an actual chosen
editor/build workload as a separate compatibility test.

## Conflicts and explicit publication

An ordinary save descends from its actual handle's content/existence bases.
Multiple local handles share one staging buffer. Explicit flush publishes a
complete revision and continues the handle from that revision; cleanup/close
also attempts publication. A regular buffered write is not a durability promise.
Windows cleanup cannot return an error: failed close publication is logged and
sets runtime health, while explicit flush can return failure. Process-kill tests
validate acknowledged explicit flushes, not physical power-loss behavior.

Among revision heads, causal ancestors lose before a stable `(device UUID,
revision UUID)` tie-break selects concurrent winners. Clock values are
provenance, not ordering. Pairwise durable conflict records retain all competing
versions, common bases, authors and ancestry. Historical records and objects are
kept even after selection or review; viewing a winner never resolves a conflict.
Namespace history uses competing-fact witnesses within each entry's dependency
cone: its historical ancestors and same-name competitors, closed transitively.
Sibling files do not interact merely by sharing a folder. These cones include
three-way and wider cycles.
Content-only history and unchanged TRUE presence assertions without tombstones
add no topology witnesses. With tombstones, analysis retains boolean-winner rank
extrema per unbranched presence segment and causal relationship to topology/false
facts; explicit delete/undelete and delete/write semantics remain fully versioned.
All original events, revision parents and bytes remain durable.

The remaining cap is **4,096 competing witness cuts per namespace dependency
cone**, counting antichains of at least two relevant witnesses. Empty cuts,
sequential cuts and independent histories do not share/exhaust this budget.
Pathological topology contention exceeding it refuses the new commit with
`NAMESPACE_HISTORY_LIMIT` before acknowledgement, preserving existing records.
It is not a limit on ordinary save count: 10,000 durable content saves, later
delete/undelete, two 100-save offline replicas, and independent 65-rename chains
pass. See [SCALABILITY.json](test-evidence/SCALABILITY.json) and [VALIDATION.md](docs/VALIDATION.md).

```powershell
tkfs --runtime <state>\runtime.json conflicts
tkfs --runtime <state>\runtime.json cat-object <alternative-or-base-hash>
tkfs --runtime <state>\runtime.json resolve --conflict <reviewed-conflict-ID> --revision <chosen-revision-ID>
```

For three contenders, pass the exact reviewed pair IDs together to `--conflict`.
The resolution parents their union, while unseen concurrent revisions remain
competitors. Content, existence and same-file rename registers can be resolved
this way. `recover <entry-ID> <free-path> --conflict <reviewed-ID> ...` relocates
a displaced entry without replacing its stable identity or bytes. It reviews
exactly the supplied namespace/location conflicts; unseen concurrent contenders
remain preserved. The target parent must be visible and the target free. Recover
requires no live contexts. A deleted entry first needs an explicit existence
resolution. Automatic text merging is deferred. Inspect conflicts before using
a canonical tree to build or publish.

Old editor buffers may reopen a path after losing their original handle, hiding
the revision from which the text was derived. TKFS cannot infer that real base;
the new handle captures the then-visible revision. Live handle bases are tracked;
arbitrary stale GUI buffers require editor cooperation/reload. With no local
writer or dirty staging, existing read-only handles refresh after peer changes.
Writers retain their captured base and staging view. The WinFsp mount disables
file data caching on open and sends peer namespace/content change notifications;
the harness verifies .NET FileSystemWatcher create/change/delete, renames reported
as old-path deletion plus new-path creation, and a long-lived read-only handle.
Notification failures queue for retry and appear in
status. General IDE/build caches, user-space buffers and mapped-write coherence
still need workload-specific qualification.

Private branches never enter the transport's metadata or object selection.
Explicit publication creates a **new shared branch**:

```powershell
tkfs --runtime <state>\runtime.json publish released --current-state-only
```

This copies the canonical visible namespace/content only. It does not include
old private ancestry, private checkpoints, tombstones, hidden entries or private
conflict alternatives. Those remain durable locally. Publication does not turn
the original branch shared and does not merge into an existing shared branch.

## Optional local object backend

`bucket-export --directory <native-path>` writes immutable verified shared objects
and a content-addressed manifest into an explicit native test directory. The
directory must be outside state and mounted views. Export takes a coherent shared
journal snapshot, performs native I/O outside the runtime lock, and excludes
private-only objects/checkpoints. This adapter exercises the `ObjectBackend`
interface; it is not a cloud provider implementation or an automatic uploader.

Real object-store validation needs a chosen provider/endpoint, bucket/container,
test prefix/region, temporary authorized read/write credentials and approval for
test writes. No remote adapter, account, bucket or persistent credentials were
created. Provider consistency/checksum/retention behavior remains to be qualified.

Limits: 16 GiB per streamed file/object, 16 MiB for buffered object APIs (including
`import` and `cat-object`), 8 MiB per locally generated event, 64 MiB per legacy
PSK peer frame (WSS pages are limited to 8 MiB), 4,096 competing topology witness
cuts per dependency cone, and unbounded retained history; no GC. Staging keeps
small files in memory and spools beyond 256 KiB to disk; immutable revisions still
store complete file objects. Names use version-1 NFC plus Unicode lowercase,
not full Windows ordinal casing equivalence. Links, reparse points, ADS, persistent
ACL edits, attributes beyond basic Readonly/Hidden/System/Archive/Normal,
root-directory metadata setters, distributed locks and live
databases are unsupported. Only the tested mapping refusal is qualified; general
mapped-write coherence and power-loss/storage-device guarantees remain unproven.

Basic creation/access/write/change timestamps are independent Windows FILETIME
values, persisted with attributes in one causal `basic` register per entry.
Directory is derived from entry type; Normal means no other flags. Attributes
supplied at creation are honored. A metadata setter does not flush dirty content;
unchanged setters and reads generate no events. Reads do not automatically advance
access time. Content flush advances write/change time and sets Archive, unless an
explicit time was supplied after the buffered write or that handle disabled the
automatic update. Zero timestamps and `INVALID_FILE_ATTRIBUTES` leave values
unchanged; access/write/change `-1` disables automatic updates on that handle and
`-2` reenables them. A later write resumes normal updates unless disabled.
Readonly prevents new writable opens and deletion of regular files; an existing
writable handle retains its granted access. Synthetic discovery remains readonly.

Metadata follows rename, branch selection, restart, checkpoint/restore and shared
replication, with deterministic concurrent winners and reviewable alternatives.
Private metadata/history stays private; explicit publication copies only the
current visible metadata. Requested timestamps do not alter causal event clocks.

Upgrade **both replication peers** to this metadata-capable build. Peer protocol
is now **5**, including portable POSIX permissions; older formats are rejected
before receiving events or acknowledging any inventory, and the client retains
its outgoing queue. Mixed-version replication
is unsupported. Older incoming event validators also reject the unknown `basic`
field, but the explicit protocol refusal avoids ambiguous partial operation.
The SQL journal layout is unchanged and new builds read legacy stores (optional
cached metadata fields have defaults and are rebuilt from events). New checkpoints
use `tkfs-snapshot-2`; new builds also restore version 1, while old builds reject
version 2. **Do not reopen an upgraded state directory with an old binary:** old
store readers do not enforce a storage-version gate and can ignore new projected
metadata when rebuilding caches. Preserve a pre-upgrade backup for downgrade;
there is no claim of mixed-version or downgrade-safe storage support. Running
user daemons are not automatically restarted by building or packaging this patch.

Run the focused local Git acceptance with
`python scripts/git_compatibility_e2e.py --exe <fresh-tkfs.exe>`.
It covers default local/`--no-local` clones into the mount root and subfolder,
Git locks/atomic replacement, four timestamps, readonly behavior, restart and
nested discovery using disposable fixtures without an external repository.

## Runtime availability and retained experiments

The runtime publishes a small sampled health/status observation so probes can
respond during longer serialized work. Directory enumeration uses the
parent/name index for ordinary projections and a stable per-handle scan cut;
continuations retain that cut until rewind, and every access checks the branch
generation. Conflicted projections retain their conflict-aware enumeration.
These changes preserve the existing checkout/shutdown busy guard.

Large file saves stream from staging into verified immutable objects; peer
transfers resume sequential parts after restart and acknowledge metadata only
after complete object verification and causal activation. Availability regressions
exercise files above 16 MiB, sparse/random-access edits, checkpoint/restore and
export. The 16 GiB ceiling is a code limit, not a claim of acceptance at that size.

[Saved availability evidence](test-evidence/availability-20261004/ENUMERATION.md)
records a full local Godot clone, clean Git status, connectivity checks, concurrent
health probes and a persisted byte/hash audit. Its full live native read sweep
timed out, so the combined full-size run has no overall pass; cooperative shutdown
was verified separately. These results describe the recorded fixture and binary.

The [read-only generation experiment](test-evidence/read-lease-20261004/FINAL-REPORT.md)
and [chunked-CAS research](test-evidence/tkfs-chunked-cas-research-20261004/REPORT.md)
remain isolated research. Same-path live branch switching failed mapped-read
isolation; separate paths passed a bounded read-only experiment. Production
checkout still remounts a quiet view, and production CAS still stores whole-file
objects. Research findings do not imply writable branch mounts or a storage
format migration.
