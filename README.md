# TKFS proof of concept

A real Windows WinFsp mount backed by a Rust runtime, immutable SHA-256 objects,
local SQLite, causal file revisions and direct authenticated peer replication.
Each branch **is** an independently writable worktree. The shared `main` branch
propagates saves and namespace changes automatically; `branch` creates a private
local branch. Checkpoints are coherent project snapshots, separate from saves.

This is an experimental filesystem for small regular-file projects. The exact
tested scope and remaining acceptance gaps are in [TKFS-PLAN.md](TKFS-PLAN.md).
The prior 101,897-byte design is preserved unchanged in
[TKFS-PLAN-2026-10-02-LEGACY.md](TKFS-PLAN-2026-10-02-LEGACY.md).

## Build and test

Prerequisites: x64 Windows, Rust/MSVC and Windows SDK, and an installed WinFsp
runtime **and SDK**. This implementation discovered WinFsp 2025, DLL version
`2.1.25156.ddca7bd`, at `C:\Program Files (x86)\WinFsp`. It did not install or
change the driver. Rust `1.96.0` and the available MSVC toolchain built the code.
`WINFSP_DIR` can select a different SDK directory. The current native link target
is x64; other Windows architectures are not supported by this build script.

Run in PowerShell from this repository:

```powershell
# Process-local DLL lookup; does not modify the system PATH.
$env:PATH = 'C:\Program Files (x86)\WinFsp\bin;' + $env:PATH
cargo build --offline
cargo test --offline
cargo fmt --check
cargo clippy --offline --all-targets -- -D warnings
python scripts\e2e.py
```

The offline commands use the dependencies already cached on the implementation
machine. On a fresh development machine, fetch Cargo dependencies normally before
using `--offline`. Cargo.lock pins them. Python 3.12 runs the mount acceptance
harness without third-party Python dependencies.

The E2E harness launches **two local runtime processes and two real WinFsp
mounts**. It generates an ephemeral 256-bit pairing key, binds only to loopback,
and terminates its own processes in `finally`. Fixtures and stdout/stderr logs
remain under `test-runs/<run UUID>/`. [VALIDATION.json](VALIDATION.json) records
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
`.tkfs-runtime.json` file at the mount root. For checkout, keep the calling shell
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

## Two explicitly configured devices

These steps are a runbook for future **real second-machine validation**. They
have not been run on two computers. No persistent service, credentials, firewall
exception, driver installation or remote infrastructure is created by this repo.

1. Build/copy the executable onto each x64 Windows machine; verify the existing
   WinFsp runtime/SDK and process-local DLL PATH. Installing a driver is a
   separately authorized setup step if missing.
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
object hashes referenced by those events. Protocol version 2 paginates metadata
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
caches reduce retransmission and are scoped to the configured peer UUID. A
one-second retry loop and `sync` perform exchanges. Objects arrive and verify
before metadata can become visible. `status` separates unflushed local staging,
local durable state, queued/acknowledged publication and last observed catchup.
Catchup also requires no incomplete incoming events; status exposes pending and
quarantined counts. Catchup is knowledge at a roundtrip, not a global distributed
barrier. Branch identity is its UUID: private and shared names may overlap, and
concurrent shared publications with one name remain distinct branches. An
ambiguous name fails clearly; use the ID returned by `branches`.

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
pass. See [SCALABILITY.json](SCALABILITY.json) and [VALIDATION.md](VALIDATION.md).

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

Limits: 16 MiB per object/file, 8 MiB per locally generated event, 64 MiB per peer
frame, 4,096 competing topology witness cuts per dependency cone, whole-file buffering and
unbounded retained history; no GC. Names use version-1 NFC plus Unicode lowercase,
not full Windows ordinal casing equivalence. Links, reparse points, ADS, persistent
ACL edits, arbitrary attribute/timestamp setters, distributed locks and live
databases are unsupported. Only the tested mapping refusal is qualified; general
mapped-write coherence and power-loss/storage-device guarantees remain unproven.
