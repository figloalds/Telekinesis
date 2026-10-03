# TKFS orchestrator and distributable product plan

Design direction recorded 2026-10-03. The bounded foreground local O1 slice is
implemented: defaults, registry/journal, create/list/inspect, strict store opening,
same-user management IPC, worker start/stop, crash reattachment and independent
mounts. Adoption and the later lifecycle, service, pairing and sharing APIs below
remain proposed. Filesystem contracts remain in [TKFS-PLAN.md](TKFS-PLAN.md).
See [README.md](README.md) for available commands and
[ORCHESTRATOR-VALIDATION.json](ORCHESTRATOR-VALIDATION.json) for local evidence.

## Accepted local O1 implementation contract (2026-10-03)

Implement the foreground, single-user, network-disabled supervisor first.
Use an owner-only Windows named pipe (including caller SID verification) for
management, separate from mounted discovery RPC. API version 1 mutations carry
UUID operation IDs and expected generations: catalog generation for creation
and supervisor shutdown, per-state management generation for start/stop.
The request hash includes the expected generation; clients retry the exact
request. Completed receipts are replayed before checking current generation.
Failures distinguish terminal validation/precondition failures from retryable
worker/busy/unavailable failures, and pending intents survive crashes.

Creation preallocates state/repository/device IDs in the registry transaction,
writes an operation-owned marker in staging, initializes with those identities,
and verifies identities before installation/activation. Existing stores are
opened strictly and never recreated by worker startup. Adoption is deferred;
the initial slice only owns stores it creates. No move/copy, trash or purge.

Workers survive an unexpected supervisor exit. Before launch the supervisor
records a fresh instance UUID and an owner-protected transient authentication
record; the secret is passed to the child through stdin, never argv or logs.
On restart, reattach only after a same-user management handshake verifies that
instance, state, repository, device and mount. If that handshake cannot prove
ownership and the state lock is occupied, report unavailable and never kill a
PID or blindly launch a replacement. An orderly shutdown fences new opens,
refuses busy views, cooperatively unmounts/stops workers and verifies exit before
completion. Readiness is reported only after the requested mount is installed.
Cooperative stop is therefore part of O1, before broader O2 lifecycle work.
Graceful supervisor shutdown preserves desired state for the next launch. Pending
shutdown inhibits automatic worker restarts until that same operation is retried;
replaying an already completed shutdown never stops a new supervisor instance.
The fence is checked before initial startup recovery and before executing pending
create/start retries. Recoverable Windows sharing/lock/busy I/O failures retain
pending intent; temporary staging-installation failure does not terminally consume
the creation UUID or reallocate identities. Invalid markers/identities remain
terminal failures requiring explicit repair.

O1 acceptance: two independently mounted UUID stores, duplicate supervisor/store
owner refusal, exact-request replay without duplicate creation, supervisor crash
and authenticated reattachment, strict missing-store refusal, durable data after
restart, caller/instance rejection, partial worker startup failures isolated to
one state, and orderly shutdown. Mount reassignment/removal/restore, adoption,
machine credentials/networking, service installation and cross-machine
qualification remain later work. Implementation evidence is recorded separately.

## Direction and process boundary

Add `tkfs orchestrator --defaults-file <path>` as a new mode in the existing
executable. It manages many local states and supervises one existing-style daemon
worker per running state. Enrich those workers with lifecycle and authorized
replication controls, rather than making the current single-state daemon also
own the machine catalog.

The orchestrator owns configuration, its own SQLite registry, state lifecycle,
mount reservations, machine identity, pairing, sharing contracts and background
jobs. Each worker continues to own its state's database, objects, staging,
handles, selected branch, mount and durable replication receipts. CLI and later
UI are clients of the same versioned management API. Closing either client must
not stop the orchestrator or its workers.

```text
CLI / desktop UI -> local management API -> orchestrator A
                                            | registry + key references
                                            | supervised state workers
                                            +-> state A1 -> WinFsp mount
                                            +-> state A2 -> WinFsp mount
                                            |
                                  authenticated machine connection
                                            |
                                        orchestrator B
                                            +-> state B1 -> WinFsp mount
```

This boundary matches the code today: `src/main.rs` launches one `Store` and one
`Engine`; `src/runtime.rs` has one optional `PeerConfig`; `src/mount.rs` uses
process-global `ENGINE`/`MOUNT` values. Child processes allow multiple states
without first rewriting the mount bridge for multiple engines in one process.
The existing exclusive `owner.lock` remains the final guard against duplicate
owners, including standalone daemons. Use bounded restart backoff and isolate a
broken worker so other states remain available.

This is local orchestration and direct peer sharing. It introduces no central
metadata server, global filesystem owner or ownership lease. Shared writable
branches retain the existing concurrent-write/conflict semantics.

## Identities and terminology

Here, a **state** is one local repository replica, including its native store;
it is not a branch or a snapshot of the current projection.

| Identity | Meaning and scope |
|---|---|
| `orchestrator_id` | Stable UUID for an installation's management/trust identity, bound to its credential; not its hostname or address |
| `state_id` | Local registry UUID; directory name under `states/`; remains stable across mount changes |
| `repo_id` | Logical repository shared by replicas on different machines |
| `device_id` / `replica_id` | Existing per-store event-origin identity; preserve it during adoption |
| `branch_id` | Existing authoritative branch identity; labels are display names |
| `pair_id` | Durable trust relationship between two orchestrators |
| `contract_id` and revision | Agreed sharing scope, participant bindings and policy |
| `operation_id` | Retry-safe management request/job identity |

A and B normally have different local `state_id` and `device_id` values for the
same `repo_id`. A machine identity authenticates which replicas it may represent;
it must not replace all existing event origins with one machine UUID. No identity
or authorization decision is based on a friendly name, mount path or remote path.

## One bootstrap config and a durable registry

Use one explicit versioned config file, similar in purpose to `--defaults-file`.
Proposed initial syntax is TOML; parser/format support is new work. Resolve
relative paths against the config file's directory, never the launcher's cwd.
Keep config limited to installation defaults and service policy. State records,
mount choices, pairings and contracts belong in the database, so a UI does not
rewrite a growing config file or introduce a second authoritative catalog.

Illustrative config, not accepted by today's executable:

```toml
format_version = 1
data_directory = 'C:\ProgramData\Telekinesis'

[control]
transport = 'named-pipe'
name = 'telekinesis-control'

[network]
enabled = false
listen = '127.0.0.1:0'

[workers]
restart_policy = 'on-failure'
maximum_running = 8
```

Load and validate the entire file before starting workers. Reject unknown fields
and unsupported versions. Start with only the defaults-file CLI override; any
future overrides need explicit documented precedence. A future reload should
report the effective configuration revision and which changes require restart.
Changing `data_directory` is an explicit migration, not a live config reload or
an instruction to silently initialize an empty replacement registry.

```text
<data_directory>/
    orchestrator.sqlite       # management metadata, versioned schema
    orchestrator.lock         # one supervisor per registry
    states/
        <state_uuid>/
            metadata.sqlite   # existing Store, including repo/device identities
            objects/
            owner.lock
            state.json        # versioned state/repo/device/creation-operation marker
            worker.json       # owner-protected transient instance/endpoint/token
            runtime.json      # transient discovery; protect access
    staging/                  # operation-owned incomplete creation/import
    trash/                    # retained states pending explicit purge
    logs/                     # bounded logs; no keys or bearer tokens
```

Protect the data root and control endpoint with OS permissions for the service
identity and authorized administrators/users. The keystore's protected storage
is provider-specific; `orchestrator.sqlite` contains references, not secret bytes.
Registry SQLite uses WAL, full synchronous durability, foreign keys and explicit
schema migrations. A live backup must use a supported SQLite snapshot mechanism;
copying only the main database file while ignoring WAL is insufficient.

The registry owns desired management state. The state's database owns actual
branch selection/generation and filesystem history. The registry may cache
worker observations but must reconcile them rather than overwrite core truth.
The registry is local and is not replicated wholesale to paired machines.

| Proposed record | Contents |
|---|---|
| `installation` | Schema version, orchestrator identity, credential reference |
| `states` | State/repo/device IDs, label, native location, OS owner, desired running state, management generation, retention status |
| `mounts` | State binding, locally chosen target, canonical reservation, desired/observed mount status; initially one per state |
| `paired_orchestrators` | Verified identity/key binding, endpoint hints, trust status, credential reference, last contact |
| `sharing_contracts` | Contract ID/revision, repository, branch IDs, accepted participant/replica bindings, permissions, activation/revocation state |
| `catalog_policy` | Which peers may discover which state/branch summaries |
| `operations` | Request UUID, caller, payload hash, durable intent, progress, result/error and affected generations |
| `audit` | Local management/pairing/policy changes; exclude secrets and file contents |

Avoid duplicating event journals, conflicts or object inventories into this
database. Workers retain authoritative receive/acknowledgement durability.
Observed PIDs are hints: validate the worker's identity and instance before
attaching, stopping or issuing commands to it, because PIDs can be reused.

## Management API and lifecycle

Expose local authenticated management IPC, preferably a Windows named pipe with
OS caller identity and ACL checks. Separate administrative capabilities from
ordinary mounted filesystem operations. Today's `.tkfs-runtime.json` bearer
discovery must not become a machine-wide administrator capability. A network
peer receives the separate catalog/contract protocol, not arbitrary local admin
RPC or a shell command interface.

Each mutation carries an operation UUID and expected management generation.
Persist its payload hash, intent and eventual result: replaying the same UUID
with different parameters is rejected, and retries do not create extra states,
contracts or jobs. Distinguish `accepted` from `completed`; return queryable
progress for long operations. Filesystem steps, worker RPC and registry commits
are not one SQLite transaction, so record and reconcile every external step.

Proposed API/CLI intents, with spelling to finalize before implementation:

| Intent | Behavior |
|---|---|
| `state create` | Allocate state/repo/device identities and initialize a new local store |
| `state adopt` | Validate and register an existing native store while preserving identities; explicit move/copy policy |
| `state receive` | Materialize an authorized remote offer into a fresh local UUID directory |
| `state list/inspect` | Local desired/observed status, worker health, mount, branch and sharing details |
| `state start/stop` | Start or cooperatively quiesce/unmount one worker; replication can run without a mount |
| `state mount/unmount/set-mount` | Manage one locally chosen view and its reserved path |
| `state remove/restore/purge` | Remove from use into retained trash, restore it, or explicitly erase managed local storage |
| `pair begin/accept/list/revoke` | Establish and manage machine trust independently of state sharing |
| `peer states` | Query the peer-authorized catalog with paging, revision and freshness |
| `share offer/accept/pause/revoke` | Agree repository/branch/replica scope and manage its lifecycle |
| `key list/rotate/lock/unlock` | Manage credential references and availability without returning raw secrets |
| `operation inspect` | Retrieve durable progress/result after timeout or restart |

Creation proceeds through durable intent, operation-owned staging, native store
initialization, verification, durable installation at `states/<UUID>`, and registry
activation. Preallocate identities so retries find the same store. Verify marker
identities before adopting any leftover directory. Registering a store does not
have to mount it. Worker launch is a separate reconciled step.

Reception additionally records the accepted contract and verifies objects/events
through the existing incoming/activation machinery. Show partial synchronization
as such; do not report a complete mounted replica or acknowledge incomplete
events. Initial MVP reception completes the initial authorized catchup before
mounting; partial/lazy content reads need a separate filesystem contract.

On startup, take the orchestrator lock, load pending operations, reconcile
registry/directory/worker identities, reserve mount paths, then start eligible
workers. Missing/corrupt registered stores become unavailable; do not call an
initializing store-open path and accidentally turn missing data into an empty
healthy state. Unregistered directories need an explicit recovery/adoption
decision. Worker ownership is revalidated after supervisor crashes; either
reattach over authenticated instance-bound IPC or apply the documented worker
shutdown policy before replacement. Never start a second owner blindly.

Mount changes are changes of view location, not moves of the state directory.
Validate and reserve the new target, fence new opens, require handle/mapping/cwd
quiescence, unmount the old view, then mount the new one. Preserve the selected
branch and native data. On failure, restore the old view if safely possible or
report an unavailable view with a durable pending operation. Completion means
the requested mount is observed and verified, not just written to the registry.

Canonicalize existing parents and account for case aliases and junctions when
checking overlap. Refuse mounts inside any managed state/data directory, data
inside mounts, overlapping managed mounts, occupied targets or an attempt to
replace ordinary user files. Remote mount paths never become local defaults.

Removal first stops new sharing and access, quiesces/unmounts, stops the worker,
and records a retained trash location outside any mount. Purge requires explicit
intent, rechecks canonical managed ownership and removes only that state store.
Adopted external stores must have a documented ownership policy; deregistration
does not imply permission to erase their original directory. Removing a local
replica or revoking a contract does not delete any other machine's copy. File
deletion *inside a shared branch* still replicates according to existing semantics.

## Pair once, then discover and agree sharing

Use persistent orchestrator identity credentials and an explicit pairing ceremony
that verifies the intended peer, for example by checking a fingerprint or using
a short-lived out-of-band invitation. Endpoint addresses are changeable hints,
not identity. Negotiate protocol/capability versions. Establish authenticated
encrypted sessions with replay protection, bounded frames and connection limits.
Select an established reviewed transport before implementation rather than
extending the PoC's shared-key framing into an improvised identity protocol.

The machine connection carries catalog queries, contract negotiation and scoped
replication streams. Workers continue to verify hashes, causal prerequisites,
privacy and durable acceptance. Every stream binds both orchestrators, contract
ID/revision, repository and participating replica identities. Dispatch only to
the bound local worker. Authenticate and authorize on receive as well as send;
possession of a machine pairing credential is insufficient to access every store.
Persist policy before releasing transfer bytes or accepting a stream.

Pairing creates trust, not blanket discovery, sharing or remote administration.
An owner can explicitly grant a peer catalog visibility for all eligible states
or a subset. Listings expose only authorized display metadata: offer/state ID,
repo ID, allowed shared branch IDs/labels, supported permissions, catalog revision
and freshness/availability. Do not disclose private branches, local mount/native
paths, secrets, arbitrary filenames or private history. A discoverable offer does
not itself grant transfer access. Catalogs are bounded and paged; offline cached
entries are visibly stale and must be revalidated on acceptance.

Example of the intended user workflow:

1. Pair orchestrators A and B once and verify their identities. Persist trust and
   protected credential references so restarting does not require retyping keys.
2. A chooses Project X and explicitly makes an offer visible to B, identifying
   its repository and shared branches. An owner-approved standing policy can
   permit automatic acceptance within a fixed scope; it must be inspectable.
3. B lists A's authorized states, selects the offer, and requests a local replica.
   B chooses its own name and mount path. Allocate B's local state/device IDs.
4. Both persist the same contract ID/revision and participant bindings. Pending
   negotiation is resumable; allow data only after bilateral acceptance is
   confirmed. A failed exchange leaves a pending job, not an assumed agreement.
5. Bootstrap and verify the permitted history/objects, register and optionally
   mount B's state, then continue offline saves and reconnect catchup under the
   existing conflict rules. Surface status per contract and replica.

Contracts must specify repository, branch-ID allowlist, exact replica bindings,
direction/permissions, whether future shared branches are included, history
scope and revocation behavior. Default to explicit branch IDs; adding a new
shared branch does not silently expand an existing contract. MVP supports the
current bidirectional trusted-writer model; read-only recipients, asymmetric
write authorization and automatic policies require additional enforcement/tests.

For an existing shared branch, bootstrap retains its authorized causal history
and alternatives. For publishing private work, use the existing explicit
`publish --current-state-only` into a new shared branch; a contract cannot turn
the original private history into shared data. Validate the complete event/object
dependency closure of a proposed branch scope; reject an unsatisfiable scope
instead of stripping causal parents or acknowledging an incomplete history.

Changing a contract uses a monotonically increasing policy revision and explicit
acceptance when granting more access. Either side can immediately suspend or
revoke its local grant without waiting for the other. Block new streams, invalidate
sessions and recheck queued/in-flight work before further acceptance. Reject
stale policy revisions after reconnection; re-enabling requires a fresh accepted
revision. This cannot retract already copied bytes or remotely prevent offline
edits. Define treatment of pre-revocation queued edits before supporting complex
membership changes; wall-clock timestamps cannot prove their authorization.

## Replication changes required by this design

The current worker sends all shared branches to its one configured peer, admits
only the two configured device origins, and maintains a single acknowledgement
flag per outbox event. Merely adding orchestrator routing or more keys cannot
provide branch-scoped contracts, arbitrary peer counts or relayed history.

Required changes are:

- Apply contract scope to event selection, incoming validation, event/object
  inventories, object provenance and authorized dependency closure. A branch's
  `shared` flag means eligible for sharing; an active grant selects its recipients.
  Initial `main` remains shared-eligible without being network-exposed by default.
- Bind machine identity to each authorized per-state replica identity. Preserve
  existing device IDs on adoption and reject repository/replica substitution.
- Store event delivery/accepted inventories per destination and applicable
  contract scope. Acknowledgement by B must not imply receipt by C. Retain existing
  receive-before-ack, quarantine, replay and withdrawn-inventory requeue behavior.
- Report catchup per destination/scope, distinct from local save durability or
  merely having a connected machine session. Report aggregate status only against
  an explicit required-recipient policy; disjoint branch inventories need not match.
- Bound simultaneous transfers, queue sizes and worker memory so a large state
  does not starve other states. Keep network waits outside core writer/SQL locks.

Start with exactly two orchestrators and two replicas per repository, while
allowing many distinct repositories over the one machine pairing. A third replica
and relay introduce event-origin authorization, historical membership/provenance,
fanout and revocation questions. Freeze that protocol and its acceptance tests
before permitting arbitrary mesh routing. Never widen the current two-origin
check to arbitrary UUIDs just to make relaying work.

## Keys, OS users and distribution

Introduce a keystore abstraction early, initially with one protected OS-backed
provider and opaque key handles/references. Separate identity credentials,
pairing trust and ephemeral session keys. Registry/config/UI/logs do not expose
raw secrets. Define rotation, peer re-verification, locked-keystore behavior and
credential recovery before enabling unattended restart. If credentials are
unavailable, local filesystem use can remain available while network sharing
reports locked/unavailable. This does not add encryption at rest to state objects.

The existing `TKFS_PEER_KEY` is an ephemeral PoC mechanism; persistent managed
credentials are an explicit new product capability. Copying the data directory
must not silently clone a machine identity or run two replicas with one event
origin. Restore/adoption distinguishes recovery of a stopped replica from a new
independent replica, which receives fresh identity bindings. Backups cover the
registry, consistent state stores/objects, schema versions and credential
recovery policy; a registry-only backup cannot restore file contents.

Target one installation-level supervisor and machine trust identity, with state
ownership and local permissions enforced separately. A computer-wide pairing
does not authorize every logged-in OS user to inspect every user's states or keys.
The first vertical slice can run in the foreground under one user while using
the final service boundary. Before a Windows service release, validate worker
launch credentials, named-pipe caller authorization, user-session mount visibility
and shutdown across logoff/reboot. User-session helpers may be required; do not
assume a service-created mount has the expected desktop behavior.

Distribution also needs a versioned package/installer, WinFsp prerequisite flow,
service install/uninstall/start/stop, protected default paths, diagnostics and
upgrade/rollback policy. Upgrades quiesce affected workers and back up metadata
before schema migration; older binaries must refuse incompatible stores rather
than attempt unsafe downgrade. Uninstall preserves states/credentials by default;
data removal is a separate explicit operation. Do not claim distributability
solely because the supervisor starts successfully.

## Delivery sequence and acceptance gates

| Stage | Deliverable | Gate before proceeding |
|---|---|---|
| O1: local supervisor | Implemented defaults/registry/IPC/journal, create/list/inspect, child-worker lifecycle; adoption deferred | Local tests passed: two independent real mounts, duplicate ownership, crash/intent recovery, durable data and missing-store refusal |
| O2: lifecycle | Mount reassignment, cooperative stop, retained removal/restore/purge, bounded jobs/health | Open handles/mappings refuse disruptive actions; overlap/alias checks; crashes between registry/native/worker steps recover; no deletion outside managed ownership |
| O3: identity and catalog | Protected keys, verified persistent pairing, version negotiation, catalog policy and paging | Restart without re-entering keys; wrong-peer/replay rejection; locked-key reporting; private metadata/path canaries absent from catalog |
| O4: two-machine contracts | Bilateral offers/acceptance, branch filtering, safe bootstrap, scoped transport/status/revocation | Multiple repositories over one pairing; state/branch isolation; partial bootstrap restart; current-state-only privacy; offline conflict/reconnect/ack-loss checks on two real computers |
| O5: distribution and UI | Service/package lifecycle, upgrade/backup/restore, desktop client of management API | Clean install and upgrade; user authorization/session-mount checks; UI close leaves service running; uninstall retains data; restore validates objects and identity handling |
| O6: expanded sharing | Third replica, fanout, read-only policy, optional trusted relay | Per-recipient receipts; authenticated historical origin membership; no grant escalation; scoped revoke/rotation and multi-replica offline permutations |

Completed first slice: foreground orchestrator, one defaults file, two UUID stores
and independent mounts, restart/reattachment, cooperative stop/start and durable
intent recovery. Next local slice: register-in-place adoption and O2 mount
reassignment/removal/restore. These remain separate from networking/UI.

The next slice pairs two machines once, advertises two authorized projects,
materializes one on the peer at a locally chosen mount path and proves that edits
and receipts remain repository/contract scoped. Keep the existing filesystem
acceptance gates, including the corrected acknowledgement implementation's
outstanding physical-machine qualification.

Later this can support a desktop state browser, storage/health dashboard,
share-offer wizard, key administration, headless replicas and explicitly selected
backup peers. Discovery through LAN rendezvous, relays/NAT traversal, remote
administration, GC, at-rest encryption and cross-platform mounts are separate
capabilities with their own contracts, not consequences of pairing alone.

The local O1 request/job, marker, shutdown/reattachment and single-user authorization
contracts are recorded above and implemented. Before their respective later
stages, finalize adoption, service authorization/session visibility, reviewed
machine transport/keystore provider and contract wire format.
The direction is a supervisor with per-state workers, one bootstrap config,
local registry and pair-once/per-state sharing; those implementation choices
remain open until their respective delivery stage.
