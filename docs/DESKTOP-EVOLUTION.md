# Desktop evolution: audit, contracts and delivery ledger

Baseline audited 2026-10-11 at `cacbdd0` (`Documents update`). Initial worktree:
only untracked `misc/`, including the user's `misc/prototype.html`. Preserve it;
it is a visual reference, not production code or acceptance evidence.

## Product boundary and decisions

Build on the embedded Slint desktop and existing filesystem core. Keep TOML
format version 1; there is no YAML migration. Pairing grants published-data
discovery/sync only, never remote administration, remote shell access or private
branch access. All mutations of local states/mounts remain owner-local.

**Reset pairing code** creates exactly 200 independent random ASCII decimal
digits, including possible leading zeroes. Show the code once and persist only
a versioned salted verifier beside the selected orchestrator configuration.
The receiver verifies the submitted original code over the existing pinned,
authenticated transport. The code is reusable until explicitly rotated.

Rotation invalidates the old code for future submissions. It does not revoke
already approved certificate identities, repository grants, or durable sync
receipts. Already submitted pending candidates remain pending and still require
explicit local exact-key approval. Pause, pair revocation, and code rotation are
different operations. A lost reset response requires an explicit new reset;
there is no secret retrieval or automatic retry.

Safe scope: disposable fixtures only; no actual user code rotation, persistent
live credentials, existing state/mount mutation, public listener, installation,
firewall change, push or deployment. Fixture credentials/listeners are allowed
only within isolated tests and numeric loopback. Record platform limitations
instead of claiming an unexecuted cross-machine test.

## Verified capability and remaining gaps

| Area | Baseline evidence | Delivery consequence |
|---|---|---|
| Desktop | Windows-only Slint 1.17; `src/gui.rs`, `ui/app.slint`, `desktop.rs` | Retain embedded core and real callback execution; Linux GUI is not implemented |
| Onboarding | Shared `src/onboarding.rs`; init/start/create, pinned installation and strict catalog validation | Reuse existing config and identity; missing established catalogs must fail closed |
| Supervisor | `src/orchestrator.rs`: portable O1, Windows owner SID pipe/Linux same-UID socket, generation/operation receipts, worker reconciliation | Current docs describing O1 as Windows-only are stale; preserve operation semantics |
| State/mounts | Managed create/start/stop with local mount choice, independent worker processes; WinFsp and Linux FUSE | No managed adoption, mount reassignment, deletion/trash, quota, or replica-receive API yet |
| Branches | `src/desktop_branch.rs` and runtime: branch, checkout, current-state publication, checkpoint, restore | Keep truthful running/busy/stale guards and private-history exclusion |
| Pairing | Separate `pairing.toml`, protected credentials, single-use invitations, exact-key local approval, revocation | Add numeric code enrollment without replacing legacy invitation behavior |
| Network | `src/wss_transport.rs`, `ws_tunnel.rs`: pinned WSS and numeric-loopback WS with inner TLS | A code is not a replacement for authenticated server identity; numeric addresses remain required |
| Discovery/sync | `PublishedState`, grants, `paired_sync.rs`; all published branches, one active peer/two replica origins per repository | Do not advertise per-branch grant filtering or arbitrary multi-peer mesh |
| Resume | TLS service and WSS service already schedule configured sync/reconnect with bounded work/backoff | Desktop lacks service supervision and automatic replica provisioning; distinguish backend support from UI integration |
| Desktop pairing | `desktop_pairing.rs`: protected outbound client, discovery, existing runtime selection, manual sync/pause/resume/revoke | Remote discovery is usable; selecting a remote repo does not yet create/mount a replica |

Prototype source inspected in full. It proposes a navy sidebar, compact toolbar,
searchable folder table, summary cards, details and modal forms. Its folder sizes,
quota, online status, latency, remote mount/delete and path changes are in-memory
mock data. Translate layout, not claims: expose measured states; omit unsupported
controls instead of successful-looking no-ops. Initial browser automation failed
at kernel initialization (`0x80070003`); installed headless Chrome subsequently
rendered the reference with a disposable profile. Its screenshot and native
light/dark renders were inspected. Synthetic layout evidence is separate from
the real backend callback tests.

## Ownership and API contracts (agreed)

| Owner | Files and responsibility |
|---|---|
| UI worker | `ui/*`, `src/gui.rs`, `src/desktop*.rs`, `src/gui_*acceptance.rs`, desktop tests |
| Pairing worker | `src/pairing_code.rs`, pairing/service/sync/transport modules, `src/orchestrator.rs`, `src/lib.rs` module export, dedicated pairing tests; necessary Config literal changes outside UI files |
| Integration lead | This document and operational docs, `src/main.rs` shared CLI wiring, separate integration tests and validation ledger; serialized explicit-path milestone commits |
| Security reviewer | Read-only review and findings; no source/index writes |

No worker runs whole-tree formatting/staging or commits another worker's files.
Coordinate shared `Cargo.toml`/lock changes before writing; no new dependency is
planned. Build/test jobs share `target/`, so serialize expensive acceptance runs.

Agreed pairing API:

```rust,ignore
pairing_code::Store::for_config(orchestrator_config: &Path) -> Result<Store>
Store::reset(&mut self) -> Result<ShowOnceCode>
ShowOnceCode::expose(&self) -> &str
ShowOnceCode::generation(&self) -> u64
pairing_service::join_code(
    config: &Config, issuer: Peer, enrollment_endpoint: &str,
    code: Zeroizing<String>, stop: Arc<AtomicBool>
) -> Result<Value>
pairing_service::read_pairing_code() -> Result<Zeroizing<String>>
```

`ShowOnceCode` zeroizes on drop, redacts Debug and has no Serialize implementation.
The verifier is stored in the private sibling
`<orchestrator filename>.pairing/verifier.sqlite`, bound internally to the selected
O1 installation identity and canonical config path, without forcing the separately
established orchestrator and pairing identities to match. Replacement of a config
at the same path must fail closed. The existing directory/ownership checks must
protect that association; reset and verification plus pending admission require
cross-process ordering so admission after reset cannot use the old code.
The verifier uses fresh salt, a version/domain separator, constant-time checking,
and persisted rate limits. Uniform 200-digit CSPRNG output has about 664 bits of
entropy; this is not a human password storage mechanism.

Pairing `Config` gains optional `orchestrator_config: Option<PathBuf>`, resolved
relative to `pairing.toml`; omission preserves legacy behavior. No implicit scan
for another installation's verifier. Public issuer bootstrap metadata contains
the installation, certificate and sync endpoint, never the code or private key.
Client confirmation of that fingerprint remains explicit.

`Action::ResetPairingCode {}` is owner-local and mutating, but handled before the
durable operation journal. It requires absent operation ID and expected generation.
Normal response wrapping is retained, with result `{code, generation, display_once}`.
CLI syntax: `tkfs manage -f <orchestrator.toml> reset-pairing-code`; output is only
the original code once. Reject global replay/generation flags for this action.
UI must use a dedicated ephemeral dialog/property, clear on dismiss, and exclude
the response from general messages, snapshots and request journals.

CLI enrollment: `tkfs pairing -f <pairing.toml> join-code --issuer <public.json>
--enrollment-endpoint <endpoint> --fingerprint <independently-confirmed-SHA256>`.
`pairing public-bootstrap` exports public Peer JSON. Input comes through bounded masked stdin, never
argv. A successful submission returns a pending approval ID usable with the
existing approve command; enrollment alone does not grant a repository.

## Phased milestones and acceptance

| Milestone | Deliverable | Required evidence / status |
|---|---|---|
| M0 audit/contracts | This baseline, file ownership, rotation semantics, prototype mapping | Source and rendered prototype inspected; agreement recorded above |
| M1 desktop usability | Search/table/sidebar/status hierarchy, config selection, real details, unchanged local lifecycle/branch safeguards | Passed native light/dark layout and callback qualification; measured counters and guarded real actions |
| M2 code lifecycle | Owner reset, salted verifier, receiving-host check, protected input and approval, unchanged existing sync trust | Passed digit/rotation/crash/concurrency/private-storage/pin and secret-absence tests; legacy enrollment retained |
| M3 integration | CLI + desktop consume same backend contracts; corrected operational docs | Passed CLI and real light/dark reset/enroll/approve callbacks; deployment remains separate |
| M4 application completion | Fresh replica provisioning with same repo/new state+device, local mount, supervised background sync independent of GUI | Owner-mediated slice implemented; passed actual receive/WinFsp callbacks and post-GUI WSS transfer, Linux FUSE lifecycle, crash/retry and service ownership tests. Cross-host deployment remains unqualified |

M4 API design must allocate local state/device IDs before work, preserve remote
repo/branch identities, never copy a remote device ID, verify initial catch-up
before exposing a complete mounted view, and journal only public operation
metadata. Remote peers cannot invoke this owner-local API. Mount reassignment and
state removal require their own explicit lifecycle contract and are not hidden
inside a UI action.

Bounded end-to-end fixture path: create two fresh repositories with one logical
repo ID and distinct devices; start owned headless workers; provision temporary
test identities; reset fixture receiver code; submit against pinned loopback WSS;
approve exact candidate locally; configure both published-data grants; publish a
file, discover it and sync bytes; rotate code and reject an old-code new candidate;
restart fixture service/worker and sync using established trust without the code;
revoke and prove access denial. Test private data exclusion and current branch
preservation. Mount/native UI acceptance is separate and uses only owned fixture
paths if runtime support is available.

## Migration and compatibility

Existing orchestrator TOML, pairing TOML without code mapping, invitation format,
pair registry, grants and current state format remain valid. Numeric enrollment
uses a new typed wire payload; older hosts must reject it, not interpret it as
an invitation or downgrade authentication. Reset does not migrate or rewrite
the orchestrator config or touch state databases. The code cannot be recovered
from normal status, operation replay, diagnostics or backups of its verifier.
No change is claimed to cross-host certificate provisioning or Linux GUI support.

## Validation and retrospective ledger

At audit no build/test run was claimed. Current milestone evidence:

- `cargo test --offline --locked --test cli_pairing_code --test orchestrator
  --test paired_sync`: **12 passed** (2 CLI, 7 supervisor, 3 published-sync).
  The CLI fixture starts an empty headless supervisor, rotates twice, checks the
  unchanged TOML, zero operation/state rows, and no original code in verifier
  files. No mount, TCP listener or identity credential is created by this test.
- Restricted-token initialization failed Windows access checks. Normal-token
  tests then correctly refused unsafe sandbox TEMP and workspace ancestry.
  Read-only ACL inspection confirmed additional write principals. No existing
  permissions were changed. Tests passed with an approved fresh disposable
  parent under the already trusted user profile and inherited TEMP/TMP.
- UI worker reports native synthetic dashboard acceptance passed light/dark,
  including search/index preservation, empty results, pending/stale operation
  guards, details/configuration/pairing pages, minimum size, focus traversal and
  show-once erasure/screenshot refusal using synthetic placeholder input only.
  Evidence: `test-evidence/ui-productization/{light,dark}/report.json` and BMPs.
  Integration inspected the rendered light dashboard. This does not substitute
  for actual numeric enrollment through GUI callbacks.
- Pairing worker reports 7 code-store tests passed plus one ignored subprocess
  helper invoked by the passing crash-atomicity test; all 4 pairing-service
  tests passed, including actual numeric enrollment over TLS, WSS and encrypted
  loopback WS, pin refusal, rotation and preserved trust/grants. Broader registry
  and WSS regression checks remain in progress at this ledger entry.

Historical reports under `test-evidence/` are not evidence for this change.
Baseline lesson: the project already has
substantial real lifecycle/sync machinery; the main product gaps are cohesive
onboarding and durable integration, not reproducing prototype success messages.

Resolved decisions: reusable code until rotation, retention of pending exact-key
approvals, explicit fingerprint/bootstrap exchange, and the owner-mediated M4
receive/supervision scope below were accepted. Automatic network grant issuance
remains deferred until an independently reviewed protocol design is supplied.

## M4 feasibility at the original audit

The discover-to-mount gap is concrete, not cosmetic. `ListPublished` lists only
existing grants, and each grant already pins `remote_replica`; `Exchange` rejects
a fresh device ID that does not match. `PublishedState` returns the source repo
and source replica but does not authorize the new joining replica. O1 `Create`
always allocates a new repository ID and immediately reconciles its desired
worker/mount. It cannot receive an existing logical repo, hold it headless until
initial catch-up, then durably assign a mount. Closing a UI-created client does
not start a supervised pairing service.

Two bounded paths are possible without remote administration:

1. **Owner-mediated receive, recommended next vertical slice.** Add an
   owner-local durable `PrepareReplica` operation (existing repo, new device and
   state IDs, initially headless); return public replica metadata for the owner
   of the source to authorize via the existing grant command. Once both grants
   match, configure sync and catch up; add owner-local attach-mount/start after
   selecting a received branch. Expose these steps as a resumable wizard, with
   an explicit waiting-for-source-grant state. This preserves today's security
   model and removes raw runtime-path/manual standalone-daemon preparation.
   It requires coordinated supervisor/desktop work and crash/retry/mount tests,
   but no new network administration or grant authority.
2. **Automatic receive from a published offer.** Add explicit owner-created
   offers and a scoped network accept message binding the authenticated joining
   installation to its newly allocated replica. This changes grant admission
   and wire compatibility and needs separate security review for race/retry,
   malicious binding, quotas, offer expiry/revocation and existing two-origin
   restrictions. It must not be silently inferred from successful pairing.

For either path, run a distinct supervisor-owned pairing child with protected
credential references, authenticated readiness, bounded restart/backoff and
persisted enabled state. Do not launch an untracked UI thread and claim sync
survives closing the window. Completion requires isolated two-host/headless
restart/rotation tests plus optional fixture WinFsp/FUSE mount checks.

No user credential or public listener is required to implement/test path 1.
The remaining product choice is whether source-owner grant confirmation is
acceptable for the first complete receive wizard or automatic offers are a
hard requirement. Path 1 can proceed as the next implementation phase with
clear UI waiting states; path 2 requires an explicit reviewed protocol contract.

### M4 implementation contract (owner-mediated path approved)

The coordinator selected path 1. No new remote protocol operations or automatic
grant issuance are authorized by this stage. Integration owns `orchestrator.rs`,
shared CLI, integration tests and docs; UI owns desktop adapters/views and
native callback acceptance; pairing worker owns a new background-service
controller module once its interface is agreed. All commits remain serialized.

Owner-local `PrepareReplica { label, repo }` uses catalog generation and an
operation UUID. It preallocates a new state/device with the supplied logical
repo in the same intent transaction as ordinary create, always headless. Exact
retry returns the same state/device, mismatched replay is refused, and existing
managed repo duplication is refused before intent. Existing marker/staging/crash
recovery and worker reconciliation are reused. No existing store is adopted.

Owner-local `AttachMount { state, mount }` uses that state's management generation
and a retry UUID. It requires the headless worker to be cooperatively stopped
and ownership provable with its free state lock. It reserves a canonical unused
mount outside data, commits the mount while leaving desired state stopped, and
does not launch a worker. A separate existing Start action activates the mount.
This intentionally does not implement general remount/move/delete.

Wizard states: prepared headless -> waiting for source grant -> authorized
discovery/local grant -> syncing -> caught-up observation -> choose published
branch -> stop -> attach mount -> start. Public state/repo/device identities,
existing journals and persisted grants allow resumption; no enrollment code is
retained. After a worker restart, re-run initial sync to obtain a new caught-up
observation before first mount. A paused/revoked/missing grant stays visible and
must never be reported as mounted/caught up. Cancellation preserves committed
local preparation and sync receipts. Receiving-owner actions never change the
source owner's grant automatically.

Supervised background sync initially accepts only an already provisioned
outbound-only pairing config (no new key or public listener). Persist only its
public path/identity binding and enabled state; closing the GUI leaves it under
the local supervisor. Restart uses the configured protected credential, and
locked/changed/missing credentials yield explicit unavailable status/backoff.
Shutdown/disable must cancel and drain the service with bounded deadlines.

Acceptance extends the existing isolated headless process flow: prepare with
same logical repo/new device, duplicate/replay/crash invariants, source-owner
grant, published catch-up/private exclusion, branch selection, stopped-only
mount reservation and restart. Real fixture mount qualification runs only when
the installed WinFsp/FUSE supports it. Preserve old format-v1 catalogs and
generation semantics; any additional optional registry records require
transactional initialization and fail-closed compatibility review.

### M4 integration evidence and review resolutions

Implemented owner-local `PrepareReplica`, `AttachMount`,
`ConfigureBackgroundSync` and `BackgroundSyncStatus`. Prepare journals IDs before
creation; mount assignment leaves the worker stopped. Desktop attachment carries
the selected shared branch and view generation. The supervisor holds the state
owner lock while checking the stopped store's actual branch/generation, complete
causal history and visible content objects, then commits the mount reservation.

The controller pins normalized public config, registry identity and credential
fingerprint. It manages all enabled grants and never unpauses grants implicitly.
Startup failures back off; config drift cancels the runner. An uncertain run or
drain retains the actual Service/registry lock (or unfinished runner) for the
process lifetime. A separate process therefore cannot overlap detached transport
work. Supervisor Shutdown remains durably pending on uncertain drain and tick
cannot restart desired-running workers. This condition requires explicit process
recovery; automatic retries are not represented as a repair.

Validated on Windows with a normal owner token and disposable protected fixtures:

- CLI + supervisor + published-sync + onboarding regression command: 18 passed,
  three unrelated mount tests ignored in that batch. Includes O1 background
  binding persistence, restart, edited-config refusal, exact receipt replay,
  disable with a missing config, and restart remaining disabled.
- Managed controller: 8 passed plus a subprocess helper invoked by the retained
  lock test. Actual WSS existing-grant sync receives later data after controller
  restart without an enrollment code. Blocked child IPC plus forced outer-loop
  failure refuses a separate process opener before and after Controller Drop.
- Latest pairing service 4 and WSS 10 tests passed after lifetime changes.
- `uncertain_background_drain_keeps_shutdown_durable_and_workers_fenced` passed.
- The isolated WinFsp first-attach/start/write/stop/start fixture passed and
  preserved repository/device identity and file bytes.
- Actual numeric GUI callbacks passed both themes; see
  `test-evidence/ui-code-callbacks/{light,dark}/report.json`. These cover reset,
  enrollment, pin/config/address drift, code erasure/no disk leakage, existing
  pair preservation, and late response rejection after window close.
- Actual receive GUI callbacks passed both themes (26.43 seconds total); see
  `test-evidence/ui-receive/{light,dark}/report.json` and prepared/verified/mounted
  screenshots. Tests cover duplicate preparation, premature attach refusal,
  source-owner grant/discovery/configuration, paused-grant proof invalidation,
  shared checkout, planned stop, competing stopped-store checkout refusal,
  restart/resync recovery, mounted verified bytes and private-canary exclusion.
  Background service reaches running then disabled without resuming paused grants.
  The final expanded run passed in 28.94 seconds: after the GUI process exits,
  a fresh mounted-file write reaches the source over WSS with exact matching
  bytes; fixture supervisor shutdown then removes the mount cleanly.
- Thirteen desktop/branch/migration library tests passed (6.32 seconds). Atomic UI records now
  occupy dedicated config/purpose leaves under `.tkfs-desktop/`, avoiding a
  Windows rename conflict with protected pairing ancestors. Original lock paths
  remain; pending request bytes/IDs survive legacy migration, unrelated valid
  branch journals are not imported into a different installation, and changed
  legacy records fail closed. Public desktop metadata retains ordinary writable
  AppData/Temp compatibility; pairing credential protections are unchanged.
  Default and explicit config selection resolve the full existing config path
  before choosing the metadata scope and original locks; missing-file first-run
  remains supported, while a broken config symlink is refused.

An offline isolated Ubuntu/WSL source copy passed 23 targeted checks: code-store
7, shutdown fence 1, CLI 2, supervisor 7, published sync 3, prepare/attach crash
recovery 2, and actual FUSE first mount/restart 1. The Linux run exposed fresh O1
catalogs inheriting mode 0644 under umask 022; fresh creation now uses 0600 without
repairing existing permissions. Existing unsafe catalogs still fail closed for
pairing verification. No Linux desktop is claimed.

All evidence directories are ignored local artifacts; tests and this ledger are
versioned. No existing state/mount, live pairing code, credential, listener,
firewall rule or installed service was changed by qualification.

## Refinement R1: address, port and pairing key

The follow-up product request removes certificate files and fingerprint chores
from ordinary enrollment. Existing executable-adjacent and explicitly selected
orchestrator TOML files keep their location and identity. A different first-run
storage location must be a visible choice, never a silent migration. The requested
new screenshot is not yet available locally; provisional UI work uses the already
rendered `misc/prototype.html`, without claiming exact screenshot fidelity.

Ownership remains UI/desktop adapters with the UI worker, protocol and dependencies
with the pairing worker, shared setup/supervision/CLI/docs with integration, and
independent read-only security review. New integration modules are
`pairing_setup.rs` and `protected_secret.rs`.

The normal protocol is v3 OPAQUE with pinned `opaque-ke` 4.0.1, fixed suite and
bounded local Argon2 settings. Its dedicated provisional TLS 1.3 channel preserves
TLS signature verification, then authenticates server metadata through OPAQUE and
the TLS exporter. No original code is sent, no unverified fetched certificate is
trusted, no automatic downgrade occurs. The context binds the actual leaf,
exporter, protocol/suite/roles, generation, O1 identity and pairing identity.
Admission rechecks the code generation in a short transaction after KE3. This is
an integration requiring adversarial review, not a claim of formal cryptographic
certification or an audit of this exact dependency release.

The old salted hash cannot produce an OPAQUE registration. Existing installations
therefore need an explicit new pairing-key reset to enable simplified enrollment.
Reset still displays exactly 200 random decimal digits once; existing trusted
pairs, grants and configured sync survive. Explicit pinned v2 CLI enrollment and
v1 invitations remain compatibility paths; the normal UI never falls back to
them. OPAQUE server setup and registration are private material, encrypted
together with O1 binding before persistence. They are not harmless public hashes.

Explicit Connect calls `pairing_setup::client` and uses the selected O1 root's
`desktop-pairing/client.toml`. Explicit Enable receiving calls
`ConfigureReceiving { address: Some(numeric_ip), port }`, creating
`receiving-pairing/server.toml` and a separate supervised listener. These roles
have separate registries to preserve the existing single-owner service lock.
Setup commits one encrypted identity and its O1/role/address intent durably before
publishing credentials and configuration; crash retries reuse that exact identity.
Published configurations with missing, changed or corrupt credentials require
recovery and are never silently re-keyed. Config publication is create-only.

`ReceivingStatus` reports desired state separately from observed listener state,
public address, pending approvals and key readiness. `ConfigureReceiving` with
`address: None` disables; it does not revoke pairs or grants. Receiver bindings
include the actual O1 identity and use the same bounded drain/quarantine and
durable shutdown fence as outbound synchronization. Local pending approval does
not grant folders or create remote administration rights. Folder access remains
an explicit source-owner grant.

Windows app-created credentials and OPAQUE material use CurrentUser DPAPI.
Existing Systemd and WindowsDpapi credential configurations remain readable.
Linux uses an existing root-owned systemd 256+ host-backed credential provider
only when the process is already root. It does not elevate. A shared file lock
is held through direct helper completion, preventing systemd from replacing a
stale host secret; no host-key payload is read. Non-root IPC is excluded because
its privileged daemon can outlive local cancellation. No plaintext fallback,
host-secret initialization, unit installation, privilege prompt or OS security
modification is introduced. This deliberately does not claim universal Linux v3
availability. Existing advanced systemd deployments are not migrated. A separate
explicit `reset-legacy-pairing-code` preserves advanced pinned-v2 rotation on
unsupported hosts; it disables v3 enrollment and never runs as an automatic fallback.

Qualification used only disposable fixtures. The final Windows native three-field
callbacks passed in both themes (32.40s), including protected automatic setup,
wrong-key refusal, cancellation, local approval, reopening without reentering a
key, missing-registry recovery without repair, and plaintext-key file scans.
The two-process explicit profile setup/reopen test passed (5.69s). Desktop tests
(12), GUI tests (5), both reset journal guards, setup phase/crash tests (3),
receiving lifecycle tests and the real-listener/uncertain-outbound shutdown test
passed. The v3 protocol tests (3) include a TLS-terminating relay, wrong/changed
pin, reset between KE2 and KE3, missing final acknowledgement, malformed and
oversized messages, and replay on a fresh TLS channel. Store tests (8) cover
encrypted records, rate limits, generation fences and crash boundaries. Updated
CLI tests (2) and explicit legacy rotation also passed on Windows.

Linux Rust 1.88 compiled the full library and checked all test targets. Thirteen focused
tests passed: six protected-provider mock/subprocess tests, three real isolated
O1 verifier tests, two portable OPAQUE tests and two CLI tests. No real systemd
host secret or installed provider was invoked. The shared-lock synthetic test
proved a stale-host replacement attempt was killed and reaped before releasing
the lock, preserving the fixture inode and bytes. Helper I/O has a five-second
deadline per invocation; final child reaping deliberately precedes lock release
and is not an absolute wall-clock guarantee under an uninterruptible kernel wait.

Initial regression attempts failed: a service process fail-fast, a WS TLS
carrier error and a native legacy-callback live WAL access denial. Each passed a
bounded isolated/full rerun without relaxing production guards. Their original
causes are not established; passing reruns do not prove those failures harmless.
Latest legacy callbacks passed both themes (34.79s). Reports and screenshots are
under ignored `test-evidence/ui-address-pairing`, `ui-refinement` and
`ui-code-callbacks`; the final integration manifest records the built revision.

The final receive regression reproduced the protected-sidecar symptom in a
second real workflow: light passed including post-GUI transfer, but dark failed
during the receive callback phase with `PAIRING_STORAGE_UNSAFE` on
`client/registry/pairs.sqlite-shm`, flags 28, Windows access denied (error 5).
The failing report is retained at
`test-evidence/ui-receive-refinement/dark/report.json`; a single bounded rerun is
kept separately. This is an **open intermittent workflow reliability finding**.
Source-level security/architecture review is clear, but full product
qualification is not clean. No storage guard was relaxed and no root cause is
claimed. The bounded rerun passed both themes (30.03s), including transfer after
GUI exit and orderly mount cleanup, in `ui-receive-refinement-rerun`.
The source path is `private_storage.rs:261` (`validate_existing_files`) through
`local_ipc.rs:36` to `native/control.c:88` (`tk_storage_open`). Error 5 currently
does not distinguish open, metadata/type/link or descriptor validation failure.
A follow-up should add public validation-stage diagnostics and reproduce with
concurrent SQLite open/close/checkpoint; it must not ignore AccessDenied or remove
ACL/link/reparse checks.

Remaining product limits are explicit: numeric IP only; receiving address/port
fixed after initial setup; source-owner folder grants still required; no general
change-path/remove workflow, automatic firewall changes or service installation.
Portable application-folder configuration remains the default; profile setup and
reopening require explicit selection. Exact comparison with the newly attached
screenshot, cross-machine deployment and Linux GUI/installed-provider end-to-end
qualification were not completed. The previously rendered prototype informed the
layout; no screenshot pixel-match or formal cryptographic certification is claimed.
