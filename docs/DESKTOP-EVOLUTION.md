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
controls instead of successful-looking no-ops. UI worker's browser rendering
attempt failed at kernel initialization (`0x80070003`); native Slint synthetic
snapshot acceptance is the fallback and does not prove backend operations.

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
| M0 audit/contracts | This baseline, file ownership, rotation semantics, prototype mapping | Source inspection complete; browser rendering blocked; agreement recorded above |
| M1 desktop usability | Search/table/sidebar/status hierarchy, config selection, real details, unchanged local lifecycle/branch safeguards | Native synthetic light/dark rendering, no invented counters; real callback regression where safe |
| M2 code lifecycle | Owner reset, salted verifier, receiving-host check, protected input and approval, unchanged existing sync trust | Exact digit/length/leading-zero handling; old/new code rotation; persistence and malformed input; secret absence in DB/log/receipt; wrong pin, denied approval, revoked pair |
| M3 integration | CLI + desktop consume same backend contracts; corrected operational docs | Offline build/tests and fixture reset/enroll/approve/grant/discover/sync/restart flow; report partial completion honestly |
| M4 application completion | Fresh replica provisioning with same repo/new state+device, local mount, supervised background sync independent of GUI | Requires additional owner-local durable receive/service lifecycle APIs, crash recovery and two-host qualification; not completed by a dashboard/code feature |

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

Open product decisions for the coordinator: accept reusable code until rotation
and retention of pending exact-key approvals; prioritize the durable receive and
service-supervision milestone; retain explicit fingerprint/bootstrap exchange
until an independently reviewed authenticated discovery design is supplied.

## M4 feasibility after current milestone

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
