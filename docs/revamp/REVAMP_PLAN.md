# Telekinesis (tkfs) production revamp — final plan

Baseline: branch `revamp/production-sync`, commit `6298d0b`. Inputs: `synthesis.md` (RC1–RC13, invariants 1–30, pillars 0/A–F) and `reports.json`. Every claim below that cites a file or line was re-checked against the working tree. Where judges disagreed, the code decided.

This document is the single source of truth for the implementing agents. It covers:

| § | Content |
|---|---|
| 0 | How to read this |
| 1 | Lineage: what came from which proposal, and every judge must-fix with its resolution |
| 2 | Target architecture: processes, threads, failure domains, end-state module map |
| 3 | Orchestrator design: catalog, folder controllers, gate, status hub, management API, peer stack, inbound |
| 4 | Worker design: startup, locks, mount controller, inbox/activation, status cell |
| 5 | Core store changes: fast paths, append-mode derivation, inbox, indexes, feeds, spools |
| 6 | Protocols: worker IPC v3, peer protocol v3 (framing, handshake, auth), metadata feed format 7, objects v3 |
| 7 | Status model v2, legacy compatibility projection, concrete UI |
| 8 | Error taxonomy |
| 9 | Storage and migration rules |
| 10 | Invariant guardrails (all 30) |
| 11 | Decisions and rationale |
| 12 | Scope cuts |
| 13 | Delivery rules for parallel agents |
| 14 | Waves and work packages (owned files, interfaces, tasks, acceptance tests, verification) |
| 15 | Expected impact per wave, risk register |

---

## 0. How to read this

- **Package ids** are `WPw.n`; each wave also has an integration package `WPw.INT`.
- **"Frozen"** means a type, signature or file that no package may change during its wave. A change needs a **Contract Change Request (CCR)**, which the wave integrator applies between waves (§13).
- **"Owned"** means the only package allowed to edit a file in that wave. Exception: the append-only registry lines listed in §13.
- **Codes** in `SCREAMING_CASE` are the error codes of §8. The error class always comes from `model::errors::classify`.
- **Units:** times in ms, sizes in bytes. KiB/MiB are binary units.

---

## 1. Lineage and must-fix resolution

### 1.1 Base and grafts

**Base:** the *isolation-first* proposal, which scored highest overall: integrity 7, impact 7, implementability 7.5. These parts are kept nearly unchanged:

- the Wave 0 carve-ups and frozen contracts (`src/model`, `src/util`);
- trait seams;
- the catalog actor;
- the FolderController with its stop ladder, adoption and watch heartbeat;
- the RPC-first worker and the detachable mount;
- the StatusHub;
- interim legacy stabilizers;
- a single cutover package that deletes v2 networking.

**Grafted from *throughput-first*:**

- **Metadata:** a cursor feed over `journal_sequence`, validated by `(epoch, seq, id)`, with payload elision via `peer_known`, need lists, and acknowledgements derived only from per-id evidence in the peer's committed feed.
- **Objects:** receiver-driven object wants with per-object results and demand > current > history priority.
- **Transport:** a single non-blocking I/O thread per connection, driven by `polling`. This is the only design that is full duplex and WSS-safe on Windows without `SO_RCVTIMEO` polling.
- **Upgrade header:** the `X-Tkfs-Sync` upgrade header for fail-closed detection, so the code is never sent to an old acceptor.
- **Startup verification:** `Verify::Changed` verification markers.
- **Status:** status aggregation as pure, table-tested functions.
- **Stubs:** Wave 0 stubs for every cross-package seam.

**Grafted from *risk-first*:**

- **Oracle:** `Store::verify_derived_state`, which runs a rebuild inside a SAVEPOINT, compares, then rolls back.
- **Kill switches:** `TKFS_REMOTE_FASTPATH` and `TKFS_CONFLICT_MODE`.
- **Append-mode conflict derivation:** a provable restriction of full mode, with refusal counting over full-mode antichains. It ships **early** (Wave 1), with an exactness precondition and full-mode fallback.
- **Process:**
  - one integration package per wave, with merge order, qualification floors and evidence;
  - the CCR procedure;
  - legacy fixtures captured with the baseline binary;
  - a conservative data plane: no sparse spools, no cross-lane striping, per-append fsync, whole-object verification.
- **Liveness and poison handling:**
  - activity/watchdog-style liveness (realized here inside the session I/O thread);
  - engine poison handling: `into_inner`, then `retain_pending` for every dirty session, then `exit(70)`.
- **Shutdown:** leaves busy workers running for adoption.

### 1.2 Judge must-fix traceability

Every must-fix item from the three judge panels appears below with its resolution and its location in this plan.

**Integrity panel**

| # | Must-fix | Resolution | Where |
|---|---|---|---|
| I1 | Outbox acks only from per-id evidence. Durable rejections. No capped reject list that leaks acks. | An ack is set only when an id appears in the peer's **committed feed** and the 64-bit payload digest matches our committed payload. Rejections are durable rows (`feed_rejected`, quarantined inbox rows) and are never acks. Tests cover an unauthorized event, 300 quarantined events, and an event staged from a third peer. | §6.3, WP2.5 |
| I2 | Journal regression detection; epoch regeneration on delete; port the withdrawn-event behaviour; never stuck on `UNBOUND_OR_EARLY_ACK`. | Cursors are `{epoch, seq, id}` and are validated against `journal_id_at(seq)` and the tip. A mismatch means reset, clear `peer_known(peer)`, recompute outbox. `journal_epoch` is regenerated by an `AFTER DELETE ON events` trigger. A regression of a reported cursor clears `peer_known`. There is no ack field to be "early", so no stuck state. The test `peer_restore_requeues_withdrawn_events` covers it. | §6.3, WP2.5 |
| I3 | Conflict counting over full-mode antichains; reproduce DELETE or prove monotonicity; accepted histories must rebuild; incremental mode behind a switch. | Append mode runs only under an **exactness precondition**: the old cone set is a subset of the new cone set, and each touched cone's relevant set equals the old set plus the new event. Under that precondition the old antichains are unchanged and full mode's DELETE is provably a no-op. Otherwise full mode runs, including the DELETE. Counting is always over all full-mode antichains. `TKFS_CONFLICT_MODE=full\|append\|verify`; `verify` runs both and fails on any difference. The oracle rebuild catches any accepted history that full mode would refuse. Clock-based incremental derivation is a **scope cut**. | §5.2, WP1.7 |
| I4 | Private CAS: register only after a full verified transfer; no `AlreadyAvailable` or startup registration path. | The receiver skips an object only if it is already **shareable** (published or received_shared, and available). A physically present private object is re-transferred, verified and then registered. No startup reconciliation registers objects. The private-CAS tests, including the restart leg, are kept. A crash-between-install-and-register test is added. | §6.4, WP1.8, WP3.2 |
| I5 | No ALTER on positional-INSERT tables. | Side tables only. The single exception is `incoming_events`, whose writers all use named columns (`core.rs:914`, `:2287`). Audit list in §9. The false claim in the base proposal ("object_errors uses named columns") is corrected: `core.rs:440` and `runtime.rs:1059` are positional. | §9 |
| I6 | View cache on secondary handles. | The freshness key becomes `(branch, total_changes, PRAGMA data_version, transactional)`. | WP0.3 |
| I7 | Off-lock commit re-validates DB-dependent checks. | `commit_prepared` re-runs `EVENT_ID_REUSED`, `OBJECT_SIZE_CHANGED`, `BRANCH_DESCRIPTOR_CHANGED`, `UNREFERENCED_OBJECT_SIZE` and the branch-seq staleness check inside the commit savepoint, under the engine lock. | WP2.4 |
| I8 | Dirty sessions on poison and force; Shutdown never kills responsive or refusing workers; verified pid kill. | **Poison:** `into_inner`, then `retain_all_dirty`, then `exit(70)`. **Force detach:** fence, drain callbacks, persist dirty sessions, stop native, clear orphaned handles. **Shutdown:** leaves busy or unresponsive Running workers for adoption. **Kills:** require pid, OS process start time and image path to match `worker.json`. | WP1.4, WP1.5, WP1.2/1.3 |
| I9 | Gate deleted only after attach; source-based gate; fallback only on unpair or MountNow. | The gate source is the installation the folder was received from. The gate requires all of: cursor at the source tip; no inbox rows from the source in any state; no `feed_rejected` rows from the source; the source's `pending_active == 0`. Fallback to any sharing peer happens only when the source is unpaired. MountNow overrides. The gate marker is deleted only after `MountState::Attached`. | §3.3, WP1.2, WP2.7 |
| I10 | `rebuild_frontiers` after evacuation; chunked resumable backfills with progress; keep the forced snapshot seal. | `rebuild_frontiers` always runs (unchanged). Backfills are chunked, guarded by markers, and emit `startup_progress` from the opening thread. The forced seal after slow-path commits is **kept**. | WP0.3, WP1.8 |
| I11 | No pipelining of large bodies on blocking sockets; the reader never writes; WSS non-blocking; no replay. | One I/O thread per connection owns all reads and writes; nothing else touches the socket. `CallError::is_uncertain()`; no replay. | §6.2, WP1.9 |
| I12 | Route binding from authenticated identity; keep install-change and self-pair checks; no ack on payload mismatch. | Dialer routes come from the catalog plus the remote catalog of that authenticated peer. Inbound routes use the authenticated installation. `PEER_INSTALLATION_CHANGED` and `CANNOT_PAIR_WITH_SELF` are kept. A digest mismatch blocks `peer_known` and the ack. | §6.2–6.3 |

**Impact panel**

| # | Must-fix | Resolution | Where |
|---|---|---|---|
| M1 | Two-way activation is first-class. | Append mode lands in **Wave 1** (WP1.7), before the background activator (Wave 2). Qual scenario: A streams 2K creates while B has one unseen local edit. Floor: ≤ 100 ms per event at 2K. | WP1.7 |
| M2 | Positional INSERT audit. | See I5 and §9. | §9 |
| M3 | RC5 structural: per-(peer, repo, op) slots. | Multiplexed v3 sessions; MetaScheduler slots with per-request deadlines. Every Wave 1 test claim is achievable on legacy lanes (15 s timeout, then charge, back off, rotate). | WP1.9, WP2.7, WP1.12 |
| M4 | No `SO_RCVTIMEO` polling; no reader-side writes. | Single I/O thread with `polling` readiness. Socket timeouts exist only during pre-auth handshakes, and any timeout closes the socket. | §6.2 |
| M5 | Bounded engine-lock holds in every fallback. | Off-lock `prepare_slow` (WP2.4). A Stale result is re-queued with backoff, **never** committed inline in production. Starvation is reported in telemetry. Before WP2.4, append mode bounds the common slow path. | WP2.4 |
| M6 | Invariants 2, 10 and 19 preserved literally. | No sparse or striped spools; the durable offset is advertised only after fsync, and the durable offset is persisted. Acks keep the semantics of today's known-set (`remember_peer_inventory`). Replies stay bound by `reply_to`. `Lane::owns` sharding is kept, plus worker leases. | §6.3–6.4 |
| M7 | Status earlier. | Hub in Wave 1; `Status`/`Subscribe` plus CLI `manage status --watch` in Wave 2 (the first wiring wave); GUI in Wave 3. | WP2.2, WP3.4 |
| M8 | Wave 1 relief on the legacy path. | Wave 1 ships: remote fast paths, append mode, availability index and no startup re-hash, per-object outcomes, RPC-before-mount (mount failure no longer kills sync), quarantine excluded from pending, and lane stabilizers (charge failure to the repo, rotate, keepalive, contention, crash backoff, non-fatal main loop). | Wave 1 |
| M9 | Strict ownership; pre-declared seams; split overloaded packages. | Every intra-wave seam is a W0 stub (WP0.INT). Splits: catalog / controller / process (3 packages); worker entry / mount (2); session core / handshake (2); peer supervisor / scheduler / inbound (3); objects worker / orchestrator (2); runtime wiring / management API (2). | §13–14 |
| M10 | Accurate baseline facts. | `tests/orchestrator.rs` has 15 tests. WinFsp `tk_mount_start` already creates a fresh `FSP_FILE_SYSTEM` per call; only `mount.rs`'s `ENGINE` OnceLock (`:72`, set before the path check) and the permanent fence (`:98-108`) block re-attach. `polling` 3.11.0 and `rustix` 1.1.4 are already in `Cargo.lock` (via slint on Windows); on Linux they compile newly, with no new lock entries. | §11 |
| M11 | Mixed-version window. | No wire format bump on legacy lanes. Feed format 7 travels **only** inside v3 sessions. | §6 |
| M12 | Credential equivalence. | Documented: the verifier hash equals the code for HMAC purposes, and dialers store plaintext codes today. Token files are owner-only. Per-peer credentials and revocation arrive in WP4.1. | WP4.1, §11 |
| M13 | Heavy metadata off bulk lanes. | Large events travel only on the control connection, through `meta.exchange` need-serving. The session layer chunks bodies over 1 MiB into ≤ 1 MiB frames interleaved by priority. Bulk lanes never stage, activate or ack. | §6.2–6.3 |

**Implementability panel**

| # | Must-fix | Resolution | Where |
|---|---|---|---|
| P1 | Positional INSERTs. | See I5. | §9 |
| P2 | `Discovery` struct literal (`tests/runtime_lanes.rs:110`). | `Discovery` is never changed. Identity goes in `worker.json`; tokens go in a separate `runtime.token` file (WP4.1, which also owns `tests/runtime_lanes.rs`). | §6.1 |
| P3 | GUI in the bin crate. | The view model moves to the library module `tkfs::ui_model`, tested on Windows and WSL. `gui.rs` keeps only Slint glue. | WP3.4 |
| P4 | Forced seal. | Kept. | WP1.6 |
| P5 | Freeze the legacy List/Inspect/Peers projection. | Baseline goldens captured by WP0.5. Semantics table in §7.3. Until cutover, legacy LinkStatus JSON is served verbatim from the legacy lanes. | §7.3 |
| P6 | Gate test semantics. | v6 `pending_events` = ready + waiting + retry, so missing-parent orphans still count. The gate's new rule keeps "source `pending_active == 0`". `received_mount_waits…` stays semantically unchanged. | WP1.6, §3.3 |
| P7 | No intra-wave dependencies. | WP0.INT lands every seam stub. Objects v3 is split into a worker side and an orchestrator side, both tested against the frozen W0 op contract. Daemon e2e lives in WP3.INT. | §13 |
| P8 | No signature changes across a wave boundary. | New function plus wrapper (for example `receive_parts_outcomes` beside `receive_parts_data_locked`). | WP1.8 |
| P9 | Legacy deletions own their references. | `ObjectLocks` moves to `core::objects` in W0 and is re-exported from `bulk_sync`. `paired_sync` and `bulk_sync` stay as an in-process test surface (scope cut). Only the orchestrator's legacy network files are deleted, by their owner. | WP0.3, WP3.1 |
| P10 | Split packages; non-mechanical ≤ ~2,000 lines. | Done. Max non-mechanical estimate is 2,200 (WP2.5). | §14 |
| P11 | `tests/support` dead-code. | `#![allow(dead_code)]` at the top of `tests/support/mod.rs`. | WP0.5 |
| P12 | Verification protocol. | Per-worktree `CARGO_TARGET_DIR`; WSL target under `$HOME`; WSL `cargo test` for fuse, lifecycle, orchestrator and session packages; `scripts/suite-lock.ps1` serializes full-suite runs; WinFsp SDK and Slint are needed on Windows. | §13 |
| P13 | `Cargo.lock` ownership; HMAC. | Only WP0.4 edits `Cargo.toml`/`Cargo.lock` (adds `polling = "=3.11.0"`). HMAC-SHA256 is hand-written over `sha2` 0.10 with RFC 4231 vectors. | WP0.4 |
| P14 | Integration package per wave. | `WPw.INT` for every wave. | §14 |
| Base-proposal errors | `tests/gui_model.rs` could not import `gui`; WP4.2 lacked `worker_client.rs`; `hmac` was a new lock entry; `object_errors` was ALTERed; the forced seal was dropped. | All fixed: see P3, WP4.1 owned files, P13, I5, P4. | |

---

## 2. Target architecture

### 2.1 Topology (unchanged process model)

- **Orchestrator:** one per data directory (`orchestrator.lock`).
- **Worker:** one per folder state (`owner.lock`). Workers never dial peers (invariant 25).
- **GUI and CLI:** clients of the orchestrator's loopback control endpoint.
- **Peers:** reached through pools of TCP/WebSocket connections. Lane 0 is control; lanes 1..N-1 are bulk.

### 2.2 Design rules

1. **One owner per mutable resource.**

   | Resource | Owner |
   |---|---|
   | `folders.sqlite` | Catalog actor |
   | Each worker process | FolderController |
   | Each peer's sessions | PeerSupervisor |
   | Each connection socket | its I/O thread |
   | Each worker's mount | MountController |
   | Each worker's inbox activation | Activator |

2. **No lock is held across I/O.** Shared state is `util::Snapshot<T>` (`RwLock<Arc<T>>` plus a generation counter and a condvar) or a short in-memory critical section. Debug builds assert that orchestrator locks are held < 50 ms (`util::lock::Tracked`).
3. **Signals instead of sampling.** Ready lines, `watch` heartbeats and generation counters wake waiters. The 10 ms, 50 ms and 2 s polling loops disappear.
4. **Liveness is separate from latency.** Session heartbeats prove liveness, and every request has its own deadline.
5. **Typed, scoped failures.** `ErrorRecord {code, class, scope}` drives retry, status and UI.
6. **Crash-only internals under supervision.** A poisoned engine lock or a wedged accept loop makes the worker exit 70 after persisting dirty sessions. The controller restarts it with backoff and a circuit breaker.
7. **Durability first.** The journal is authoritative. Migrations are additive, and derived tables can be rebuilt.

### 2.3 Failure-domain matrix (end state)

| Domain (owner) | A fault here may stop | It must never stop | Recovery |
|---|---|---|---|
| Object (worker object pipeline) | that object | other objects; the other direction; the lane | per-object backoff (`object_error_state`). `CORRUPT_AT_SOURCE` makes the sender re-verify and quarantine its copy. |
| Inbox event (Activator) | that event and its causal descendants (shown as `blocked`) | unrelated events, pending accounting, other branches | Transient: retry 1 s → 5 min. Validation: quarantined, plus `revalidate`. |
| Exchange (peer, repo, op) (scheduler slot) | that key | other repos on the same connection; the connection | per-key backoff by class; Contention is not counted |
| Connection (session I/O thread) | its in-flight requests (uncertain, never replayed) | other lanes and peers | reconnect 1 s → 60 s with jitter; reset after 30 s healthy |
| Peer (PeerSupervisor) | that peer | other peers, inbound sessions, folders | Backoff, Incompatible, Unauthorized states |
| Mount (MountController) | file access for that folder | sync, status, graceful stop (it refuses only while files are open), other folders | attach retry, or NeedsUserAction |
| Worker process (FolderController) | that folder | API latency, other folders, other repos | restart with backoff; circuit breaker to Failed |
| Catalog (Catalog actor) | management writes | running workers, lanes, inbound, status (they keep the last snapshot) | retry; daemon-level ErrorRecord |
| Status/GUI (StatusHub) | display freshness | any producer | none needed |
| Daemon | everything until restart | running workers and their mounts (adopted on restart) | restart, then adoption |

### 2.4 End-state module map

```
src/lib.rs            pub mod backend, bulk_sync, core, mount, orchestrator, paired_sync, pairing_code,
                      runtime, staging, startup, ws, wire(priv), model, util, session, worker, ui_model
src/model/            mod, errors, health, ipc, status, protocol, orchestrator      (W0 contracts)
src/util/             mod, snapshot, notify, cancel, backoff, lock, log, time, fault, hmac (W0)
src/session/          mod, frame, io, conn, handshake, auth                         (peer & loopback v3 sessions)
src/core.rs + core/   snapshot, verify, objects, demand, status, inbox, commit, derive, sharing, transfer
src/runtime/          mod, ctx, engine, control, server, published, sync_rpc, objects_rpc, content, notify, export
src/worker/           mod, record, heartbeat, mount_ctl, activator, status_cell, maintenance
src/orchestrator/     mod, config, api, manage, manage_v2, run, ctx, shutdown, catalog, published, folder, registry,
                      process, worker_client, hub, hub_rules, peer, scheduler, meta_exchange, objects,
                      inbound, inbound_objects, compat, diagnostics
                      (catalog_db, supervisor, lanes, peering, status(legacy), legacy_bridge: deleted
                       in WP2.INT / WP3.1)
src/ui_model.rs       GUI view model (cross-platform, tested)
src/gui.rs, ui/app.slint   Slint glue (Windows)
src/paired_sync.rs, src/bulk_sync.rs   legacy v6 in-process replica API (test surface; no network use after WP3.1)
```

### 2.5 Threads

**Orchestrator**

| Thread | Owns / does |
|---|---|
| main | Builds Ctx and waits on `shutdown`. Runs the shutdown sequence (§3.6). Removes `orchestrator.json` through a Drop guard. |
| `tkfs-catalog` | Only connection to `folders.sqlite`. Publishes `Snapshot<CatalogSnapshot>`. |
| `control-accept` | Blocking accept on the loopback control listener. One thread per connection (cap 64). Persistent request loop. |
| `net-accept` | Blocking accept on the peer listener. Pre-auth slots ≤ 32; sessions ≤ 128 (WP2.8). |
| `folder-<state>` | FolderController mailbox loop, plus helper threads: stdout reader, exit waiter, watch loop. |
| `peer-<installation>` | PeerSupervisor: lane dialing, catalog client. |
| `sess-<id>` | One session I/O thread per connection (§6.2). A handler pool per server session. |
| `ctl-<peer>` and `bulk-<peer>-<i>` | MetaScheduler on lane 0; bulk lane loop on each bulk connection. |
| `hub-tick` | 1 Hz: rates, ETA, stall detection. |

**Worker**

| Thread | Owns / does |
|---|---|
| main | Startup sequence (§4.1), then waits on `stopping`. |
| `rpc-accept` | Loopback listener. Never exits on accept errors; 50 consecutive errors → exit 70. |
| `rpc-conn-*` | JSON RPC per connection. From Wave 3, also v3 loopback sessions. |
| `activator` | Engine lock in slices (§4.4). |
| `maintenance` | 1 Hz: pending-save retry, scrubber, status cell. |
| `heartbeat` | 1 Hz: `Snapshot<WorkerHeartbeat>`. Never takes the engine lock. |
| `mount-ctl` | Serialized attach/detach. |
| `notifier` (Windows) | Bounded, coalesced change notifications; runs only while attached. |
| FS callbacks | WinFsp/FUSE threads; short engine holds. |

---

## 3. Orchestrator design

### 3.1 Context and seams (`model::orchestrator`, frozen in W0)

Every component receives `Arc<Ctx>`:

```rust
pub struct Ctx {
    pub installation: String,
    pub config: Arc<crate::orchestrator::Config>,
    pub catalog: Arc<dyn CatalogApi>,
    pub folders: Arc<dyn FolderDirectory>,
    pub published: Arc<dyn PublishedCatalogApi>,
    pub status: Arc<dyn StatusSink>,
    pub status_source: Arc<dyn StatusSource>,
    pub network: std::sync::OnceLock<Arc<dyn NetworkApi>>,       // legacy impl (W2) → v3 impl (W3)
    pub legacy: std::sync::OnceLock<Arc<dyn LegacyNetworkView>>, // legacy LinkStatus (W2) → hub projection (W3)
    pub clock: Arc<dyn crate::util::time::Clock>,
    pub shutdown: crate::util::cancel::Cancel,
    pub stopping: Arc<std::sync::atomic::AtomicBool>,
    pub started_at_ms: u64,
    pub endpoints: crate::util::snapshot::Snapshot<DaemonEndpoints>, // control/network URLs, log path
}
impl Ctx {
    pub fn network(&self) -> Result<&Arc<dyn NetworkApi>, ErrorWire>;       // NETWORK_NOT_READY
    pub fn legacy(&self) -> Result<&Arc<dyn LegacyNetworkView>, ErrorWire>;
}
```

The trait definitions are in §14 under WP0.4: `CatalogApi`, `FolderDirectory`, `PublishedCatalogApi`, `StatusSink`, `StatusSource`, `WorkerClient`, `WorkerLauncher`, `WorkerProcess`, `NetworkApi`, `LegacyNetworkView`.

### 3.2 Catalog actor (WP1.1)

- **Ownership:** thread `tkfs-catalog` acquires `orchestrator.lock`, then opens `folders.sqlite` with `journal_mode=WAL; synchronous=FULL`.
- **Snapshot:** `CatalogSnapshot {generation, installation, folders, peers, verifier, verifier_generation, gates, peer_meta}`, indexed by state, repo, endpoint and installation.
- **Commands:** `apply(CatalogCmd)` runs each command in one transaction, with `catch_unwind` per command and 3 retries on `SQLITE_BUSY`. The last good snapshot is kept on failure, and `CATALOG_UNAVAILABLE` is returned if the thread has died. The caller waits at most `APPLY_WAIT` (5 s): a command that has not started by then is withdrawn (`CATALOG_UNAVAILABLE`, never applied), one that is running answers `REQUEST_TIMEOUT` (it may still be applied), and while one command has run that long new ones are refused at once. A slow catalog write therefore never parks a folder controller or a control connection (§2.3 Catalog row; WP2.INT post-integration re-review).
- **Side tables** (§9): `folder_intents`, `mount_gates`, `peer_meta`, and the settings key `verifier_generation`. Existing tables are never ALTERed.
- **Gate marker durability:** new code writes **both** `initial_sync` (legacy, positional `VALUES(?,?)`) and `mount_gates`, and deletes both after a successful attach. A downgrade therefore still sees the gate.
- **Mount validation:** `validate_mount_path(snapshot, data_dir, state_dir, requested)` is pure. It keeps today's rules: absolute path; parent created; no overlap with data, state or other mounts; the path must not exist.

### 3.3 FolderController (WP1.2), process/adoption (WP1.3)

There is one thread per folder. Its mailbox `Msg` enum covers:

- `Cmd(FolderCmd, reply)`;
- `Process(ProcessEvent)` (pushed by the launcher callback);
- `Heartbeat(Arc<WorkerHeartbeat>)` and `WatchFailed(CallError)` (pushed by the watch thread);
- `MountResult(...)`;
- `Tick`.

It loops `recv_timeout(next_deadline)` and publishes `Snapshot<FolderView>`.

**Phases (`model::orchestrator::FolderPhase`)**

| Phase | Entered when | Leaves to |
|---|---|---|
| `Stopped` | desired=0 and no process | `Starting` on Start, Retry or CatalogChanged with desired=1 |
| `Starting{since_ms, attempt, phase, progress_seq, adopting}` | spawn, or adoption probe | `Running` on a valid Ready line (repo, device, ipc=3) or a matching adopted hello. `Stopping(kill)` if no `startup_progress` for 30 s (`startup::Watchdog`). `Backoff` on exit or spawn failure. |
| `Running{since_ms, instance, pid, address, adopted}` | Ready, or adopted | `Stopping` (Stop, Shutdown, Unresponsive); `Backoff` (exit) |
| `StopBlocked{since_ms, reason, open_handles, unsaved}` | graceful stop answered `BUSY_VIEW` | `Stopping{force}` on ForceStop; retry graceful every 2 s; `Running` on Start (cancels the stop) |
| `Stopping{since_ms, stage: Requested\|AwaitExit\|Killing, deadline_ms, force}` | Stop or Shutdown | `Stopped`; `Backoff` (crashed while stopping) |
| `Backoff{until_ms, attempt, last_exit}` | crash, start failure, Unresponsive | `Starting` when the timer fires and desired=1. Delay is 1 s · 2^(attempt-1), cap 300 s, ±20 % jitter. The attempt count resets after 10 min Running. |
| `Failed{error}` | breaker (5 failures in 10 min) → `CRASH_LOOP`; FatalLine with class NeedsUserAction or Fatal; `PREVIOUS_WORKER_UNRESPONSIVE` | `Starting` only on Retry, Start or a catalog change for this folder |

**Stop ladder.** `desired=0` is persisted first (`CatalogCmd::SetDesired`), then:

1. `stop{mode:graceful}` with a 5 s deadline. A `BUSY_VIEW` reply moves to `StopBlocked`.
2. ForceStop (a user action, or Shutdown is **not** it) sends `stop{mode:force}`: the worker persists dirty sessions, force-detaches, and exits.
3. Wait 10 s for exit.
4. Kill a verified pid, then reap.

A worker in `Starting` (no RPC served yet) is killed directly. Store recovery is crash-safe.

**Shutdown{deadline}.**
- Running folders get a graceful stop.
- `BUSY_VIEW`, or no answer within the deadline, gives outcome `left_running` (the worker is adopted at the next start, protecting dirty sessions per invariant 8).
- `Starting` folders are killed.
- `desired_running` is never changed.

**Liveness.**
- A dedicated `JsonWorkerClient` connection loops `watch{after_seq, wait_ms:2000}` with a 4 s deadline.
- No heartbeat for 8 s, or 3 consecutive watch failures, triggers a `hello` probe (2 s). If the probe fails: `Unresponsive`, then verified kill, then `Backoff`.
- `engine.stalled` (engine lock held > 120 s) marks the folder Degraded, with no automatic kill.
- Spawned children report exit immediately through the exit-waiter thread. Adopted workers get a 1 Hz pid probe.

**Adoption** (at controller start whatever `desired`, and at every start; CCR-WP2.INT-2, ratified in WP2.INT):
1. Read `STATE/worker.json`.
2. Check the pid is alive and that `process_start` and `executable` equal the record (verified via OS APIs, §14 WP1.3). This probe comes before any `maximum_running` accounting, and adopting a live worker always takes a slot, beyond the maximum if need be (the worker already runs).
3. `hello` (2 s): repo, device and instance must match.
   - Version equal: `Running{adopted}`. With `desired=0` (a stop that was StopBlocked when the daemon ended, a Shutdown that left the worker running), the stop ladder then runs again (graceful, StopBlocked with retries, ForceStop).
   - Version differs: graceful stop, wait for `owner.lock` to free, then spawn.
   - Alive but silent: retry for 30 s, then `Failed{PREVIOUS_WORKER_UNRESPONSIVE}` (the user can ForceStop). A graceful Stop or Restart sent before `hello` answered follows the same rule: a failed stop is retried, never followed by a probe and a kill.

A wait for `owner.lock` is not a worker failure (nothing ran, nothing crashed): it never counts toward the breaker. Past 10 s the folder shows `PREVIOUS_WORKER_STOP_TIMEOUT` and keeps waiting in `Starting`; a `worker.json` published meanwhile (an orphan that finished its recovery) is adopted.

Legacy orphans (a `runtime.json` without `worker.json`) keep today's path: status RPC, stop, wait for the lock, then spawn (`orchestrator.rs:500-518`). A spawn never happens while `owner.lock` is held by a live process (invariant 23).

Workers are not tied to daemon lifetime: no Job Object, no PDEATHSIG. A `--supervised` worker with no `hello`/`watch` contact for 10 min (`TKFS_UNSUPERVISED_EXIT_S`) stops itself gracefully if quiet.

**Mount and gate.** When `Running`, `mount_enabled`, a mount path is set, and the gate is `None|Satisfied|Overridden`, the controller sends `mount-attach{path}` (60 s deadline).

| Attach result | Controller action |
|---|---|
| `Attached` | Delete the gate marker (`CatalogCmd::ClearGate`). A folder with no mount requested (no path, or `mount_enabled` off) deletes it once the gate is `Satisfied` or `Overridden` (the legacy rule; a mount configured later attaches at once; CCR-WP2.INT-2). |
| `AttachFailed`, class NeedsUserAction | Wait for an Attach action or ConfigureMount. |
| `AttachFailed`, class Transient | Retry 2 s → 120 s. |

Gate states:

- `Awaiting{source, since_ms, waiting_on}`: holds the attach.
- `Blocked{reason}`: the source is caught up, but blocked, quarantined or rejected rows from it exist. This needs user action: Revalidate or MountNow.

`GateEvidence{installation, at_ms, caught_up, waiting_on}` arrives from the exchange that involves the source:

| Wave | Evidence comes from | Rule |
|---|---|---|
| 2 | legacy lanes | legacy rule: v6 `metadata_caught_up` within 15 s |
| 3+ | meta exchanges (§6.3.6) | new rule below |

The new gate rule, in full. The source installation is `mount_gates.source_installation`. If that peer is unpaired, the controller re-targets to any peer sharing the repo (`CatalogCmd::RetargetGate`). The gate is satisfied when all of these hold:
- the local cursor of the source feed is at the source tip, with `more = false`;
- there are no inbox rows sourced from it in any state (ready, waiting, retry, blocked, quarantined);
- there are no `feed_rejected` rows from it;
- the source's reported `pending_active == 0`.

The gate never mounts before it is satisfied or overridden (invariant 26).

The controller's own catalog writes that follow an event (`ClearGate`, `RetargetGate`) run on a helper thread and come back as a message: the actor never waits for the catalog (§2.3 Catalog row).

**Published catalog.**
- On Ready, whenever heartbeat `generations.branches` changes, and every 60 s while the entry is `unavailable`, the controller calls `published-summary` (3 s) and then `PublishedCatalogApi::update`.
- The feed tip and content counters are refreshed from every heartbeat without an RPC.
- A failure marks only that entry `unavailable{error}`.

**Status.** The controller emits `StatusEvent::FolderLifecycle(view)` on every transition and `StatusEvent::WorkerHeartbeat` on each heartbeat.

### 3.4 StatusHub (WP1.11)

- A passive `Mutex<HubState>`. `record()` does in-memory work only (p99 < 50 µs).
- `snapshot()` returns an `Arc<StatusSnapshot>` rebuilt lazily at most 4 times per second; `wait_newer(gen, timeout)` serves `subscribe`.
- `hub-tick` (1 Hz) computes EWMA rates (10 s and 60 s), ETA, the stall detector, error display policy and the activity ring (500 entries).
- Pure derivation lives in `hub_rules.rs`: per-(folder, peer) state, folder aggregate precedence, stall, dedupe.
- It provides the legacy projection functions used after cutover (§7.3).

### 3.5 Management API v2 (WP2.1 core and legacy actions with the compatibility projection; WP2.2 new actions, ControlClient, CLI)

- **Transport:** blocking accept, one thread per connection, persistent request loop. Every request first checks `stopping`. At most 64 connections outside a request, plus as many parked inside one (a write behind a slow catalog, a long Subscribe or Stop wait), so parked requests never lock List, Inspect and Status out (WP2.INT post-integration re-review).
- **Reads** (List, Inspect, Peers, Status) are built from snapshots, FolderViews, the hub and `ctx.legacy()` with no waiting.
- **Writes:** `catalog.apply`, then `FolderCmd`. Start returns immediately (`starting=true`). Stop waits ≤ 15 s for `Stopped|StopBlocked` (`wait:false` opts out).
- **Create** runs `Store::initialize` on the request thread (no shared lock), then `CreateFolder`, then Start.
- **New actions** (§14 WP2.2): `status`, `subscribe`, `attach`, `detach`, `pause-sync`, `resume-sync`, `override-gate`, `force-stop`, `retry`, `revalidate`, `diagnostics`.
- **`ControlClient`:** one persistent socket. It never replays a mutating action after a transport error: it returns `CONNECTION_LOST` and the caller re-reads status.

### 3.6 Shutdown (WP2.1)

All triggers take the same path: the `Shutdown` action, Ctrl-C and console close (`SetConsoleCtrlHandler` FFI), and SIGINT/SIGTERM (`libc`).

1. Set `stopping`; reject new actions with `ORCHESTRATOR_STOPPING`.
2. `network.shutdown(5 s)`: Goaway to sessions, cancel lanes, join.
3. `folders.shutdown(30 s)`: parallel, per-folder outcomes (§3.3).
4. Build and **flush** the reply: `{stopped:true, folders:[{state, outcome, error}]}`.
5. Join control threads (2 s).
6. Drop guard removes `orchestrator.json`; exit code 0.

### 3.7 Outbound peer stack (WP2.6 PeerSupervisor, WP2.7 schedulers)

- **PeerManager:** one PeerSupervisor per catalog peer (keyed by installation). It reacts to catalog generation changes: Goaway, cancel, join ≤ 5 s, respawn.
- **Lanes:** lane 0 is a control session; lanes 1..N-1 are bulk sessions; pool = `max(connections, 2)`.

  Lane state machine:

  | State | Notes |
  |---|---|
  | `Idle` → `Dialing` → `Handshaking` → `Ready{since, rtt}` | normal path |
  | `Backoff{until, attempt, error}` | 1 s → 60 s, ±20 % jitter; reset after 30 s `Ready` |
  | `Incompatible{code}` | rechecked every 10 min or on catalog change |
  | `Unauthorized` | rechecked every 60 s or on code change |
  | `Closed` | |

  Dialing tries every resolved address.
- **RemoteCatalog:** `catalog.list{since}` every 2 s on lane 0, into `PeerContext.catalog: Snapshot<RemoteCatalog>`. The last good copy survives reconnects, so bulk lanes never depend on lane 0 being up (RC4 catalog gate).
- **MetaScheduler** (`scheduler::run_control`) on lane 0:
  - a due-time queue per repo, driven by:
    - local heartbeat `generations.sync` changes (via `FolderDirectory::wait_newer`);
    - remote catalog `tip` changes;
    - idle anti-entropy every `sync_interval_ms`;
    - backoff expiry.
  - Slots: ≤ 4 exchanges in flight, ≤ 1 per repo, 30 s deadline each.
  - These states are **not** failures:
    - `WaitingForWorker`: the local folder is not Running.
    - `WaitingForRemote`: the remote entry is `starting` or `unavailable`.
    - `Paused`: sync is disabled.
- **Exchange** (`meta_exchange::exchange`):
  1. worker `feed-offer` (10 s);
  2. peer `meta.exchange` (30 s);
  3. worker `feed-apply` (10 s);
  4. emit `ExchangeFinished{summary}`, plus `GateEvidence` to the folder.
- **Error policy:**
  - `CallError::Remote`/`Timeout` → per-(peer, repo, op) backoff by class.
  - `ConnectionLost` → in-flight keys are *uncertain*; the next attempt is a fresh request (invariant 16).
- **Bulk lanes** (`objects::run_bulk_lane`): interim in Wave 2 (§6.5), objects v3 in Wave 3 (§6.4).

### 3.8 Inbound server (WP2.8)

- **Limits:** blocking accept; ≤ 32 concurrent pre-auth handshakes and ≤ 128 sessions (extra connections are closed immediately). Pre-auth messages ≤ 16 KiB and 5 s.
- **`AcceptPolicy`** reads `CatalogSnapshot`: verifier, `verifier_generation`, own installation (self-pair refusal), and from Wave 4 revocation.
- **Handler pool** per session: 4 (control) or 2 (bulk) threads, queue 16. A full queue replies `BUSY` (Contention).
- **Routing:**

  | Op | Handling |
  |---|---|
  | `ping` | answered directly |
  | `catalog.list` | `PublishedCatalogApi::list()` (O(1); `unchanged:true` if `since` == generation) |
  | `meta.exchange` | requires `role=control` (else `METADATA_ON_BULK_LANE`). Route `{peer: authenticated installation, repo, local_replica: folder device, remote_replica: request.sender}`. Then worker `feed-exchange` (25 s). |
  | `objects.*` | requires `role=bulk` (else `BULK_ON_CONTROL_LANE`) and the lane check (`BULK_LANE_MISMATCH`); then `inbound_objects::handle` |
  | any, folder not Running | `REMOTE_WORKER_STARTING` |
  | any, sync paused | `FOLDER_SYNC_PAUSED` |

- **Rotation:** a `verifier_generation` change sends `GOAWAY{PAIRING_CODE_CHANGED}` to older sessions within 1 s.
- **Legacy v2 hello:** answered in legacy format with `PEER_NEEDS_UPGRADE`; `StatusEvent::InboundIncompatible` is recorded.
- **Visibility:** `InboundSession` open/close and served `ExchangeFinished{direction:inbound}` events, so the serving host has status (RC12).

---

## 4. Worker design

### 4.1 Startup (WP1.4; deferred activation in WP2.3)

1. Take `owner.lock`. If held: FatalLine `ALREADY_RUNNING`, exit 75.
2. Read the previous `worker.json` into `WorkerCtx.previous` (used for FUSE recovery).
3. `Store::open_existing_with(OpenOptions{defer_activation, verify: Changed})`, emitting `startup_progress` from this thread (invariant 28). Wave 1 uses `defer_activation=false`; Wave 2 sets it to `true`.
4. `Engine::new` (recovers pending saves).
5. Build `WorkerCtx`.
6. Start the RPC listener (`start_rpc_ctx`).
7. Write `runtime.json` (`Discovery`, unchanged) and `worker.json` (atomic temp+rename).
8. Mount:
   - **standalone or legacy** (`--mount`, without `--supervised`): `ctx.mount.attach(path)` synchronously; failures are reported, never fatal;
   - **`--supervised`**: wait for `mount-attach`.
9. Print the `ReadyLine` (a broken pipe is ignored).
10. Start the heartbeat, maintenance and (Wave 2) activator threads.
11. Wait for `stopping`; flush the stop reply (wait on a reply-flushed flag ≤ 2 s); remove `runtime.json` and `worker.json`.

Failures before Ready print a `FatalLine` and exit with 73 (store), 75 (locked), 78 (config) or 70 (internal). Store recovery always finishes before the listener opens (invariant 24).

### 4.2 Locks

| Holder | Takes the engine lock? | Notes |
|---|---|---|
| FS callbacks | yes, through `lock_engine_fs` (counts waiters) | content hydration and hashing stay outside the lock (unchanged) |
| Activator | slices of ≤ 64 events or 25 ms | yields when FS waiters > 0; slow-path derivation is off-lock from WP2.4 |
| Feed RPCs (`feed-*`), `objects.*`, `status`, `hello`, `watch` | no | per-thread `SyncStore` secondary connections; status from the cell |
| Legacy `published-*` (v6) | yes (unchanged semantics), but stage-only when the activator runs | removed from the network path at cutover |
| Control ops (mkdir, resolve, checkout, …) | yes | |
| Maintenance | brief `try_lock` | |
| Heartbeat, MountController state reads | no | |

**Poison handling** (`runtime::lock_engine`):
- In the worker (`PoisonPolicy::Exit`): `into_inner`, then `retain_all_dirty`, then a fatal log, then `exit(70)`.
- In tests (`PoisonPolicy::Panic`): panic.

### 4.3 MountController (WP1.5)

- **States** (`model::health::MountState`): `Disabled`, `Detached`, `Preflight`, `Attaching`, `Attached{since_ms}`, `Detaching`, `AttachFailed{error, attempt, next_retry_at_ms}`, `Lost{error}`.
- **Preflight** classifies `MOUNT_PATH_MUST_NOT_EXIST`, `MOUNT_OVERLAPS_*` and `WINFSP_RUNTIME_UNAVAILABLE` as NeedsUserAction; all other errors are Transient. FUSE runs `recover_managed(path, previous.instance)` only for a stale `fuse.tkfs` mount proven to be ours by the previous `worker.json` (invariant 27).
- **Graceful detach** requires quiet (no handles, no dirty sessions), else `BUSY_VIEW`.
- **Force detach**, in this order:
  1. fence (`switching=true`);
  2. wait until the in-flight callback counter reaches 0 (≤ 10 s);
  3. `retain_all_dirty()` (invariant 8);
  4. stop native;
  5. drop orphaned handles and sessions (dirty ones are already in `pending_saves`);
  6. clear the fence;
  7. state `Detached`.
- **WinFsp:** `bind(engine)` runs once (idempotent for the same `Arc`, checked by `ptr_eq`) **before** any path check. `attach(path)` validates the path, then calls `tk_mount_start` (a fresh `FSP_FILE_SYSTEM` per call is already supported by `native/mount.c`).
- **Mount errors never write `Engine.health`.** The close-path health write at `mount.rs:339-347` becomes a mount-scoped error counter plus a rate-limited log. A checkout remount failure becomes `Lost`.
- **FUSE:** `start_managed(engine, path, instance)`; a dispatch thread death sets `Lost`.

### 4.4 Inbox and activation (WP1.6, WP2.3, WP2.4)

- **States** (in `incoming_events`):

  | State | Meaning |
  |---|---|
  | `ready` | can be tried now |
  | `waiting` | missing parents, recorded in `incoming_waits` |
  | `retry` | transient error; `retry_at_ms` |
  | `quarantined` | validation error; `code` |
  | `blocked` | a quarantined ancestor; `blocked_on` |
  | NULL | legacy row, classified lazily |

  The legacy `error` column stays non-NULL for `quarantined` **and** `blocked`, so the legacy `incoming_quarantined` count and the old binary's drain are unchanged.
- **`stage_incoming`** works on any connection. Staging never activates.
- **`activate_slice(budget)`:**
  - pops `ready` and due `retry` rows in `(received_ms, rowid)` order, using point lookups only;
  - fast paths: content, creation, namespace — the last two now also for remote events (§5.1);
  - at most one slow-path event per slice;
  - moves children of committed events from `waiting` to `ready`;
  - the budget is measured **after** the queue load (fixes `core.rs:2352`).
- **Errors:** `SQLITE_BUSY`/`LOCKED`/IO become `retry` (1 s → 5 min), never quarantined. Validation errors become `quarantined`, and their waiting descendants become `blocked`. `revalidate(ids)` re-queues.
- **Accounting:** `pending_active = ready + waiting + retry + unclassified`. Quarantined and blocked rows are reported separately.
- **Activator** (WP2.3):
  - loop: `wait(activation Notify | 1 s)` → `lock_engine` → `activate_slice{64, 25 ms}` → unlock → `Engine::after_activation(ids)` (refresh only sessions whose content revision changed; queue notifications only when attached) → status cell refresh → `generations.sync += n`;
  - a slow event yields a `SlowTicket`; `prepare_slow` runs **off-lock** on a dedicated read store; `commit_prepared` runs under the lock;
  - `Stale` → re-queue with backoff. After 4 consecutive `Stale` outcomes the event is committed inline, alone in its slice (`Prepared::Inline`): one O(history) hold for that event instead of unbounded starvation behind a local writer that commits more often than a preparation takes (CCR-WP2.INT-1, ratified in WP2.INT). Starvation is reported in `activation.starved_since_ms`: the first `Stale` time of the oldest event still starving, cleared only when that event commits, is quarantined or leaves the inbox.
  - The commit of a prepared ticket and the re-queues around it never wait for another connection's writer under the engine lock (up to 2 s of busy attempts, as a slice): the preparation is handed back and the commit retried.

### 4.5 Status cell and heartbeat (WP1.4 basic, WP2.3 full)

- **`WorkerCtx.status_cell`** (a `Snapshot<Value>` with the legacy status JSON plus typed fields) is refreshed:
  - synchronously by mutating RPC paths (read-your-writes);
  - after every activator slice;
  - by maintenance at 1 Hz (`content_counters`, `inbox_counts`, per-peer feed counters).
- The **`status` RPC** returns the cell without any lock. It never sets `busy`/`stale` unless the cell is older than 5 s.
- **`WorkerHeartbeat`** (§6.1) is published at 1 Hz by the heartbeat thread from atomics, the cell and `MountControl::status()`. `watch` returns as soon as `seq > after_seq`.

---

## 5. Core store changes

### 5.1 Remote fast paths (WP1.6)

- `creation_successor` and `namespace_successor` drop the `event.device != self.device` restriction (`core.rs:1306`, `:1412`) unless `TKFS_REMOTE_FASTPATH=0`.
- The global gate `!self.conflicts()?.is_empty()` (`:1349`, `:1450`) reads the **active** branch only (`core.rs:1932-1946`). It is replaced by `branch_has_unresolved_conflicts(&event.branch.id)`, which is `EXISTS(SELECT 1 FROM conflicts WHERE branch=? AND resolved_by IS NULL)`.
- All other preconditions stay: `dependencies == frontier` (sorted list equality); fresh entity or free slot; a single head parented by the event; a live parent directory; no live children on delete; no directory cycle; projection `pending` and `conflicts` empty.
- **Why this is a restriction of full mode:** with `deps == frontier`, every committed event of the branch is an ancestor of E.
  - The only antichain containing E is `{E}`, so the only new cut is the current projection plus E. The successor checks reject any slot collision, missing parent or cycle in that cut.
  - Location pairs and register pairs against E are all ancestor-related.
  - E is not a review.
- **If the oracle finds a mismatch**, the gate falls back to "any conflict row on **this** branch" and the oracle re-runs.
- **Snapshot suffix replay** (`snapshot.rs:266-276`) reuses the same successors, but still only for the active branch (unchanged). The forced seal after slow-path commits (`core.rs:1159`) is **kept**.

### 5.2 Append-mode conflict derivation (WP1.7)

`persist_projection(tx, branch, p, events, Some(E))`. Mode comes from `TKFS_CONFLICT_MODE` (`append` default, `full`, `verify`).

1. **Location pairs:** only pairs that include E's location changes. Old pairs have unchanged ancestry, because E is not an ancestor of any committed event.
2. **Reviewed cuts:** only E's own cut, and only if E is a review.
3. **Register pairs:** unchanged (today's code already does new/old pairs only, `core.rs:2979-3003`).
4. **`namespace_history`:**
   - (a) Compute `cones_before` = `NamespaceCones::new(events \ {E}).members()` and `cones_after`.
   - (b) Touched entities are those whose `kind`, `location` or `alive` E changes. Touched cones are the `cones_after` members containing a touched entity.
   - (c) **Exactness precondition:**
     - `cones_before ⊆ cones_after` (as member sets);
     - for every touched cone `C ∈ cones_before`: `relevant_after(C) \ {E} == relevant_before(C)`, both computed with the same relevance plus TRUE-presence compression routine (`core.rs:3318-3394`).

     If the precondition fails, run **full** mode, including the DELETE of non-rederived namespace rows (`core.rs:3037-3048`).
   - (d) Under the precondition:
     - For each touched old cone, enumerate all antichains of `relevant_after(C)` **for counting** (`NAMESPACE_HISTORY_LIMIT`, identical to full mode), and call `project(cut)` only for antichains containing E.
     - For each new cone (in `cones_after \ cones_before`), derive fully (project every antichain).
     - Untouched old cones are skipped.
     - No DELETE is needed: every old antichain still exists with an identical cut, so the old derived set is a subset of the new one.
5. **Memoization:** `FactGraph::closure` results are memoized as bitsets per call, in both modes.
6. **`verify` mode** runs append and full in separate savepoints and returns `CONFLICT_MODE_MISMATCH` on any difference in conflict rows or `resolved_by`. This is used by the fuzzer and the qualification run.
7. **Proof obligation:** the induction argument is documented in code comments. The oracle (`verify_derived_state`) is the executable check. Clock-based incremental derivation is **not** in scope.

### 5.3 Indexes, markers and per-object outcomes (WP1.8)

- **`object_availability(hash PK, size, state, mtime_ns, verified_ms)`**, with state 0 missing, 2 available, 3 corrupt. It is maintained by `put_object`, `put_stream`, install, `register_received`, quarantine renames and verify failures, and heals on read paths (`object_available_checked`).
- **`event_objects(hash, event, shared, PK(hash, event))`** is maintained by `AFTER INSERT`/`DELETE` triggers on `events`, using `json_each` over `$.changes` where `field='content'`. Like `journal_sequence`, this also covers writes by older binaries.
- **`staged_objects(hash, event, PK)`** is maintained by triggers on `incoming_events` insert and delete.
- **`object_error_state(hash PK, code, class, attempts, retry_at_ms, last_ms)`** is a side table. The positional `object_errors` table and its writers are untouched.
- **Backfills** are one-time, chunked (10K rows per transaction), resumable through config markers `availability_v2` and `event_objects_v1`, and emit `startup_progress` per chunk.
- **Startup** (`VerifyMode::Changed`): an object is trusted if its row matches size and mtime. Otherwise it is hashed once (as today) and the marker recorded. A one-time full verify happens after upgrade. `scrub_step(budget_bytes)` is the background scrubber API.
- **Per-object outcomes:**
  - `receive_parts_outcomes` returns one `PartOutcome` per object;
  - the old `receive_parts_data_locked` stays as a wrapper;
  - availability is re-checked after taking the object lock;
  - no spool is created for `offset > 0` without an existing spool;
  - the redundant second `sync_all` is removed;
  - completed objects in a page are registered even if a later part fails.
- **`bulk_sync` (v6 wire unchanged):** part cap ≤ 4,096 per page, with correct overhead accounting (fixes the > 10,031 tiny-object cliff, `core.rs:2702`). The sender skips objects inside their `object_error_state` backoff.

### 5.4 Feed tables (WP2.5): see §6.3

### 5.5 Spool state and objects v3 (WP3.2): see §6.4

---

## 6. Protocols

### 6.1 Worker IPC v3 (`model::ipc`; orchestrator ↔ worker, same binary)

**stdout lines** (one JSON object per line):

```json
{"startup_progress": 12, "phase": "verifying available objects"}
{"ready": true, "repo": "...", "device": "...", "runtime": "STATE/runtime.json", "mounted": false,
 "mount_error": {"code":"MOUNT_PATH_MUST_NOT_EXIST","class":"needs_user_action","message":"..."},
 "instance": "uuid", "pid": 1234, "address": "ws://127.0.0.1:5555", "version": "0.1.0+6298d0b", "ipc": 3}
{"fatal": true, "code": "STORE_IDENTITY_MISMATCH", "class": "fatal", "message": "...", "exit_code": 73}
```

`mounted` is now the **actual** state. The legacy keys `ready`, `repo`, `device`, `runtime` and `mounted` are kept. `ReadyLine`, `WorkerRecord` and `WorkerEndpoint` also carry `#[serde(default)] token: Option<String>`, frozen in W0 and always `None` until WP4.1. WP1.2 copies it through from Ready to `WorkerEndpoint`.

**`STATE/worker.json`** (written atomically; removed on clean exit):

```json
{"format":1,"pid":1234,"instance":"uuid","address":"ws://127.0.0.1:5555","version":"0.1.0+6298d0b",
 "ipc":3,"repo":"...","device":"...","started_at_ms":1790000000000,
 "process_start":133780000000000000,"executable":"C:\\...\\tkfs.exe","supervised":true}
```

`process_start` is the Windows `GetProcessTimes` creation FILETIME, or Linux `/proc/self/stat` field 22 (starttime ticks).

`runtime.json` (`Discovery`) is **unchanged**.

**JSON RPC ops** (`{"request": id, "payload": {"op": ..., ...}}`):

| op | request | reply | lock | wave |
|---|---|---|---|---|
| `hello` | `{}` | `HelloReply{instance,pid,repo,device,version,ipc,started_at_ms,process_start}` | none | W1 |
| `watch` | `{after_seq:u64, wait_ms:u32≤5000}` | `WorkerHeartbeat` | none | W1 |
| `stop` | `{mode:"graceful"\|"force"}` (no mode = graceful) | `{stopped:true, persisted_dirty:u32, detached:bool}`; graceful refusal = RemoteError `BUSY_VIEW: …` | brief | W1 |
| `mount-attach` | `{path}` | `MountStatus` | brief | W1 |
| `mount-detach` | `{force:bool}` | `MountStatus` | brief | W1 |
| `mount-status` | `{}` | `MountStatus` | none | W1 |
| `revalidate` | `{ids?: [String]}` | `{requeued:u64}` | yes | W1 |
| `published-summary` | `{}` | `PublishedSummary{repo,replica,label,branches,tip:{epoch,seq},sync_generation}` | none | W1 (extends `PublishedState`) |
| `status` | `{}` | legacy status JSON plus `activation`, `mount`, `store_health`, `inbox`, `heartbeat_seq` | none (W2) | W1/W2 |
| `feed-offer` | `{route}` | `MetaExchangeRequest` | none | W2 |
| `feed-exchange` | `{route, request: MetaExchangeRequest}` | `{reply: MetaExchangeReply, local: FeedApplyResult}` | none | W2 |
| `feed-apply` | `{route, request_id, reply: MetaExchangeReply}` | `FeedApplyResult` | none | W2 |
| `objects.*` | §6.4 (v3 loopback session, binary bodies) | | none | W3 |
| existing ops | unchanged (`published-*`, `folder-details`, `state`, `events`, CLI ops) | | | |

### 6.2 Peer protocol v3: sessions (`src/session`, WP1.9 and WP1.10)

**Upgrade:**
- The client sends `X-Tkfs-Sync: 3` on the WebSocket upgrade, and v3 servers echo it.
- **Client facing an old server:** the echo is missing, so the client returns `DialError::NeedsUpgrade`. The code is never sent; the lane becomes `Incompatible{PEER_NEEDS_UPGRADE}`.
- **Old client facing a v3 server:** the header is missing. The server accepts the upgrade, reads one legacy JSON message (≤ 16 KiB, 5 s), replies `{"ok":false,"error":"PEER_NEEDS_UPGRADE: remote runs tkfs peer protocol 3; upgrade this orchestrator"}` as text JSON, records `InboundIncompatible`, and closes.

**Frame** (exactly one per WebSocket binary message):

```
off len  field
0   1    kind: 1 HELLO 2 HELLO_ACK 3 AUTH 4 AUTH_OK 5 REQUEST 6 RESPONSE 7 CHUNK 8 PROGRESS 9 CANCEL 10 HEARTBEAT 11 GOAWAY
1   1    flags: bit0 MORE (body continues in CHUNK frames of the same id), bit1 LAST (CHUNK only)
2   2    reserved = 0
4   4    header_len u32 LE   (≤ 16 KiB pre-auth, ≤ 64 KiB after)
8   n    header: UTF-8 JSON object (serde types in model::protocol::frame)
8+n m    body: raw bytes (≤ 1 MiB; 0 for HELLO/AUTH/HEARTBEAT/GOAWAY/CANCEL/PROGRESS)
```

- Bodies larger than 1 MiB are sent as REQUEST/RESPONSE with `MORE`, followed by CHUNK frames, and reassembled up to the per-op `max_body`: 16 MiB for `meta.exchange` and the interim `objects.exchange` (CCR-WP2.7-1 / CCR-WP2.8-1, applied in WP2.INT), 1 MiB for others.
- Streaming ops (`objects.pull` responses, `objects.push` requests) deliver CHUNK frames to callbacks instead of reassembling them.
- Trailing bytes, unknown kinds, oversize headers and bodies, and a non-zero reserved field are protocol errors and close the connection.

**Handshake** (blocking; socket read/write timeouts of 5 s apply **only** here, and any timeout closes the socket):
1. → `HELLO`: `ClientHello{protocol:"tkfs-peer", versions:[3], installation, role: control|bulk|probe, lane?:{index,count}, pool, session: uuid, client_nonce: 32-byte hex, features:[]}`.
2. ← `HELLO_ACK`: `ServerHello{protocol, version:3, installation, role, lane, salt, server_nonce, verifier_generation, features, limits{max_header, max_chunk, max_body}}`, or `GOAWAY{CANNOT_PAIR_WITH_SELF|PAIRING_CODE_NOT_SET|TOO_MANY_SESSIONS|PEER_REVOKED|PEER_PROTOCOL_UNSUPPORTED}`.
3. → `AUTH{proof}`, where `proof = hex(HMAC-SHA256(K, "client" ‖ T))`.
   - `K` is the 32-byte verifier digest. The server stores it as `Verifier.hash`; the client derives it with `Verifier::derive_key(salt, code)`.
   - `T = "tkfs-peer-auth-v3" ‖ 0 ‖ client_inst ‖ 0 ‖ server_inst ‖ 0 ‖ role ‖ 0 ‖ lane_index u32 ‖ lane_count u32 ‖ session ‖ client_nonce ‖ server_nonce ‖ verifier_generation u64`.
4. ← `AUTH_OK{server_proof = hex(HMAC(K, "server" ‖ T))}`. The client verifies it (mutual authentication), or receives `GOAWAY{INVALID_PAIRING_CODE}`.
5. The client checks `server.installation` equals the catalog's value (`PEER_INSTALLATION_CHANGED`).
6. Both sides switch the socket to non-blocking and start the I/O thread.

The code never crosses the wire, and replaying a captured proof fails because the nonce differs.

**Loopback sessions** (orchestrator ↔ worker, Wave 3): the upgrade header `X-Tkfs-Sync: 3-loopback`; no HELLO or AUTH until WP4.1 adds the `X-Tkfs-Token` check.

**I/O model:**
- One I/O thread per connection owns the `WebSocket<MaybeTlsStream<TcpStream>>` in non-blocking mode, plus a `polling::Poller`.
- Loop: `wait(timeout = min(heartbeat due, 250 ms deadline scan))`; read until `WouldBlock` (**always drained**, because TLS buffers internally); dispatch; write from priority queues while tungstenite's buffer is < 256 KiB; `flush` (`WouldBlock` arms writable interest).
- Producers enqueue and call `poller.notify()`. Only this thread reads or writes the socket, so tungstenite's automatic Pong and Close replies are safe.
- Queues:
  - control (REQUEST/RESPONSE headers, PROGRESS, CANCEL, HEARTBEAT, GOAWAY; ≤ 16 MiB);
  - response bodies;
  - data CHUNKs (≤ 32 MiB; round-robin across stream ids).

  Producers block, outside the I/O thread, when the data queue is full (backpressure).
- **Liveness:** `HEARTBEAT{seq, at_ms, echo}` after 5 s of send idleness; the connection is dead after 20 s with no received frame. RTT comes from the echo.
- **Requests:** client-allocated u64 ids; only the client originates REQUEST, which keeps the server passive.
- **Deadlines:**
  - absolute `deadline` per call;
  - optional `idle` for streaming;
  - PROGRESS resets idle;
  - on expiry, send CANCEL and return `CallError::Timeout`.
- A **Remote error** (RESPONSE `ok:false`) leaves the connection usable (invariant 17).
- **Goaway:** no new requests; in-flight requests complete; close after idle or 5 s.

**Peer ops and deadlines:**

| op | role | deadline | request | reply |
|---|---|---|---|---|
| `ping` | any | 5 s | `{}` | `{alive:true}` |
| `catalog.list` | control/probe | 10 s | `CatalogListRequest{since}` | `CatalogListReply{generation, unchanged, entries:[PublishedEntry]}` |
| `meta.exchange` | control | 30 s | header `{repo}` + body `MetaExchangeRequest` | `MetaExchangeReply` |
| `objects.exchange` (interim W2–W3) | bulk | 60 s | `bulk_sync::Page` (v6 JSON) | `bulk_sync::Page` |
| `objects.pull` | bulk | idle 30 s | `ObjectsPull` | CHUNK stream + `ObjectsResult` |
| `objects.wants` | bulk | 10 s | `ObjectsWantsRequest` | `[WantItem]` |
| `objects.push` | bulk | idle 30 s | `ObjectsPush` header + CHUNK stream | `ObjectsResult` |
| `objects.report` | bulk | 10 s | `ObjectsReport{repo, sender, hash, code}` | `{}` |

**Framing** (CCR-WP2.6-1, CCR-WP2.7-2, CCR-WP2.8-2; ratified in WP2.INT). Readers parse a non-empty body before the header.
- `catalog.list`: the request is the header. The reply is the header; when it would not fit a response header (above 63 KiB) it is the JSON body, with the header `{generation, unchanged:false, in_body:true}` and no `entries`, so a header-only reader fails instead of reading an empty catalog.
- `meta.exchange`: request header `{repo}`, body the JSON `MetaExchangeRequest`; reply header `{repo}`, body the JSON `MetaExchangeReply`.
- `objects.exchange` (interim): request header `{repo, lane}`, body the JSON page (the header is accepted when the body is empty); reply header `{repo}`, body the JSON page, or the page streamed as body CHUNK frames before the RESPONSE.

### 6.3 Metadata feed, format 7 (`model::protocol::feed`, WP2.5 worker side, WP2.7 orchestrator side)

#### 6.3.1 Concepts

- **Feed of replica R:** R's committed **shared** events in local acceptance order (`journal_sequence.seq`, AUTOINCREMENT).
- **Epoch:** config `journal_epoch` (32 random hex characters). An `AFTER DELETE ON events` trigger regenerates it (`INSERT INTO config VALUES('journal_epoch', lower(hex(randomblob(16)))) ON CONFLICT(key) DO UPDATE SET value=excluded.value`).
- **Cursor:** `{epoch, seq, id}`. Start is `{epoch, 0, ""}`.
- **Cursor validation** (by the feed source S): `epoch == S.epoch && seq ≤ S.tip && (seq == 0 || journal_id_at(seq) == id)`. If the check fails, the cursor is invalid, so S's journal regressed or was replaced.
- **Digest:** the first 16 hex characters of SHA-256 of the payload text.

#### 6.3.2 Messages

```rust
pub const FEED_FORMAT: u32 = 7;
pub struct Cursor { pub epoch: String, pub seq: i64, pub id: String }
pub struct FeedEntry { pub seq: i64, pub id: String, pub digest: String,
                       #[serde(default)] pub event: Option<Event>,   // None = elided (receiver has it) or large
                       #[serde(default)] pub large: Option<u64> }    // payload bytes; fetch via `need`
pub struct FeedPage { pub epoch: String, pub after: Option<Cursor>, pub reset: bool,
                      pub entries: Vec<FeedEntry>, pub through: Cursor, pub tip: i64, pub more: bool }
pub struct FeedState { pub epoch: String, pub tip: i64, pub pending_active: u64,
                       pub blocked: u64, pub quarantined: u64 }
pub enum PageStatus { Accepted, CursorMismatch { expected: Cursor }, Reset }
pub struct RejectedEvent { pub id: String, pub seq: i64, pub code: String, pub class: ErrorClass }
pub struct MetaExchangeRequest { pub format: u32, pub request: String /*uuid*/, pub sender: String, pub receiver: String,
    pub repo: String, pub page: FeedPage,              // sender's feed after receiver's last reported cursor
    pub cursor: Option<Cursor>,                         // sender's cursor of RECEIVER's feed
    pub need: Vec<String>,                              // ≤ 256 ids wanted from receiver
    pub state: FeedState }
pub struct MetaExchangeReply { pub format: u32, pub reply_to: String, pub sender: String, pub receiver: String,
    pub repo: String, pub page_status: PageStatus, pub cursor: Cursor, // receiver's cursor of SENDER's feed (after this request)
    pub page: FeedPage,                                 // receiver's feed after request.cursor (or reset)
    pub staged: u32, pub rejected: Vec<RejectedEvent>,  // ≤ 256 shown; full set is durable in feed_rejected
    pub need_served: Vec<Event>, pub unknown: Vec<String>, pub state: FeedState }
pub struct FeedApplyResult { pub summary: ExchangeSummary, pub gate: GateView }
pub struct GateView { pub caught_up: bool, pub waiting_on: Vec<String>, pub blocked: bool }
pub struct ExchangeSummary { pub sent_events: u32, pub received_events: u32, pub staged: u32, pub rejected: u32,
    pub acked: u32, pub local_tip: i64, pub remote_tip: i64, pub cursor_in: i64, pub cursor_out: i64,
    pub to_send: u64, pub to_receive: u64, pub local_pending: u64, pub remote_pending: u64,
    pub blocked: u64, pub quarantined: u64, pub caught_up: bool, pub metadata_advanced: bool,
    pub bytes_up: u64, pub bytes_down: u64 }
```

#### 6.3.3 Sender rules (building a page from R's feed for peer P)

- Start after `feed_out(P)`, P's last reported cursor of R's feed, validated by R.
  - Unknown: send a probe page (no entries, `after: None`).
  - Invalid: `reset: true, after: Start`.
- Take entries with `seq > after.seq` in seq order: ≤ 4,096 entries, ≤ 1 MiB of payload, and ≤ 8 MiB of stored payloads read for the listed entries' digests, elided and `large` ones included (the page always lists its first entry; WP2.INT post-integration re-review).
  - The payload is elided (`event: None`) when `peer_known(P, id)`.
  - Events > 256 KiB are flagged `large`, and their payload is served only through `need` (control connection, chunked by the session layer, ≤ `MAX_EVENT` 8 MiB). This removes the 349,525-byte cliff.
- `through` is the last scanned seq, or the tip if the feed is exhausted. `more` means the feed is not exhausted.

#### 6.3.4 Receiver rules (staging a page from P)

- **Accept** if `page.reset`, or if `page.after == feed_in(P)` exactly (with a new peer, `feed_in` is Start). Otherwise reply `CursorMismatch{expected: feed_in(P)}` and stage nothing.
- **Reset:**
  1. `feed_in(P) = Start`;
  2. `DELETE FROM peer_known WHERE peer=P`;
  3. `UPDATE outbox SET acknowledged = EXISTS(SELECT 1 FROM peer_known k WHERE k.event = outbox.event)` (re-queue withdrawn events, porting `core.rs:2173-2186`);
  4. then accept.
- **Entry with payload:** validate as today (the `paired_sync::receive_for_lane` rules):
  - repo;
  - shared branch (`PRIVATE_BRANCH_REJECTED`);
  - `FORGED_EVENT_ORIGIN` (if `event.device == local device`, a committed identical payload must exist);
  - `EVENT_ID_REUSED` (committed or staged payload differs);
  - size bounds;
  - the `object_sizes` rules;
  - the digest matches the payload.

  If valid, `stage_incoming` with source `(P, seq)`. If invalid, insert a `feed_rejected(peer, event, seq, code, payload, at_ms)` row. Rejected events are never acked.
- **Elided entry:**
  - If we hold `id` committed or staged with an equal digest: OK.
  - If we hold it with a different digest: `feed_rejected{EVENT_ID_REUSED}`.
  - If we don't hold it: insert into `feed_needs(P, id)`.
- **`peer_known` and acks:** for every entry, payload or elided, whose digest equals our committed or staged payload, `INSERT OR IGNORE peer_known(P, id)`, and if it is committed locally, `UPDATE outbox SET acknowledged=1 WHERE event=id`. This is **per-id evidence that P committed `id`**, because P's feed contains only P's committed events. It is the same semantics as today's known-set acks (`remember_peer_inventory`), which "cover committed events only" (invariant 10). Responses stay bound by `reply_to == request` (`UNBOUND_PEER_RESPONSE`).
- **Cursor advance:** `feed_in(P) = page.through`, contiguously, in the same transaction as staging.
- **Need serving:** for `request.need`, serve committed shared events by id (total ≤ 16 MiB); unknown ids go in `unknown`. A payload that does not fit the budget is not read.
- **Regression detection by the sender:** if the reply's `CursorMismatch.expected.seq` or the reported `cursor.seq` is lower than the previously recorded `feed_out(P).seq` for the same epoch, P regressed. Then `DELETE peer_known WHERE peer=P` and recompute outbox acks. A reply's report is compared with both channels' reports (the request channel's only if it was recorded before the request was built: a request-borne cursor may be newer than a reply under mutual dialing); a request's report only with its own channel (WP2.INT post-integration re-review, CCR-WP2.INT-4).
- **Stage-time bounds:** an event whose `dependencies` or change `parents` are not valid ids (1–128 bytes) is rejected (`INVALID_EVENT`); a need list never carries such an id.

#### 6.3.5 Exchange flow (dialer D, acceptor A, D's route `{peer: A_inst, repo, local: D_replica, remote: A_replica}`)

1. **D worker `feed-offer`** builds a `MetaExchangeRequest`:
   - `page` from `feed_out(A)`;
   - `cursor = feed_in(A)`;
   - `need` from `feed_needs(A)` and `missing_parents(256)`;
   - `state`.
2. **A worker `feed-exchange`**:
   - validates the route, stages the request page per §6.3.4, and computes `reply.cursor = feed_in(D)`;
   - validates `request.cursor` against A's journal: if valid, page after it; if not, `reset:true` from Start;
   - serves the need.

   It also returns `local: FeedApplyResult`, which is A's view and gives inbound gate evidence.
3. **D worker `feed-apply`**:
   - checks `reply_to`;
   - stages `reply.page` per §6.3.4;
   - records `feed_out(A) = reply.cursor` after validating it (invalid → the next page resets);
   - applies the `CursorMismatch` or `Reset` handling;
   - stages `need_served`;
   - marks `unknown` ids in `feed_needs` as `missing_at(A)`.
4. Both workers `poke` the activator.

#### 6.3.6 Caught-up and gate view (per peer P)

- **`metadata.caught_up`:** `feed_in(P).seq == P.tip && !more` and no inbox rows sourced from P in `ready|waiting|retry|unclassified`.
- **`gate.caught_up`:** the above, plus no `blocked|quarantined` rows from P, no `feed_rejected` rows from P, and `P.state.pending_active == 0`. `GateView.caught_up` is this full gate claim, and `GateView.blocked` means blocked, quarantined or rejected rows from P exist (CCR-WP2.7-5, ratified in WP2.INT): the FolderController reads `caught_up && !blocked` as satisfied and `caught_up && blocked` as the Blocked gate. So `GateEvidence.caught_up` is `GateView.caught_up || (GateView.blocked && metadata.caught_up)`, on the dialer (`feed-apply`) and on the acceptor (`feed-exchange`'s `local`) alike (`meta_exchange::evidence_of`; WP2.INT post-integration re-review).
- **`waiting_on`:** human-readable reasons, for example `"412 events remaining at source"`, `"3 events waiting for missing parents"`, `"source has 1 unactivated event"`, `"2 events quarantined"`.

#### 6.3.7 Cost

- An idle exchange is O(1): empty pages and point lookups.
- Catch-up is O(delta).
- Echo is about 60 bytes per event (id, seq and digest), sent once.
- There are no full-set caps: `INVENTORY_TOO_LARGE` disappears from the v3 path.

### 6.4 Objects v3 (`model::protocol::objects`; WP3.2 worker side, WP3.3 orchestrator side)

**Dialer-driven, both directions on each bulk lane.** Ownership:
- `Lane::owns(hash)` shards objects across the bulk lanes of a session (invariant 19).
- Worker **leases** (`WorkerCtx.inflight_objects`: hash → `{session, expires_ms}`, 60 s TTL renewed on progress) deduplicate across sessions and peers (mutual pairing).

**Download** (D pulls from A):
1. D worker `objects.want{route, lane, max_items:256, max_bytes:16 MiB, session}` → `[WantItem{hash, size, offset: durable, priority}]`.
   - Candidates: hashes referenced by committed **or validly staged** shared events (`event_objects` ∪ `staged_objects`, which removes the RC4 activation gate), that are not shareable-available, are not leased by another session, are owned by the lane, and are not in backoff.
   - Priority: demand (`object_requests`, newest first, 24 h expiry), then current projection (from the status cell's current-object set), then history; within a class, smaller sizes first, with fair interleaving.
2. Peer `objects.pull{repo, sender, receiver, lane, items, budget_bytes:16 MiB}`.
3. A orchestrator → A worker `objects.read{route, hash, offset, len ≤ 1 MiB}` (≤ 4 in flight) → CHUNK frames `{id, hash, offset, total, last}`. The final response is `ObjectsResult{results:[{hash, outcome, bytes}]}`.
4. D orchestrator pipes chunks into D worker `objects.write` (≤ 4 in flight). When half the budget is consumed, the next pull is issued (≤ 2 pulls in flight per lane).

**Upload** (D pushes to A):
1. Peer `objects.wants{repo, sender, receiver, lane, max_items, max_bytes}` → A worker `objects.want` → `[WantItem]`.
2. D worker `objects.read` (authorization) → peer `objects.push` (CHUNK stream) → A worker `objects.write` → `ObjectsResult`.

**Worker ops** (binary bodies over the loopback session):

| op | header | body | reply header / body |
|---|---|---|---|
| `objects.want` | `{route, lane, max_items, max_bytes, session}` | — | `{items:[WantItem]}` |
| `objects.read` | `{route, hash, offset, len}` | — | `{total}` / bytes |
| `objects.write` | `{route, session, items:[{hash, offset, total, len, last}]}` | concatenated bytes | `{results:[{hash, outcome, durable_offset}]}` |
| `objects.report` | `{route, hash, code}` | — | `{action:"quarantined"\|"verified_ok"}` |

**Authorization** (invariant 13):
- `objects.read` serves only if all of these hold: `object_availability.state = available`, the hash is in `published_objects ∪ received_shared_objects`, and it is referenced by a **committed** shared event (`event_objects.shared = 1`). Otherwise it returns `NOT_PERMITTED` or `NOT_AVAILABLE`.
- `objects.write` accepts only hashes it wants (referenced by committed or validly staged shared events, and not already shareable-available), with the lease held by this session. Otherwise it returns `UNSOLICITED_OBJECT` or `IN_FLIGHT_ELSEWHERE`.
- A physically present private object **is** accepted and fully verified, then registered as `received_shared` (I4).

**Durability** (invariants 1–3, literal):
- `spool_state(hash PK, durable, total, updated_ms)`. On open, a spool longer than `durable` is truncated to `durable`. A legacy spool with no row adopts its file length once (the legacy code fsynced each append).
- An append must start at `durable`. Overlap is verified byte-for-byte, and a mismatch resets the spool (`OBJECT_PART_CHANGED`).
- After each write call: `fsync` the spool, then upsert `spool_state`. Only then is `durable_offset` returned (advertised).
- On completion: full re-read SHA-256 (whole-object check), then install (`MoveFileExW` WRITE_THROUGH, or rename plus directory fsync), then registration (sizes, `received_shared_objects`, clear requests and error state, availability). Small objects completed in one call share one registration transaction.
- A crash between install and registration is repaired by **re-transfer**: the object is not shareable, so it is still wanted.

**Results:** `ObjectOutcome = Completed | Progressed{durable_offset} | Skipped | InFlightElsewhere | NotAvailable | NotPermitted | CorruptAtSource | SizeMismatch | Unsolicited | Failed(ErrorWire)`.
- A hash mismatch on completion makes the receiver send `objects.report{CORRUPT_AT_SOURCE}`. The sender re-verifies and, if corrupt, quarantines its copy (`corrupt-<id>` rename, availability `corrupt`, error state).
- The receiver backs off that object for that source.

**Status:** `ExchangeFinished{op: objects.pull|objects.push, bytes, objects, duration}` per pull or push, plus `ObjectError` events.

### 6.5 Interim bulk path (Wave 2 → replaced in Wave 3)

`objects.exchange` carries the legacy `bulk_sync::Page` (hex parts, bulk protocol 1) over a v3 bulk session.

| Side | Owner | Handling |
|---|---|---|
| Dialer | `orchestrator/objects.rs` (WP2.7) | runs worker `published-bulk-offer`, then peer `objects.exchange`, then worker `published-bulk-apply` |
| Acceptor | `inbound_objects.rs` (WP2.8) | routes to worker `published-bulk-exchange` |

This exists so the cutover (WP3.1) is end-to-end testable in its own worktree. WP3.3 replaces both files in the same wave.

---

## 7. Status model v2, compatibility, UI

### 7.1 `model::status` (schema 2, frozen in W0)

```rust
pub const STATUS_SCHEMA: u32 = 2;
pub struct StatusSnapshot { pub schema: u32, pub generation: u64, pub at_ms: u64, pub daemon: DaemonStatus,
    pub folders: Vec<FolderStatus>, pub peers: Vec<PeerStatus>, pub activity: Vec<ActivityEntry> }
pub struct DaemonStatus { pub installation: String, pub version: String, pub pid: u32, pub started_at_ms: u64,
    pub stopping: bool, pub control: String, pub network: Option<String>, pub log_path: Option<String>,
    pub errors: Vec<ErrorRecord> }
pub struct FolderStatus { pub state: String, pub label: String, pub repo: String, pub device: String,
    pub lifecycle: LifecycleStatus, pub intents: Intents, pub mount: MountStatus, pub gate: GateStatus,
    pub store: StoreStatus, pub activation: ActivationStatus, pub content: ContentCounters,
    pub journal: JournalCounters, pub sync: FolderSync, pub errors: Vec<ErrorRecord> }
pub struct LifecycleStatus { pub phase: LifecyclePhase /* stopped|starting|running|stop_blocked|stopping|backoff|failed */,
    pub detail: Option<String>, pub since_ms: u64, pub pid: Option<u32>, pub instance: Option<String>,
    pub adopted: bool, pub restarts: u32, pub next_retry_at_ms: Option<u64>, pub last_exit: Option<ExitRecord>,
    pub degraded: bool, pub startup: Option<StartupProgress /* phase, seq */> }
pub struct Intents { pub desired_running: bool, pub sync_enabled: bool, pub mount_enabled: bool }
pub struct GateStatus { pub state: GateKind /* none|awaiting|blocked|satisfied|overridden */,
    pub source: Option<String>, pub since_ms: Option<u64>, pub waiting_on: Vec<String> }
pub struct StoreStatus { pub health: StoreHealth, pub pending_saves: u32, pub unflushed_files: u32, pub open_handles: u32 }
pub struct ActivationStatus { pub inbox: InboxCounts, pub counters: ActivationCounters }
pub struct FolderSync { pub state: FolderSyncState, pub summary: String, pub peers: Vec<FolderPeerSync> }
pub enum FolderSyncState { Stopped, NotShared, Paused, Offline, InitialSync, Metadata, Downloading, Uploading,
    Syncing, Blocked, Backoff, Stalled, NeedsAttention, UpToDate }
pub struct FolderPeerSync { pub installation: String, pub endpoint: Option<String>, pub label: Option<String>,
    pub direction: Direction /* outbound|inbound|both */, pub state: FolderPeerState,
    pub metadata: MetaProgress, pub objects_down: TransferProgress, pub objects_up: TransferProgress,
    pub last_progress_ms: Option<u64>, pub stalled_since_ms: Option<u64>, pub backoff_until_ms: Option<u64>,
    pub error: Option<ErrorRecord> }
pub enum FolderPeerState { NotShared, Offline, Connecting, Incompatible, Unauthorized, WaitingForWorker,
    WaitingForRemote, Paused, Metadata, Downloading, Uploading, Syncing, Blocked, Backoff, Stalled, UpToDate }
pub struct MetaProgress { pub to_send: u64, pub to_receive: u64, pub local_pending: u64, pub remote_pending: u64,
    pub blocked: u64, pub quarantined: u64, pub rejected: u64, pub caught_up: bool,
    pub cursor_seq: i64, pub remote_tip: i64, pub last_exchange_ms: Option<u64> }
pub struct TransferProgress { pub objects_remaining: u64, pub bytes_remaining: u64, pub in_flight: u32,
    pub rate_bps: u64, pub eta_s: Option<u64>, pub total_bytes: u64, pub last_progress_ms: Option<u64> }
pub struct PeerStatus { pub installation: String, pub endpoint: Option<String>, pub label: Option<String>,
    pub direction: Direction, pub state: PeerConnState /* online|connecting|offline|backoff|incompatible|unauthorized|revoked */,
    pub protocol: Option<String>, pub since_ms: Option<u64>, pub rtt_ms: Option<u64>,
    pub rate_up_bps: u64, pub rate_down_bps: u64, pub lanes: Vec<LaneStatus>, pub inbound_sessions: u32,
    pub shared_folders: Vec<String>, pub error: Option<ErrorRecord> }
pub struct LaneStatus { pub lane: u32, pub role: LaneRole, pub direction: Direction, pub state: LaneState,
    pub since_ms: Option<u64>, pub inflight: Vec<InflightOp /* op, repo, since_ms, bytes */>,
    pub bytes_up: u64, pub bytes_down: u64, pub rate_up_bps: u64, pub rate_down_bps: u64,
    pub reconnects: u32, pub backoff_until_ms: Option<u64>, pub error: Option<ErrorRecord> }
pub struct ActivityEntry { pub at_ms: u64, pub kind: String, pub folder: Option<String>, pub peer: Option<String>, pub message: String }
pub enum StatusEvent {
    FolderLifecycle(Box<FolderView>), WorkerHeartbeat { state: String, hb: Arc<WorkerHeartbeat> }, FolderRemoved { state: String },
    PeerConfigured { installation: String, endpoint: String,
                     #[serde(default)] display_name: Option<String> },        // CCR-WP1.11-1 (WP2.INT)
    PeerRemoved { installation: String },
    LaneState { installation: String, lane: u32, role: LaneRole, direction: Direction, state: LaneState,
                error: Option<ErrorWire>, backoff_until_ms: Option<u64>, rtt_ms: Option<u64>,
                #[serde(default)] protocol: Option<String> },              // CCR-WP1.11-2 (WP2.INT): tkfs-peer/N while Ready
    ExchangeStarted { installation: String, repo: String, lane: u32, op: String, at_ms: u64,
                      #[serde(default)] direction: Direction },            // CCR-WP2.8-3 (WP2.INT): inbound starts on the inbound lane
    ExchangeFinished { installation: String, repo: String, lane: u32, op: String, direction: Direction, ok: bool,
                       error: Option<ErrorWire>, summary: Option<ExchangeSummary>, bytes_up: u64, bytes_down: u64,
                       objects_up: u64, objects_down: u64, duration_ms: u64 },
    PeerCatalog { installation: String, entries: Vec<PublishedEntry> },
    InboundSession { installation: String, lane: u32, role: LaneRole, opened: bool },
    InboundIncompatible { address: String, detail: String },
    ObjectError { state: String, hash: String, error: ErrorWire },
    LegacyLink { key: String, value: serde_json::Value },   // pre-cutover passthrough (W2)
    Error { scope: Scope, error: ErrorWire }, Resolved { scope: Scope, code: String },
    Activity(ActivityEntry) }
```

**Folder aggregate precedence** (`hub_rules::folder_state`), first match wins:

| # | State | Condition |
|---|---|---|
| 1 | `Stopped` | lifecycle stopped and not desired |
| 2 | `NeedsAttention` | NeedsUserAction or Fatal folder errors; mount `AttachFailed` NeedsUserAction; lifecycle `Failed` |
| 3 | `Paused` | sync disabled |
| 4 | `NotShared` | no peer's catalog lists the repo. A configured outbound peer whose catalog has not been listed since the daemon started (catalogs are not persisted, §3.7) counts as possibly sharing while it is not Online (CCR-WP1.INT-5, ratified in WP2.INT) |
| 5 | `Stalled` | any sharing peer is stalled |
| 6 | `InitialSync` | gate awaiting |
| 7 | transfer states | `Metadata`, `Downloading`, `Uploading`, or `Syncing` if mixed; summary "N of M peers" |
| 8 | `Blocked` | only blocked, quarantined or rejected work remains |
| 9 | `Backoff` | all sharing peers in backoff |
| 10 | `Offline` | all sharing peers offline, incompatible or unauthorized |
| 11 | `UpToDate` | none of the above |

Peers that do not share the repo are ignored.

**Stall:** work remains, the peer is connected, and there has been no progress for `max(60 s, 20 × sync_interval)` (`TKFS_STALL_AFTER_MS` overrides). The culprit (the last error's object or event) is named.

**Error display:**
- Contention is hidden unless it has lasted 30 s.
- Transient errors are shown after 3 attempts.
- Errors clear on the next success in the same scope; history keeps 50 entries with `resolved_at_ms`.

**Three separate claims (invariant 30):** `metadata.caught_up`, `content.complete` and per-peer outbox acks (remote backup) are separate fields.

### 7.2 Status delivery

- `Action::Status` returns `StatusSnapshot`.
- `Action::Subscribe{since, wait_ms ≤ 25000}` long-polls `wait_newer`.
- CLI: `tkfs manage status [--json]` and `tkfs manage watch [--json]`.
- The GUI uses one subscription `ControlClient` and a separate command `ControlClient`.

### 7.3 Legacy compatibility projection (frozen semantics; goldens from WP0.5)

List, Inspect and Peers keep their exact shapes and add a typed `status: FolderStatus` member to List and Inspect. That member describes the folder now: its `errors` are the open ones only; resolved history stays in `Status` (CCR-WP2.1-4, ratified in WP2.INT).

| Field | Rule |
|---|---|
| `running` | `phase == Running` && gate ∉ {awaiting, blocked} && (no mount requested \|\| `!mount_enabled` \|\| mount intent `Detach` (the user detached it, a Wave 2 action; CCR-WP2.INT-3) \|\| mount ∈ {Attached, AttachFailed, Lost}) |
| `starting` | `phase == Starting` \|\| (`phase == Running` && `!running`) |
| `mounted` | mount state `Attached` (actual) |
| `initial_sync_pending` | a gate row exists |
| `startup_phase` | the `Starting` phase text; `"synchronizing remote metadata"` while gated; `"attaching filesystem"` while attaching; else null |
| `error` | lifecycle error, or mount `AttachFailed` message |
| `publication_error` | published entry error (`PUBLISHED_SUMMARY_FAILED: …`) |
| `sync` | **Waves 1–2:** legacy `LinkStatus` JSON served verbatim from legacy lanes (`orchestrator.rs:653-672` shape). **From WP3.1:** `hub.legacy_folder_sync()`, which keeps keys `endpoint`, `connected`, `remote_available`, `error`, `metadata{last_success_ms, caught_up, metadata_caught_up, metadata_progress, …}`, `lanes[{lane, role, connected, transport:"tkfs-peer/3", parallel_bulk:true, error, repository{…RepositoryStatus keys…}}]` |
| Peers | `[{endpoint, installation, connections, configured_connections, reserved_connections:1, lanes:[{role, connected, exchanges, last_success_ms, error, transport, parallel_bulk, repositories{repo:{…}}}]}]`; outbound peers only (the acceptor lists `[]`, as today) |
| Create, Receive, Start | inspect shape with `starting:true` |
| Stop | inspect shape after a ≤ 15 s wait (`running:false`, `starting:false`, `startup_phase:null`), or `{…, stopping:true, waiting_for:"BUSY_VIEW…"}` |

### 7.4 Concrete UI (WP3.4)

**Header:**
- daemon status: installation, version, uptime;
- "Log" button (opens `log_path`);
- global errors banner.

**Folder row:**
- label and mount path;
- **sync chip**, for example:
  - "Up to date"
  - "Syncing metadata · 412 left"
  - "Downloading 1.2 GiB of 3.0 GiB · 3.4 MB/s · ETA 6 min"
  - "Uploading 120 files"
  - "Stalled 4 min — CORRUPT_OBJECT photos/a.jpg"
  - "Blocked — 3 events quarantined"
  - "Waiting for peer X (offline 5 min)"
  - "Paused"
  - "Needs attention: …"
- **progress bar** (bytes or objects remaining);
- **mount chip**: "Mounted", "Headless", "Mount failed [Retry]", "Waiting for initial sync [Mount now]";
- **lifecycle chip**: "Starting — verifying objects (seq 37)", "Running", "Restarting in 32 s (attempt 3)", "Crash loop [Retry]", "Stopping — files open [Force stop]", "Adopted".

**Folder dialog tabs:**

| Tab | Contents |
|---|---|
| Overview | lifecycle, mount, gate (with `waiting_on` list), store health, pending saves, open handles |
| Sync | per-peer table: peer, direction, state, to send/receive, local/remote pending, blocked/quarantined/rejected, ↓/↑ bytes, rate, ETA, last progress, error (class, message, next retry) |
| Activation | inbox counts, rate, backlog, starvation, **Revalidate quarantined** |
| Content | counters and missing items |
| Errors | history: first and last time, count, class, resolved |
| Activity | the activity ring |

**Folder actions:** Start, Stop, Force stop, Mount now, Attach, Detach, Pause/Resume sync, Retry, Revalidate.

**Peer sidebar:**
- state badge: Online, Connecting, Offline, "Backoff (retry in 12 s)", **"Peer needs upgrade"**, "Unauthorized — re-pair", Revoked;
- direction, RTT, ↑/↓ rate;
- lanes: role, state, in-flight op, repo, since, bytes;
- inbound-only peers listed;
- shared folders.

**Behaviour:** per-action busy spinners; commands never wait behind the subscription; errors disappear when absent from a snapshot.

---

## 8. Error taxonomy (`model::errors`)

- `code_of(&anyhow::Error)` returns the first SCREAMING_CASE token in the error chain.
- `classify(code)` uses the table below. Unknown codes are Transient.
- `ErrorWire{code, class, message, retry_after_ms?}`.
- `ErrorRecord{error, scope, first_at_ms, last_at_ms, count, next_retry_at_ms, resolved_at_ms}`.
- `Scope = Daemon | Folder{state} | Peer{installation} | Lane{installation, lane} | FolderPeer{state, installation} | Object{state, hash} | Event{state, id} | Mount{state}`.

| Class | Retry policy | UI | Codes |
|---|---|---|---|
| **Contention** | 50 → 1,000 ms; never counted as failure; never resets `caught_up` | hidden unless ≥ 30 s | `WORKER_BUSY`, `BUSY`, `BUSY_VIEW`, `IN_FLIGHT_ELSEWHERE` |
| **Transient** | exponential (1 s → 60 s; lanes 1 s → 60 s; objects 30 s → 1 h; inbox 1 s → 5 min) | after 3 attempts | `REQUEST_TIMEOUT`, `CONNECTION_LOST`, `TRANSPORT_DEAD`, `WS_CONNECT_FAILED`, `WS_CLOSED`, `LOCAL_WORKER_UNAVAILABLE`, `REMOTE_WORKER_STARTING`, `WORKER_NOT_RUNNING`, `WORKER_EXITED`, `WORKER_START_TIMEOUT`, `WORKER_UNRESPONSIVE`, `CONTENT_UNAVAILABLE`, `OFFLINE_OBJECT_MISSING`, `OBJECT_IO_ERROR`, `CORRUPT_OBJECT`, `OBJECT_PART_CHANGED`, `RESUME_OFFSET_AHEAD`, `SQLITE_BUSY`, `CATALOG_UNAVAILABLE`, `PUBLISHED_SUMMARY_FAILED`, `NETWORK_NOT_READY`, `FOLDER_SYNC_PAUSED`†, `NOT_AVAILABLE` |
| **NeedsUserAction** | none (user action, or recheck 60 s for auth) | always, with an action button | `MOUNT_PATH_MUST_NOT_EXIST`, `MOUNT_OVERLAPS_DATA`, `MOUNT_OVERLAPS_FOLDER`, `MOUNT_OVERLAPS_STATE`, `WINFSP_RUNTIME_UNAVAILABLE`, `INVALID_PAIRING_CODE`, `PAIRING_CODE_CHANGED`, `PAIRING_CODE_NOT_SET`, `PEER_INSTALLATION_CHANGED`, `PEER_REVOKED`, `CANNOT_PAIR_WITH_SELF`, `NAMESPACE_HISTORY_LIMIT`, `CRASH_LOOP`, `PREVIOUS_WORKER_UNRESPONSIVE`, `STOP_BLOCKED_BY_OPEN_FILES`, `GATE_BLOCKED`, `ALREADY_RUNNING` |
| **Incompatible** | recheck every 10 min or on config change | always | `PEER_NEEDS_UPGRADE`, `PEER_PROTOCOL_UNSUPPORTED`, `INCOMPATIBLE_ORCHESTRATOR_PROTOCOL`, `INCOMPATIBLE_PEER_PROTOCOL`, `RESERVED_LANES_UNSUPPORTED`, `INCOMPATIBLE_BULK_LANE`, `WORKER_IPC_MISMATCH` |
| **Fatal** (scoped quarantine) | none in scope | always | `STATE_NOT_INITIALIZED`, `STORE_IDENTITY_MISMATCH`, `INVALID_STORE_METADATA`, `INVALID_SELECTED_BRANCH`, `REPOSITORY_MISMATCH`, `ENGINE_POISONED`, `WORKER_STATE_MISMATCH`, `EVENT_ID_REUSED`, `FORGED_EVENT_ORIGIN`, `UNAUTHORIZED_EVENT`, `PRIVATE_EVENT_REJECTED`, `PRIVATE_BRANCH_REJECTED`, `INVALID_EVENT`, `CAUSAL_CYCLE`, `INVALID_CAUSAL_PARENT`, `INVALID_CAUSAL_DEPENDENCY`, `QUARANTINED_CAUSAL_ANCESTOR`, `OBJECT_SIZE_CHANGED`, `UNREFERENCED_OBJECT_SIZE`, `BRANCH_DESCRIPTOR_CHANGED`, `EVENT_METADATA_TOO_LARGE`, `UNBOUND_PEER_RESPONSE`, `UNBOUND_OR_EARLY_ACK`, `BULK_ROUTE_MISMATCH`, `REMOTE_REPLICA_BINDING_MISMATCH`, `LOCAL_REPLICA_BINDING_MISMATCH`, `DUPLICATE_REPLICA_IDENTITY`, `METADATA_ON_BULK_LANE`, `BULK_ON_CONTROL_LANE`, `BULK_LANE_MISMATCH`, `OBJECT_ON_WRONG_LANE`, `UNAUTHORIZED_OBJECT`, `UNSOLICITED_OBJECT`, `NOT_PERMITTED`, `CORRUPT_AT_SOURCE`, `SIZE_MISMATCH`, `INVENTORY_TOO_LARGE`, `INVALID_OBJECT_PART`, `CONFLICT_MODE_MISMATCH` |

† `FOLDER_SYNC_PAUSED` is Transient but shown as the `Paused` state, never as an error.

The table test (WP0.4) greps `src/` for `"[A-Z][A-Z_]{3,}` code literals and asserts every one is classified or explicitly listed as Transient-default.

---

## 9. Storage and migration rules

**Positional-INSERT tables** (never ALTER; add side tables keyed by the same primary key):

| Table | Writers |
|---|---|
| `folders.sqlite`: `projects` | `orchestrator.rs:431` |
| `initial_sync` | `:443` |
| `peers` | `:815` |
| `settings` | `:346`, `:877` |
| `metadata.sqlite`: `object_errors` | `core.rs:440`, `runtime.rs:1059` |
| `object_requests` | `core.rs:539` |
| `events` | `:1119` |
| `branches` | `:385`, `:1115`, `:1907`, `:2157`, `:2309` |
| `config` | `:306`, `:394`, `:2638` |
| `current_parts` | `:1139`, `snapshot.rs:284` |
| `current_state` | `:2874`, `snapshot.rs:221` |
| `conflicts` | `snapshot.rs:226` |
| `event_frontier` | `:864`, `:1156`, `snapshot.rs` |
| `object_sizes` | |
| `object_references` | |
| `published_objects` | |
| `received_shared_objects` | |
| `peer_known` | `:2181` |
| `peer_objects` | |
| `peer_requests` | |
| `checkpoints` | |
| `receipts` | `runtime.rs:762` |
| `pending_saves` | `runtime.rs:147` |
| `startup_snapshots` | |

**Named-column writers** (nullable `ADD COLUMN` allowed): `incoming_events` (`core.rs:914`, `:2287`) and `outbox(event)`. Only `incoming_events` gains columns.

**New tables** (all `CREATE TABLE IF NOT EXISTS`, each module's `ensure_schema` in its own SAVEPOINT, stamped with `config('schema:<module>', n)`):

| Database | Table / object | Owner |
|---|---|---|
| folders.sqlite | `folder_intents(state PK, sync_enabled, mount_enabled, updated_ms)` | WP1.1 |
| folders.sqlite | `mount_gates(state PK, source_installation, source_endpoint, created_ms, override_ms, satisfied_ms)` | WP1.1 |
| folders.sqlite | `peer_meta(installation PK, last_seen_ms, display_name, incompatible, revoked_ms)` | WP1.1 |
| folders.sqlite | settings key `verifier_generation` | WP1.1 |
| folders.sqlite | `peer_credentials(installation PK, secret_hash, created_ms, revoked_ms)` | WP4.1 |
| metadata.sqlite | `incoming_events` + `state, received_ms, attempts, retry_at_ms, code, error_class, blocked_on` | WP1.6 |
| metadata.sqlite | `incoming_waits(child, parent, PK(child,parent))` (+ index on parent) | WP1.6 |
| metadata.sqlite | `incoming_sources(event, peer, seq, PK(event,peer))` | WP1.6 |
| metadata.sqlite | `AFTER DELETE ON incoming_events` trigger cleaning waits and sources | WP1.6 |
| metadata.sqlite | `object_availability`, `event_objects` (+ triggers), `staged_objects` (+ triggers), `object_error_state` | WP1.8 |
| metadata.sqlite | `journal_epoch` config (+ delete trigger) | WP2.5 |
| metadata.sqlite | `feed_in(peer PK, epoch, seq, id, tip, updated_ms)` | WP2.5 |
| metadata.sqlite | `feed_out(peer PK, epoch, seq, id, reported_ms)` | WP2.5 |
| metadata.sqlite | `feed_needs(peer, event, reason, requested_ms, missing_at, PK)` | WP2.5 |
| metadata.sqlite | `feed_rejected(peer, event, seq, code, payload, at_ms, PK)` | WP2.5 |
| metadata.sqlite | `spool_state(hash PK, durable, total, updated_ms)` | WP3.2 |

**Rules:**
- Backfills are chunked, resumable and progress-reporting.
- Existing states open and upgrade automatically.
- Legacy fixtures from `6298d0b` (WP0.5) must open after every wave (each integration package checks this).
- Nothing deletes user data or history. Evacuation (`core.rs:871-935`) is unchanged and `rebuild_frontiers` always runs after it.

---

## 10. Invariant guardrails (synthesis §4, all 30)

| # | Invariant (short) | Mechanism in this plan | Guard test (package) |
|---|---|---|---|
| 1 | Verified, durable install before the CAS and inventories | Full re-read SHA-256 before install. Write-through install. Availability set only after install. Sender serves only available and shareable objects. | `objects_v3_worker::corrupt_spool_never_installed` (WP3.2), `availability_index` (WP1.8) |
| 2 | Overlap verified; no gaps; offset advertised only after fsync | `spool_state.durable` advances only after a spool fsync and a commit. Append must start exactly at `durable`. Overlap is compared byte for byte. | `objects_v3_worker::crash_mid_object_resumes_from_durable` (WP3.2) |
| 3 | Register only after install; crash between them is recoverable | Registration happens after install. A crash in between is repaired by re-transfer: the object is not shareable, so it is still wanted. No startup registration. | `crash_between_install_and_register_retransfers` (WP1.8, WP3.2) |
| 4 | `synchronous=FULL`; atomic commit | Index triggers and side rows join the existing savepoints. Activation slice savepoint. | existing suites |
| 5 | Snapshots are disposable | Format unchanged; forced seal kept. | `startup_snapshots` (all) |
| 6 | Immutable events | No payload rewrite. Derived tables only. Digests are computed, never stored in events. | `EVENT_ID_REUSED` tests |
| 7 | Immutable sizes | `OBJECT_SIZE_CHANGED` checked at stage, at `commit_prepared` (re-validated), and at write. | `offlock_commit::size_change_between_prepare_and_commit_rejected` (WP2.4) |
| 8 | Dirty sessions survive | Force detach: fence → drain → persist. Poison: `into_inner`, then `retain_all_dirty`. Shutdown leaves busy workers running. | `worker_lifecycle::force_stop_persists_dirty`, `poison_exit_persists_dirty` (WP1.4/1.5) |
| 9 | Causal activation | `incoming_waits`; activation only when every parent is committed. | `activation_inbox` (WP1.6) |
| 10 | Committed-only, bound acks; staged ≠ ack | Acks come only from per-id committed-feed evidence plus a digest match. Responses are bound by `reply_to`. The cursor never acks. Legacy v6 ack checks are unchanged. | `feed_v7::staged_never_acks`, `rejected_never_acks`, `third_peer_staged_not_acked` (WP2.5) |
| 11 | Deterministic conflicts; limit refused before ACK | Fast paths are a restriction of full mode. Append mode runs under an exactness precondition with full-mode counting. `verify` mode. Oracle fuzz. | `derive_oracle` 300 seeds release (WP1.7), `activation_inbox` oracle (WP1.6) |
| 12 | Private never leaves | The feed filters shared branches. `PRIVATE_BRANCH_REJECTED` at stage. | `feed_v7::private_never_served` (WP2.5) |
| 13 | Only shared, provenanced objects sent; hash-only grants nothing | Read authorization triad. Write accepts only wanted objects. No registration without a verified transfer. | `paired_peer_cannot_disclose_private_cas…` (kept), `objects_v3_worker::hash_only_request_not_permitted` (WP3.2) |
| 14 | No forged origin | Point-query check at stage (feed) and at receive (v6). | `feed_v7::forged_origin_rejected` (WP2.5) |
| 15 | Route binding | Dialer routes from the catalog plus the authenticated peer catalog. Inbound routes from the authenticated installation. Worker binding checks kept. | `inbound_v3::route_binding` (WP2.8) |
| 16 | No replay of uncertain requests | `CallError::is_uncertain()`; schedulers issue fresh requests; ControlClient never replays mutations. | `session_core::lost_request_not_replayed` (WP1.9) |
| 17 | RemoteError keeps the session | RESPONSE `ok:false` leaves the connection usable. | `session_core::remote_error_keeps_conn` |
| 18 | Control carries no object bytes; bulk never activates or acks | Op/role table. Large events only on control, chunked. | `inbound_v3::role_violations` (WP2.8), `objects_v3_lanes::control_carries_no_object_bytes` (WP3.3) |
| 19 | One owner per object per session | `Lane::owns` sharding plus worker leases plus per-object locks. | `objects_v3_worker::duplicate_sessions_transfer_once` (WP3.2) |
| 20 | Fail closed | `X-Tkfs-Sync` both directions → `PEER_NEEDS_UPGRADE`; no downgrade. | `session_handshake::legacy_both_directions` (WP1.10), `qual_wp3_1` (WP3.1) |
| 21 | Salted verifier; rotation invalidates; installation check; no self-pair; code never in status | HMAC over the verifier digest; `verifier_generation` → GOAWAY. Status redaction test. | `session_handshake::*` (WP1.10), `security` (WP4.1) |
| 22 | Bounded input | Frame and header limits, pre-auth 16 KiB / 5 s, page ≤ 4,096 entries / 1 MiB, need ≤ 256, `max_body` per op. | `session_core::bounds`, `inbound_v3::preauth_flood` |
| 23 | Single owner per data dir and state | Catalog holds `orchestrator.lock`. Adoption never spawns while `owner.lock` is held; pid kill verified. | `folder_controller::adoption_*` (WP1.2), `qual_wp2_1` |
| 24 | Recovery before RPC | Listener opens after `Store::open` and `Engine::new`. Deferred activation is not recovery. | `worker_lifecycle::rpc_after_recovery` (WP1.4) |
| 25 | Workers never dial | Unchanged: worker sessions are loopback, accepted only. | review |
| 26 | Intent durable; gate | `desired=0` persisted first. Gate marker deleted only after attach (with no mount requested: once the gate is satisfied or overridden, CCR-WP2.INT-2). Shutdown never changes `desired_running`. | `folder_controller::gate_*`, `received_mount_waits…` (kept) |
| 27 | Mount safety | `validate_mount_path`. FUSE recovery only for mounts proven ours. | `mount_lifecycle::foreign_mount_untouched` (WP1.5) |
| 28 | Watchdog progress from the recovering thread | Backfills emit progress from the opening thread. Heartbeat is not startup progress. | `worker_lifecycle::backfill_reports_progress` (WP1.8/1.4) |
| 29 | Interruptible waits; Shutdown reply flushed | `Cancel` everywhere; reply flushed before exit. | `qual_wp2_1::shutdown_*` |
| 30 | Three separate claims; status never blocks or writes | Separate fields. Hub and cell are lock-free snapshots. | `status_hub::*` (WP1.11), `worker_concurrency::status_never_blocks` (WP2.3) |

---

## 11. Decisions and rationale

1. **Base the plan on isolation-first, plus strict seams.**
   - **Why:** best P2/P3 design; precise trait seams for parallel agents.
   - **Rejected:** throughput-first as the base (hand-built async router and Wave 2 intra-dependencies); risk-first as the base (no request multiplexing; GUI last).
2. **Concurrency: std threads, blocking caller APIs, single I/O thread per connection using `polling` 3.11.0 (the only new dependency).**
   - **Why:** full duplex; heartbeats and multiplexing; works for native-tls WSS. Avoids Windows `SO_RCVTIMEO` indeterminate socket state. tungstenite's automatic Pong and Close writes happen on the same thread. `polling` and its Linux dependency `rustix` 1.1.4 are already in `Cargo.lock`, so no new lock entries.
   - **Rejected:**
     - split reader and writer threads (tungstenite `read()` writes Pong and Close, `protocol/mod.rs:457-466`);
     - a WSS pump with 20 ms read timeouts;
     - a socket-shutdown watchdog only (no multiplexing);
     - tokio (excluded);
     - crossbeam (std mpsc with an enum mailbox suffices);
     - arc-swap or parking_lot (`Snapshot` is `RwLock<Arc<T>>`).
3. **No `hmac` crate.** RFC 2104 HMAC-SHA256 is about 30 lines over `sha2` 0.10, checked against RFC 4231 vectors (`util::hmac`).
4. **Dialer-driven exchanges in both directions on each request.** Only clients originate requests.
   - **Why:** one-way pairing must still sync both directions (`tests/orchestrator.rs:327`: the acceptor has no Peers). All scheduling lives in the PeerSupervisor, and the server stays passive and bounded.
   - **Rejected:** symmetric requests with an acceptor-side scheduler.
5. **Metadata as a cursor feed with acks from per-id evidence.** Grafted from throughput-first, adapted to dialer-driven exchanges.
   - **Why:**
     - steady state is O(delta) and idle exchanges are O(1);
     - staged events are never re-sent (the cursor); RC3 and the field symptom of a stuck 367-event pending window both go away;
     - acks keep today's known-set semantics, so invariant 10 holds by construction;
     - regressions are detected by `(epoch, seq, id)` validation.
   - **Rejected:**
     - watermark acks (`committed_through`), which over-ack (integrity judge);
     - sender-side staged or rejected suppression without regression handling;
     - per-origin version vectors (events carry no per-origin counters; invariant 6).
6. **Append-mode derivation in Wave 1, with an exactness precondition and fallback.**
   - **Why:** fixes two-way and concurrent activation early (the impact must-fix). It is provably equal to full mode when the precondition holds, falls back otherwise, and is guarded by `verify` mode and the oracle (the integrity must-fix).
   - **Rejected:**
     - "monotone union, never delete" (diverges from full mode under witness compression);
     - clock-based incremental derivation (new derived state; cut).
7. **Off-lock slow path with optimistic commit; Stale results are re-queued, never inline.**
   - **Why:** bounds the lock hold in every fallback (impact must-fix). Re-validating inside the commit covers global tables (integrity must-fix).
8. **Objects v3: receiver wants, pull plus push on dialer lanes, binary chunks, literal invariants 1–3 and 19.**
   - **Why:** removes the activation gate (staged references), stop-and-wait, hex, page atomicity and direction coupling.
   - **Rejected:** sparse or striped spools; cross-file group fsync (both put invariants 1–2 at risk).
9. **Workers outlive the daemon; adoption by `worker.json`, hello and verified pid.**
   - **Why:** dirty sessions and mounts survive a daemon crash (invariant 8).
   - **Rejected:** Job Object or PDEATHSIG.
10. **Mount as an RPC-attached sub-state; gate in place; marker cleared after attach.**
    - **Why:** RC10 and RC11. Matches the lifecycle study in `docs/ARCHITECTURE.md:43-120`.
11. **Legacy v6 replica API (`paired_sync`, `bulk_sync`, worker `published-*` ops) kept as an in-process test surface.**
    - **Why:** 21 `worker_control` call sites and about 40 tests encode invariants (forged origin, private CAS, ack binding). The network uses of these modules are deleted at cutover.
12. **Schema: side tables only, except `incoming_events`.**
    - **Why:** positional INSERTs. The code shows the opposite of the base proposal's claim.
13. **Wave 0 integrator lands every intra-wave seam stub; later integrators apply CCRs.**
    - **Why:** packages never compile against prose, and nobody edits another package's files.
14. **Interim bulk path over v3 (`objects.exchange`).**
    - **Why:** lets the cutover package pass its end-to-end tests in isolation while objects v3 is built in parallel in the same wave.
15. **Credential equivalence is documented.**
    - `Verifier.hash` is HMAC-key-equivalent to the code.
    - Dialers already store codes in plaintext in `peers.code` (`orchestrator.rs:815`).
    - Token files and `folders.sqlite` must be owner-only (Unix 0600; on Windows they inherit the user profile ACL).
    - WP4.1 adds per-peer random secrets and revocation.

---

## 12. Scope cuts

- No tokio or async migration. No separate mount-adapter process: a native adapter crash still kills its worker, which is restarted with backoff.
- No sparse or range spools, no striping of one object across lanes, no cross-file group fsync, no pack files.
- No clock or version-vector incremental conflict derivation. Append mode plus off-lock prepare is the plan.
- No snapshot FORMAT 2 (chain hash). The forced seal per slow commit stays O(N) bytes hashed, which is acceptable.
- No deterministic single session under mutual pairing. Worker leases prevent duplicate bytes, and duplicate metadata polling is O(1) when idle.
- No garbage collection of objects fetched for staged events that are later quarantined. They are kept.
- No porting of the ~40 legacy v6 in-process tests. `paired_sync` and `bulk_sync` stay as a test surface.
- No mutual TLS or Noise. HMAC challenge-response plus an operator TLS proxy (WSS) remains the model. Per-peer secrets come in WP4.1.
- No CI pipeline (none exists). `scripts/qualify.*` plus evidence JSON are the gates.
- No Linux desktop GUI. Linux gets `manage status --watch`.
- No user bandwidth limits or QoS. No cross-peer global scheduler.
- No v2 interoperability after cutover (WP3.1): old peers fail closed. Waves 1–2 remain wire-compatible with 6298d0b peers.
- No automatic kill of a worker whose engine lock is stalled (Degraded status only).

---

## 13. Delivery rules for parallel agents

1. **Ownership** is per file per wave (§14).
   - **Append-only registry exceptions:** `mod`/`pub mod` lines in `src/lib.rs`, `src/core.rs`, `src/orchestrator/mod.rs`, `src/runtime/mod.rs`, `src/worker/mod.rs`. After WP0.INT every module file exists, so later packages should not need them.
   - Only WP0.4 touches `Cargo.toml` and `Cargo.lock`.
2. **Frozen contracts:** `src/model/**`, `src/util/**`, and every stub signature landed by WP0.INT. A needed change is filed as `CCR-<pkg>-<n>` in the package's PR notes, with an adapter on the requester's own side. The wave integrator applies it, then the next wave sees it.
3. **No cross-boundary signature changes inside a wave.** Add a new function and keep the old one as a wrapper.
4. **Tests:** each package adds its own test files. It edits existing test files only where §14 says so, and justifies each changed assertion in the PR. Each `tests/qual_wpX_Y_*.rs` file is created `#[ignore = "WPX.Y"]` by WP0.5 and un-ignored and extended only by its owner.
5. **Bug-fix exception:** a defect found in a frozen file owned by an earlier package is filed as a CCR. The wave integrator fixes it.
6. **Worktrees:** `C:/Users/felyp/Desktop/Projetos/Telekinesis-wt/<pkg>` on branch `revamp/<pkg-id>-<slug>`, cut from `revamp/wave-<N>-base`. The integrator merges in the listed order and fast-forwards `revamp/production-sync`.
7. **VERIFY** (every package, in its worktree):

   ```powershell
   $env:CARGO_TARGET_DIR = "C:\tkfs-target\<pkg>"          # never share target dirs between worktrees
   cargo fmt --check
   cargo build --all-targets
   cargo clippy --all-targets -- -D warnings
   scripts\suite-lock.ps1 cargo test --all-targets          # machine-wide lock: one full suite at a time
   wsl.exe -e bash -lc 'cd /mnt/c/Users/felyp/Desktop/Projetos/Telekinesis-wt/<pkg> && CARGO_TARGET_DIR=$HOME/tkfs-target/<pkg> cargo check --all-targets'
   ```

   - **Packages touching** `fuse_linux.rs`, worker lifecycle, orchestrator, session or core activation also run the relevant `cargo test --test <files>` in WSL. WSL has `/dev/fuse`; FUSE tests skip if it is absent.
   - **Release scenarios:** `cargo test --release --test qual_wpX_Y_<name> -- --ignored --test-threads=1`.
   - **Windows prerequisites:** the WinFsp SDK (`WINFSP_DIR`) and Slint (`build.rs`).
8. **Integration package checklist** (`WPw.INT`):
   1. Merge in the listed order, running the touched tests after each merge.
   2. Full VERIFY on the merged tree, plus the WSL test subset (`--test orchestrator --test session_* --test worker_lifecycle --test mount_lifecycle`).
   3. Legacy fixtures (`tests/fixtures/legacy`) open and upgrade.
   4. Waves 1–2 only: interoperability against a 6298d0b peer (baseline binary built from the tag).
   5. Run that wave's qual scenarios in release; write `test-evidence/qualification-wave-<w>.json`.
   6. Apply CCRs.
   7. Create `revamp/wave-<w+1>-base`.

---

## 14. Waves and work packages

| Wave | Goal | Packages (run in parallel) | Integration |
|---|---|---|---|
| **0** | Behavior-neutral carve-up, frozen contracts, harness, seam stubs | WP0.1, WP0.2, WP0.3, WP0.4, WP0.5 | WP0.INT |
| **1** | User-visible P1/P3 relief on the legacy path, and new components built standalone | WP1.1–WP1.12 | WP1.INT |
| **2** | Wire the actor orchestrator (legacy network bridged); background activation; feed protocol; v3 peer stack built standalone; CLI status | WP2.1–WP2.8 | WP2.INT |
| **3** | Cutover to v3 (v2 deleted, fail-closed); objects v3; GUI | WP3.1–WP3.4 | WP3.INT |
| **4** | Security hardening, operations, qualification floors, docs | WP4.1–WP4.3 | WP4.INT |

**Critical path:** WP0.4 → WP1.9/1.10 → WP2.6/2.7/2.8 → WP3.1 → WP3.INT → WP4.INT. The worker path (WP1.4, WP1.6 → WP2.3, WP2.5) runs in parallel with it.

**Releasable points:**
- **After WP1.INT:** remote fast paths, append-mode derivation, availability index without startup re-hash, mount failure no longer kills sync, lane stabilizers. Still wire-compatible with 6298d0b.
- **After WP2.INT:** actor lifecycle (no global lock, adoption, backoff, Shutdown outcomes), background activation, CLI status. Still wire-compatible.
- **After WP3.INT:** v3 networking, the feed protocol, streamed objects, the new GUI. Older peers get `PEER_NEEDS_UPGRADE`.

---

### Wave 0 — carve-up, contracts, harness

#### WP0.1 Orchestrator carve-up (behavior-neutral)

- **Goal:** split `src/orchestrator.rs` (1,479 lines) into per-concern files with zero behavior change.
- **Owned:** `src/orchestrator.rs` (deleted) → `src/orchestrator/{mod.rs, config.rs, api.rs, catalog_db.rs, supervisor.rs, peering.rs, lanes.rs, status.rs, run.rs}`.
- **Depends on:** none.
- **Tasks:**
  1. Move code verbatim:

     | File | Contents |
     |---|---|
     | `config.rs` | `Config`, `Network`, `default_limit` |
     | `api.rs` | `Action`, `Project`, `client()`, `manage(shared, action)` |
     | `status.rs` | `LinkStatus`, `RepositoryStatus`, `retry_delay` |
     | `catalog_db.rs` | `Peer` (pub(crate)); `Supervisor::{open, projects, project, peers, verifier, initial_peer}`; and new named methods holding the **identical** SQL text currently inline in `manage`/`reconcile`/`create`: `set_desired`, `set_mount`, `insert_project_tx`, `insert_initial_sync_tx`, `upsert_peer`, `delete_peer`, `set_pairing_verifier`, `delete_initial_sync` |
     | `supervisor.rs` | `Supervisor` struct (fields pub(crate)), `Worker`, `start`, `poll_worker`, `stop`, `inspect`, `reconcile`, `create`, `mount_path`, `discovery` |
     | `peering.rs` | `dial`, `dial_role`, `remote_list`, `published`, `route`, `serve_peer` |
     | `lanes.rs` | `lane_role`, `link_key`, `lane_active`, `lane_wait`, `exchange_bulk`, `run_lane` |
     | `run.rs` | `run()` |
     | `mod.rs` | `lock`, `lock_state`, module declarations, and `pub use` of `Config`, `Network`, `Action`, `Project`, `Supervisor`, `Shared`, `client`, `manage`, `run` |

  2. Change visibility only (pub(crate)/pub(super)). No renames and no logic edits.
- **Acceptance:**
  - Full suite green with the same test count.
  - A `git diff -M --color-moved=dimmed-zebra` review shows only moves, plus the SQL-method extraction.
  - VERIFY passes.
- **Invariants at risk:** none (mechanical).
- **Estimate:** ~1,700 lines moved (mechanical; exempt from the size guideline).

#### WP0.2 Runtime carve-up, worker entry, engine seams

- **Goal:** split `src/runtime.rs` (1,344 lines), move the worker entry out of `main.rs`, and add two additive seams.
- **Owned:**
  - `src/runtime.rs` (deleted) → `src/runtime/{mod.rs, engine.rs, control.rs, server.rs, published.rs, content.rs, notify.rs, export.rs}`;
  - `src/worker/mod.rs`;
  - `src/main.rs`;
  - `src/lib.rs` (append `pub mod worker;`).
- **Depends on:** none.
- **Tasks:**
  1. Move code verbatim:

     | File | Contents |
     |---|---|
     | `engine.rs` | `Session`, `Handle`, `PendingSave`, `Engine` and all FS/session methods, `receive_peer*` |
     | `control.rs` | `Engine::control` |
     | `server.rs` | `start_rpc`, `dispatch_rpc` |
     | `published.rs` | `published_rpc` |
     | `content.rs` | `prepare_content`, `fetch_verified` |
     | `notify.rs` | `visible_paths`, `namespace_notifications`, `deliver_notifications` |
     | `export.rs` | `export_bucket` |
     | `mod.rs` | `Shared`, `MAX_FRAME`, `PEER_FORMAT` re-export, `Discovery`, `rpc`, `RpcLane`, plus `pub use` of every previously public item |

  2. Move `main.rs::worker()` and the Linux signal handler to `worker::run(state: &Path, mount: Option<&Path>) -> Result<()>`. `main.rs` calls it.
  3. Seams, which add behavior without changing any existing path:
     - `pub fn runtime::lock_engine(e: &Shared) -> MutexGuard<'_, Engine>`, with body `e.lock().unwrap()` for now.
     - `pub fn Engine::retain_all_dirty(&mut self) -> Result<usize>`: calls `retain_pending` for every dirty session and returns the count.
     - Make `retain_pending` `pub(crate)`.
- **Acceptance:**
  - Suite green; diff review shows only moves.
  - New unit test `retain_all_dirty_persists_every_dirty_session` (in `tests/worker_seams.rs`).
- **Estimate:** ~1,600 moved + 60 new.

#### WP0.3 Core carve-up, open phases, caches, oracle, view freshness

- **Goal:** split `src/core.rs` (3,911 lines) so later packages own disjoint files. Add the differential oracle and the secondary-handle cache fix.
- **Owned:**
  - `src/core.rs`;
  - `src/core/{objects.rs, demand.rs, status.rs, inbox.rs, commit.rs, derive.rs, sharing.rs, transfer.rs, verify.rs}`;
  - `src/core/snapshot.rs` (paths and visibility only);
  - `src/bulk_sync.rs` (move `ObjectLocks` out, then `pub use crate::core::objects::ObjectLocks`);
  - `tests/oracle_smoke.rs`.
- **Depends on:** none.
- **Tasks:**
  1. Move code verbatim (all keep `impl Store` blocks; `pub use` keeps every `tkfs::core::*` path):

     | File | Contents |
     |---|---|
     | `objects.rs` | `read_object`, `object_size*`, `remember_object_size`, `object_available`, `open_object`, `verify_object`, `put_stream`, `put_object`, `object_state`, `shared_object_inventory`, `install_object` (both cfgs), `ObjectLocks` |
     | `demand.rs` | `request_object`, `wanted_objects`, `peer_requests`, `remember_requests` |
     | `status.rs` | `content_status`, `status`, `outgoing_unacknowledged` |
     | `inbox.rs` | `receive`, `receive_with_budget`, `drain_incoming*`, `validate_*_links` |
     | `commit.rs` | `commit`, `commit_ready`, `content_successor`, `creation_successor`, `namespace_successor` |
     | `derive.rs` | `persist_projection`, `ancestors`, `event_ancestors`, `linear_versions`, `VersionReach`, `namespace_history`, `FactGraph`, `NamespaceCones`, `project`, `make_conflict`, `causal_order`, `activated_ids` |
     | `sharing.rs` | `shared_event_inventory`, `remember_peer_inventory`, `known_by_peer`, `shared_bundle`, `shared_page*`, `acknowledge`, `known_objects_by_peer`, `remember_peer_objects` |
     | `transfer.rs` | `Bundle`, `ObjectPart`, `partial_objects`, `peer_partial_objects`, `remember_peer_partials`, `shared_parts`, `transfer_parts`, `receive_parts*`, `register_received` |
     | `core.rs` (root) | types, helpers (`valid_object`, `stream_hash`, `object_error`, `hash`, `id`, `time`, `fault`), `Store`, `ViewCache`, open/initialize, branches/events/view/projection, rebuild, rebuild_frontiers, evacuation, all local FS/branch ops, `lookup`, `split_path` |

  2. Split `Store::open` into named phases, called in today's order:
     1. `open_schema`
     2. `snapshot::initialize`
     3. `objects::ensure_schema`, `inbox::ensure_schema`, `sharing::ensure_schema` (empty hooks)
     4. `objects::startup_object_check(&mut store, opts.verify)` (`core.rs:405-455` verbatim)
     5. evacuate
     6. `rebuild_frontiers`
     7. resume/rebuild
     8. `inbox::startup_drain(&mut store, opts.defer_activation)` (still drains)
  3. Add types and entry points:
     - `pub struct OpenOptions { pub defer_activation: bool, pub verify: VerifyMode }`, `pub enum VerifyMode { Full, Changed, Lazy }`, `Default` = `{false, Full}`.
     - `Store::open_existing_with(root, OpenOptions)`; `open_existing` delegates. In W0 all modes behave like `Full` and non-deferred.
     - `pub fn Store::open_secondary(root, repo, device) -> Result<Store>` (public alias of `open_transfer`).
  4. `pub(crate) caches: StoreCaches { objects, inbox, commit, derive, sharing }`. Each is an empty `#[derive(Default)] pub(crate) struct Cache` in its own module (interior mutability is the owner's choice). This is the only `Store` field change in the whole plan.
  5. `verify.rs`:
     - `pub struct DerivedDiff { projection_mismatch, conflicts_missing, conflicts_extra, resolved_by_mismatch, frontier_mismatch: Vec<String> }` with `is_empty()`.
     - `Store::verify_derived_state(&mut self) -> Result<DerivedDiff>`:
       1. `SAVEPOINT oracle`;
       2. capture per-branch `load_projection`, conflict rows (id, payload, `resolved_by`) and `event_frontier`;
       3. `rebuild_frontiers()` and `rebuild(branch)` for each branch;
       4. capture again;
       5. `ROLLBACK TO oracle; RELEASE oracle`;
       6. clear the view cache;
       7. compare parsed structs.
  6. View cache freshness: add `PRAGMA data_version` to the key in `view()` (`core.rs:805-810`). For a single connection the behavior is identical; it fixes stale views on secondary handles (I6).
- **Acceptance:**
  - Suite green (including `startup_snapshots`, `replication_regressions`, `scalability`).
  - `tests/oracle_smoke.rs`:
    - `oracle_empty_on_valid_histories` (sequential, concurrent, namespace collision before and after resolve, deleted parent, cycle);
    - `oracle_detects_tampered_conflicts_and_frontier` (`UPDATE conflicts SET resolved_by=NULL`; `DELETE FROM event_frontier`);
    - `oracle_has_no_side_effects`;
    - `secondary_handle_sees_engine_commits` (view freshness).
- **Invariants at risk:** 4, 5 (`RefCell` view cache semantics).
- **Estimate:** ~3,000 moved + 350 new.

#### WP0.4 Contracts, utilities, session stubs, dependency

- **Goal:** freeze every cross-package type, trait, wire, IPC and status shape, timing constant and session API before parallel work starts.
- **Owned:**
  - `src/model/{mod, errors, health, ipc, status, protocol, orchestrator}.rs`;
  - `src/util/{mod, snapshot, notify, cancel, backoff, lock, log, time, fault, hmac}.rs`;
  - `src/session/{mod, frame, io, conn, handshake, auth}.rs` (stubs with final public signatures);
  - `Cargo.toml` (`polling = "=3.11.0"`), `Cargo.lock`;
  - `src/lib.rs` (append `pub mod model; pub mod util; pub mod session;`);
  - `tests/contracts.rs`, `tests/fixtures/contracts/*.json`.
- **Depends on:** none.
- **Interfaces provided:** everything in §3.1, §6, §7.1 and §8, plus:

```rust
// util
pub struct Snapshot<T> { .. }  // new(T), load()->Arc<T>, store(T)->u64, update(impl FnOnce(&T)->T)->u64,
                               // generation()->u64, wait_newer(gen:u64, timeout:Duration)->u64
pub struct Notify { .. }       // poke(), wait(seen:u64, timeout)->u64, count()->u64
#[derive(Clone)] pub struct Cancel { .. }   // new(), cancel(), is_cancelled(), sleep(d)->bool /*true=cancelled*/,
                               // wait(), on_cancel(Box<dyn Fn()+Send+Sync>)  (runs registered killers)
pub struct Backoff { base:Duration, cap:Duration, jitter:f64, reset_after:Duration, .. }
                               // failure(now:Instant)->Duration, success(now:Instant), attempt()->u32
pub enum PoisonPolicy { Panic, Exit }  pub fn set_poison_policy(p); pub fn lock<T>(m:&Mutex<T>)->MutexGuard<T> /*poison-tolerant*/;
pub fn lock_or_exit<T>(m:&Mutex<T>, on_poison:&dyn Fn(&mut T))->MutexGuard<T>;  pub struct Tracked<T> /* debug hold-time assert 50 ms */
pub trait Clock: Send+Sync { fn now_ms(&self)->u64; fn instant(&self)->Instant; }  SystemClock; ManualClock{advance(d)}
macro event!(level, code, {k: v,..})  // JSON line {at_ms, level, code, ..}; 20/min per code then "suppressed N"
pub fn fault::hit(point:&str)->Option<String>; hang(point); fail(point)->Result<()>; exit(point); set_scope(s:&str)
pub fn fault::trigger(point:&str)->anyhow::Result<()>   // action from TKFS_FAULT_ACTION=hang|fail|exit|panic (default hang)
   // env TKFS_FAULT_POINT (exact), TKFS_FAULT_SCOPE (substring of fault::set_scope value), TKFS_FAULT_MS, TKFS_FAULT_COUNT;
   // existing core::fault (TKFS_FAULT exit 86) unchanged
pub fn hmac::hmac_sha256(key:&[u8], data:&[u8])->[u8;32]; pub fn hmac::ct_eq(a:&[u8], b:&[u8])->bool
```

```rust
// session (stubs; WP1.9/WP1.10 implement)
pub struct ConnOptions { pub heartbeat: Duration /*5s*/, pub dead_after: Duration /*20s*/, pub max_header: usize /*64Ki*/,
    pub max_chunk: usize /*1Mi*/, pub max_body: usize /*16Mi*/, pub handlers: usize, pub handler_queue: usize,
    pub control_queue_bytes: usize /*16Mi*/, pub data_queue_bytes: usize /*32Mi*/ }
pub enum Priority { Control, Response, Data }
pub struct Reply { pub header: serde_json::Value, pub body: Vec<u8> }
#[derive(Clone)] pub struct Conn { .. }
impl Conn {
  pub fn from_established(ws: tungstenite::WebSocket<tungstenite::stream::MaybeTlsStream<std::net::TcpStream>>,
                          side: Side /*Client|Server*/, peer: PeerInfo, o: ConnOptions) -> std::io::Result<Conn>;
  pub fn call(&self, op:&str, header: Value, body: Vec<u8>, prio: Priority, deadline: Duration) -> Result<Reply, CallError>;
  pub fn call_streaming(&self, op:&str, header: Value, body: Vec<u8>, prio: Priority, deadline: Option<Duration>, idle: Duration,
                        on_chunk: &mut dyn FnMut(ChunkHeader, &[u8]) -> Result<(), ErrorWire>) -> Result<Reply, CallError>;
  pub fn upload(&self, op:&str, header: Value, prio: Priority, idle: Duration) -> Result<Upload, CallError>;
  pub fn serve(&self, handler: Arc<dyn Handler>);
  pub fn goaway(&self, code:&str, message:&str);  pub fn close(&self);
  pub fn health(&self) -> ConnHealth;  pub fn closed(&self) -> Option<ErrorWire>;  pub fn wait_closed(&self, t: Duration) -> Option<ErrorWire>;
  pub fn peer(&self) -> &PeerInfo;
}
impl Upload { pub fn chunk(&mut self, h: ChunkHeader, bytes:&[u8]) -> Result<(), CallError>; pub fn finish(self) -> Result<Reply, CallError>; }
pub trait Handler: Send + Sync { fn handle(&self, req: Incoming); }
pub struct Incoming { pub id: u64, pub op: String, pub header: Value, pub body: Vec<u8>,
                      pub chunks: Option<std::sync::mpsc::Receiver<(ChunkHeader, Vec<u8>)>>, pub deadline: Instant, pub responder: Responder }
impl Responder { pub fn chunk(&self, h: ChunkHeader, bytes:&[u8]) -> Result<(), CallError>; pub fn progress(&self);
                 pub fn reply(self, header: Value, body: Vec<u8>); pub fn error(self, e: ErrorWire); pub fn cancelled(&self) -> bool; }
pub struct PeerInfo { pub installation: String, pub role: LaneRole, pub lane: Option<LaneSpec>, pub session: String, pub verifier_generation: u64 }
pub type AcceptedPeer = PeerInfo;
pub struct ConnHealth { pub since_ms: u64, pub rtt_ms: Option<u64>, pub last_rx_ms: u64, pub last_tx_ms: u64,
                        pub bytes_up: u64, pub bytes_down: u64, pub inflight: u32, pub queued_bytes: u64 }
// handshake.rs
pub fn dial(endpoint:&str, hello: ClientHello, auth:&dyn ClientAuth, connector: Option<tungstenite::Connector>, o: ConnOptions)
    -> Result<(Conn, ServerHello), DialError>;
pub fn accept(stream: TcpStream, policy: Arc<dyn AcceptPolicy>, o: ConnOptions) -> Result<(Conn, AcceptedPeer), ErrorWire>;
pub fn dial_loopback(address:&str, token: Option<&str>, o: ConnOptions) -> Result<Conn, DialError>;
pub fn accept_loopback(stream: TcpStream, token: Option<&str>, o: ConnOptions) -> Result<Conn, ErrorWire>;
pub fn upgrade_kind(request:&tungstenite::handshake::server::Request) -> UpgradeKind /* V3 | Loopback | Legacy */;
pub enum DialError { Connect(String), NeedsUpgrade{detail:String}, Unsupported{offered:Vec<u32>, supported:Vec<u32>},
                     Unauthorized(ErrorWire), Rejected(ErrorWire), Timeout, Protocol(String) }
pub trait ClientAuth: Send + Sync { fn key(&self, salt:&str) -> Result<[u8;32], ErrorWire>; }   // CodeAuth{code}
pub trait AcceptPolicy: Send + Sync { fn installation(&self) -> String; fn verifier(&self) -> Option<(Verifier, u64)>;
    fn admit(&self, hello:&ClientHello) -> Result<(), ErrorWire>; fn legacy_attempt(&self, addr: SocketAddr, hello:&Value); }
```

```rust
// model::orchestrator (the remaining traits; Ctx is in §3.1)
pub trait CatalogApi: Send+Sync { fn snapshot(&self)->Arc<CatalogSnapshot>;
    fn apply(&self, cmd: CatalogCmd)->Result<Arc<CatalogSnapshot>, ErrorWire>; fn wait_newer(&self, gen:u64, t:Duration)->Arc<CatalogSnapshot>; }
pub struct CatalogSnapshot { pub generation:u64, pub installation:String, pub folders:Vec<FolderRecord>, pub peers:Vec<PeerRecord>,
    pub verifier: Option<Verifier>, pub verifier_generation:u64, pub gates: BTreeMap<String, GateRecord>, pub peer_meta: BTreeMap<String, PeerMeta> }
    // methods: folder(state), folder_by_repo(repo), peer_by_endpoint(ep), peer_by_installation(inst)
pub struct FolderRecord { pub state, pub label, pub repo, pub device: String, pub directory: PathBuf, pub mount: Option<PathBuf>,
    pub desired_running: bool, pub sync_enabled: bool, pub mount_enabled: bool }
pub struct PeerRecord { pub endpoint: String, #[serde(skip_serializing)] pub code: String, pub installation: String, pub connections: u32, pub revision: String }
pub struct GateRecord { pub state: String, pub source_installation: Option<String>, pub source_endpoint: String, pub created_ms: u64, pub override_ms: Option<u64> }
pub struct PeerMeta { pub installation: String, pub last_seen_ms: Option<u64>, pub display_name: Option<String>, pub incompatible: Option<String>, pub revoked_ms: Option<u64> }
pub enum CatalogCmd { CreateFolder{record: FolderRecord, gate: Option<GateRecord>}, SetDesired{state, desired: bool},
    SetMount{state, mount: Option<PathBuf>}, SetIntents{state, sync_enabled: Option<bool>, mount_enabled: Option<bool>},
    OverrideGate{state}, ClearGate{state}, RetargetGate{state, source_installation: Option<String>, source_endpoint: String},
    UpsertPeer{record: PeerRecord}, RemovePeer{endpoint}, SetVerifier{verifier: Verifier},
    RecordPeerSeen{installation, at_ms: u64, display_name: Option<String>, incompatible: Option<String>} }
pub trait FolderDirectory: Send+Sync { fn view(&self, state:&str)->Option<Arc<FolderView>>; fn by_repo(&self, repo:&str)->Option<Arc<FolderView>>;
    fn all(&self)->Vec<Arc<FolderView>>; fn send(&self, state:&str, cmd: FolderCmd)->Result<(), ErrorWire>;
    fn request(&self, state:&str, cmd: FolderCmd, wait: Duration)->Result<Arc<FolderView>, ErrorWire>;
    fn client(&self, state:&str)->Option<Arc<dyn WorkerClient>>; fn generation(&self)->u64; fn wait_newer(&self, gen:u64, t:Duration)->u64;
    fn shutdown(&self, deadline: Duration)->Vec<ShutdownOutcome>; }
pub enum FolderCmd { Start, Stop{mode: StopMode}, ForceStop, Restart, Attach, Detach{force: bool}, SetSync{enabled: bool},
    OverrideGate, Retry, Revalidate{ids: Option<Vec<String>>}, CatalogChanged, GateEvidence(GateEvidence), Tick }
pub struct GateEvidence { pub installation: String, pub at_ms: u64, pub caught_up: bool, pub blocked: bool, pub waiting_on: Vec<String> }
pub enum FolderPhase { Stopped, Starting{since_ms, attempt: u32, phase: String, progress_seq: u64, adopting: bool},
    Running{since_ms, instance: String, pid: u32, address: String, adopted: bool},
    StopBlocked{since_ms, reason: ErrorWire, open_handles: u32, unsaved: u32},
    Stopping{since_ms, stage: StopStage, deadline_ms: u64, force: bool},
    Backoff{until_ms, attempt: u32, last_exit: ExitRecord}, Failed{error: ErrorRecord} }
pub enum GateState { None, Awaiting{source: String, since_ms: u64, waiting_on: Vec<String>}, Blocked{source: String, reason: String},
    Satisfied{at_ms: u64}, Overridden{at_ms: u64} }
pub struct FolderView { pub generation: u64, pub record: FolderRecord, pub phase: FolderPhase, pub gate: GateState, pub mount: MountStatus,
    pub worker: Option<WorkerEndpoint>, pub heartbeat: Option<Arc<WorkerHeartbeat>>, pub last_error: Option<ErrorRecord>,
    pub restarts: u32, pub degraded: bool, pub published_error: Option<ErrorRecord> }
pub struct WorkerEndpoint { pub instance: String, pub pid: u32, pub address: String, pub version: String,
                           #[serde(default)] pub token: Option<String> /* None until WP4.1 */ }
pub struct ExitRecord { pub code: Option<i32>, pub fatal: Option<FatalLine>, pub stderr_tail: String, pub at_ms: u64 }
pub struct ShutdownOutcome { pub state: String, pub outcome: String /* stopped|left_running|killed|not_running */, pub error: Option<ErrorWire> }
pub trait PublishedCatalogApi: Send+Sync { fn update(&self, state:&str, entry: Option<PublishedEntry>);
    fn list(&self)->(u64, Arc<Vec<PublishedEntry>>); fn wait_newer(&self, gen:u64, t:Duration)->u64; }
pub struct PublishedEntry { pub repo: String, pub replica: String, pub label: String, pub branches: Vec<crate::core::Branch>,
    pub state: EntryState /* running|starting|paused|unavailable */, pub tip: Option<FeedTip>, pub sync_generation: u64,
    pub content: Option<ContentCounters>, pub error: Option<ErrorWire>, pub updated_at_ms: u64 }
pub trait StatusSink: Send+Sync { fn record(&self, ev: StatusEvent); }
pub trait StatusSource: Send+Sync { fn snapshot(&self)->Arc<StatusSnapshot>; fn wait_newer(&self, gen:u64, t:Duration)->Arc<StatusSnapshot>; }
pub trait WorkerClient: Send+Sync { fn instance(&self)->&str;
    fn call(&self, op:&str, payload: Value, deadline: Duration)->Result<Value, CallError>;
    fn call_bytes(&self, op:&str, header: Value, body:&[u8], deadline: Duration)->Result<(Value, Vec<u8>), CallError>; }
pub enum CallError { Remote(ErrorWire), Timeout{after_ms: u64}, ConnectionLost(String), Unavailable(String), Busy, Cancelled }
    // is_uncertain() = Timeout|ConnectionLost ; class() ; to_wire()
pub struct LaunchSpec { pub state: String, pub directory: PathBuf, pub executable: PathBuf, pub supervised: bool, pub log: PathBuf, pub env: Vec<(String,String)> }
pub enum ProcessEvent { Line(StartupLine), Exited{code: Option<i32>, fatal: Option<FatalLine>, stderr_tail: String} }
pub type ProcessSink = Arc<dyn Fn(ProcessEvent) + Send + Sync>;
pub trait WorkerProcess: Send { fn pid(&self)->u32; fn kill(&self)->Result<(), ErrorWire>; fn adopted(&self)->bool; }
pub trait WorkerLauncher: Send+Sync {
    fn launch(&self, spec:&LaunchSpec, events: ProcessSink)->Result<Box<dyn WorkerProcess>, ErrorWire>;
    fn adopt(&self, record:&WorkerRecord, events: ProcessSink)->Result<Option<Box<dyn WorkerProcess>>, ErrorWire>; // None: dead or identity mismatch
    fn client(&self, endpoint:&WorkerEndpoint)->Arc<dyn WorkerClient>; }
pub trait NetworkApi: Send+Sync { fn pair(&self, endpoint:&str, code:&str, connections: Option<u32>)->Result<PairResult, ErrorWire>;
    fn remote_list(&self, endpoint:&str)->Result<Vec<PublishedEntry>, ErrorWire>; fn unpair(&self, endpoint:&str)->Result<(), ErrorWire>;
    fn reconfigure(&self); fn shutdown(&self, deadline: Duration); }
pub struct PairResult { pub endpoint: String, pub installation: String, pub connections: u32, pub configured_connections: u32, pub reserved_connections: u32 }
pub trait LegacyNetworkView: Send+Sync { fn folder_sync(&self, folder:&FolderRecord)->Value; fn peers(&self)->Value; }
pub struct RemoteCatalog { pub generation: u64, pub at_ms: u64, pub entries: Vec<PublishedEntry> }
pub struct PeerContext { pub installation: String, pub endpoint: String, pub local_installation: String, pub pool: u32,
                         pub catalog: Snapshot<RemoteCatalog> }
pub struct DaemonEndpoints { pub control: String, pub network: Option<String>, pub log_path: Option<String> }
```

```rust
// model::health
pub trait MountControl: Send+Sync { fn attach(&self, path:&Path)->Result<MountStatus, ErrorWire>;
    fn detach(&self, force: bool)->Result<MountStatus, ErrorWire>; fn status(&self)->MountStatus; /* lock-free */ }
// plus MountState, MountStatus{intent, path, state}, MountIntent, StoreHealth, InboxCounts{ready, waiting, retry, blocked,
// quarantined, unclassified; fn pending_active()}, ActivationCounters, ContentCounters, JournalCounters, Generations,
// EngineGauges, PeerFeedCounters{replica, cursor_seq, remote_tip, to_receive, to_send, staged_pending, blocked,
// quarantined, rejected, caught_up, last_exchange_ms}
```

- **Tasks:**
  1. Write the types above with `serde` derives (snake_case enums; `#[serde(default)]` on every optional field).
  2. Golden JSON fixtures, one per wire, IPC and status type.
  3. The `classify()` table (§8).
  4. `util` implementations (all real, not stubs).
  5. Session stubs (`unimplemented!()` or `Err(..NOT_IMPLEMENTED..)` bodies).
  6. HMAC with RFC 4231 test vectors.
- **Acceptance** (`tests/contracts.rs`):
  - every contract type round-trips and matches its golden fixture byte for byte;
  - the `classify` table covers every grep-found code literal;
  - Backoff bounds over 10,000 samples;
  - `Cancel::sleep` returns ≤ 50 ms after `cancel()`;
  - `Snapshot::wait_newer` wakes ≤ 10 ms;
  - `lock_or_exit` invokes the hook under `Exit`;
  - `Tracked` reports holds > 50 ms;
  - `fault` scope matching;
  - HMAC RFC 4231 cases 1–7.
  - VERIFY passes on Windows and WSL; `cargo build` with `polling` on WSL.
- **Estimate:** ~2,300.

#### WP0.5 Qualification harness, baseline goldens and fixtures

- **Goal:** make every root cause and field failure testable. Capture the baseline behavior the compatibility projection must keep. Provide fault injection and scale fixtures.
- **Owned:**
  - `tests/support/{mod.rs, node.rs, proxy.rs, hung.rs, fixtures.rs, metrics.rs, legacy.rs}`;
  - `tests/harness_selftest.rs`;
  - all `tests/qual_*.rs` (created `#[ignore]`):

    | File | Owner |
    |---|---|
    | `qual_wp1_4_worker.rs` | WP1.4 |
    | `qual_wp1_5_mount.rs` | WP1.5 |
    | `qual_wp1_6_activation.rs` | WP1.6 |
    | `qual_wp1_7_derive.rs` | WP1.7 |
    | `qual_wp1_8_objects.rs` | WP1.8 |
    | `qual_wp1_9_session.rs` | WP1.9 |
    | `qual_wp1_12_legacy.rs` | WP1.12 |
    | `qual_wp2_1_lifecycle.rs` | WP2.1 |
    | `qual_wp2_3_worker.rs` | WP2.3 |
    | `qual_wp2_4_offlock.rs` | WP2.4 |
    | `qual_wp2_5_feed.rs` | WP2.5 |
    | `qual_wp3_1_transport.rs` | WP3.1 |
    | `qual_wp3_2_objects.rs` | WP3.2 |
    | `qual_wp3_int_e2e.rs` | WP3.INT |
    | `qual_scale.rs` | WP4.2 |

  - `tests/fixtures/legacy/**`, `tests/fixtures/compat/*.json`;
  - `scripts/qualify.ps1`, `scripts/qualify.sh`, `scripts/suite-lock.ps1`;
  - `test-evidence/qualification-baseline.json`.
- **Depends on:** none (built and run on the baseline code).
- **Tasks:**
  1. `support/mod.rs` starts with `#![allow(dead_code)]` and re-exports the helpers.
  2. `node.rs`: daemon `Node` (generalized from `tests/orchestrator.rs:16-160`):
     - `NodeConfig{interval, connections, listen, env}`;
     - `raw_call(json)`, which sends arbitrary action JSON over `ws::connect` + `ws::call`, so qual files compile before new `Action` variants exist;
     - `kill()`, `restart()`, and a `daemon.log` and `worker.log` dump on timeout.
  3. `proxy.rs`: `FaultProxy::start(target)` with `set_latency`, `set_bandwidth`, `stall_for`, `stall_after(bytes, d)`, `reset_all`, `reset_after(bytes)`, `blackhole`, `refuse`, and byte and connection counters.
  4. `hung.rs`: `HungServer` (accepts the WS upgrade, reads, never replies); `redirect_runtime_json(state, addr)` points a worker discovery at it.
  5. `fixtures.rs` (seeded):
     - `linear_history(store, n)`;
     - `concurrent_histories(stores, ops, seed)`;
     - **`two_way_backlog(a, b, n)`**: A authors n creates while B authors one unseen local edit;
     - `staged_inbox(store, n)`;
     - `small_objects(engine, count, size)`;
     - `large_object(engine, bytes)` (streamed from a PRNG).
  6. `metrics.rs`: stopwatch, percentiles, per-PID CPU (Windows `GetProcessTimes` FFI, Linux `/proc`), and `QUAL {json}` line emitter.
  7. `legacy.rs` plus `tests/fixtures/legacy/`. Captured **with the baseline binary**, each ≤ 2 MB:
     - a worker state with resolved and unresolved conflicts, staged active and quarantined rows, a partial spool and `pending_saves`;
     - an orchestrator data dir with projects, peers, `initial_sync` and a verifier.

     `copy_legacy_worker()` and `copy_legacy_datadir()` return temp copies.
  8. `tests/fixtures/compat/*.json`: baseline daemon responses for Create (starting), Inspect (running headless; gated received; mounted), List, Peers (dialer and acceptor), Stop and Shutdown. Volatile values are normalized to type markers. `support::assert_same_shape(golden, actual)`.
  9. Qual scenario files written against dynamic JSON (`raw_call`, worker `rpc`), each `#[ignore = "WPX.Y"]`. Scenario list in §14.6.
  10. `scripts/qualify.ps1 -Wave N`: runs the release qual files, parses `QUAL` lines, compares them with `tests/qualification-floors.json` (created here with the floors of §15, each tagged with its enforcing wave), and writes evidence. `qualify.sh` is the Linux subset. `suite-lock.ps1` takes a machine-wide named mutex around `cargo test`.
  11. Run the baseline and record `qualification-baseline.json`, including which scenarios fail or time out.
- **Acceptance:**
  - `harness_selftest`: the proxy relays, stalls, caps bandwidth within 20 % and resets; `HungServer` never replies; fixtures are deterministic per seed; legacy fixtures open with the current build; compat goldens re-capture identically.
  - All qual files compile and are ignored (default suite time unchanged).
- **Estimate:** ~2,200 (test code).

#### WP0.INT — merge and land every seam stub

- **Merge order:** 0.1, 0.2, 0.3, 0.4, 0.5.
- **Seams landed** (exact signatures; each file begins with `//! Owned by WPx.y — stub landed by WP0.INT`):

| File | Content | Implemented by |
|---|---|---|
| `src/runtime/ctx.rs` | `WorkerCtx` (below), `SyncStorePool`, `ObjectLease`, `EngineGaugeCell` | frozen; WP1.4 constructs |
| `src/runtime/sync_rpc.rs` | `pub fn dispatch(ctx:&WorkerCtx, op:&str, payload:&Value)->anyhow::Result<Value>` → `bail!("UNSUPPORTED_OPERATION: {op}")` | WP2.5 |
| `src/runtime/objects_rpc.rs` | `pub fn dispatch(ctx:&WorkerCtx, op:&str, header:&Value, body:&[u8])->Result<(Value, Vec<u8>), ErrorWire>` → `UNSUPPORTED_OPERATION` | WP3.2 |
| `src/worker/mount_ctl.rs` | `pub struct MountController`; `MountController::new(engine: Shared, previous: Option<WorkerRecord>)->Arc<Self>`; `impl MountControl` delegating to `mount::start`/`mount::stop` (today's semantics) | WP1.5 |
| `src/worker/{record,heartbeat,activator,status_cell,maintenance}.rs` | empty | WP1.4 / WP2.3 |
| `src/orchestrator/{catalog,published}.rs` | empty | WP1.1 |
| `src/orchestrator/{folder,registry}.rs` | empty | WP1.2 |
| `src/orchestrator/{process,worker_client}.rs` | empty | WP1.3 |
| `src/orchestrator/{hub,hub_rules}.rs` | empty | WP1.11 |
| `src/orchestrator/{ctx,shutdown,legacy_bridge}.rs` | empty | WP2.1 |
| `src/orchestrator/manage.rs` | `pub fn manage(ctx:&Arc<Ctx>, action: super::api::Action)->anyhow::Result<Value>` → `bail!("NOT_IMPLEMENTED")` | WP2.1 |
| `src/orchestrator/manage_v2.rs` | `pub fn handle(ctx:&Arc<Ctx>, action: super::api::Action)->anyhow::Result<Value>` → `bail!("NOT_IMPLEMENTED")` (`status`, `subscribe`, `attach`, `detach`, `pause-sync`, `resume-sync`, `override-gate`, `force-stop`, `retry`, `revalidate`, `diagnostics`) | WP2.2 |
| `src/orchestrator/api.rs` | **Action variants added** (frozen): `Status`, `Subscribe{since:u64, #[serde(default)] wait_ms:Option<u32>}`, `Attach{state}`, `Detach{state, #[serde(default)] force:bool}`, `PauseSync{state}`, `ResumeSync{state}`, `OverrideGate{state}`, `ForceStop{state}`, `Retry{state}`, `Revalidate{state, #[serde(default)] ids:Option<Vec<String>>}`, `Diagnostics`; `Stop{state, #[serde(default)] wait:Option<bool>}`. The legacy `manage()` answers new variants with `NOT_IMPLEMENTED` (explicit arms; no `unreachable!`). | frozen |
| `src/orchestrator/peer.rs` | empty | WP2.6 |
| `src/orchestrator/scheduler.rs` | `pub fn run_control(ctx:Arc<Ctx>, peer:Arc<PeerContext>, conn:session::Conn, cancel:Cancel)` → `cancel.wait()` | WP2.7 |
| `src/orchestrator/meta_exchange.rs` | empty | WP2.7 |
| `src/orchestrator/objects.rs` | `pub fn run_bulk_lane(ctx:Arc<Ctx>, peer:Arc<PeerContext>, lane:LaneSpec, conn:session::Conn, cancel:Cancel)` → `cancel.wait()` | WP2.7 (interim), WP3.3 |
| `src/orchestrator/inbound.rs` | empty | WP2.8 |
| `src/orchestrator/inbound_objects.rs` | `pub fn handle(ctx:&Arc<Ctx>, peer:&session::AcceptedPeer, req:session::Incoming)` → replies `UNSUPPORTED_OPERATION` | WP2.8, WP3.3 |
| `src/orchestrator/compat.rs` | empty | WP3.1 |
| `src/orchestrator/diagnostics.rs` | `pub fn collect(ctx:&Arc<Ctx>)->anyhow::Result<Value>` → `Ok(json!({"schema":1}))` | WP4.2 |
| `src/ui_model.rs` | empty; `pub mod ui_model;` in `lib.rs` | WP3.4 |

```rust
// src/runtime/ctx.rs (frozen after WP0.INT)
pub struct WorkerCtx {
    pub engine: crate::runtime::Shared,
    pub root: PathBuf, pub state: PathBuf, pub repo: String, pub device: String,
    pub instance: String, pub started_at_ms: u64, pub supervised: bool, pub version: String,
    pub previous: Option<crate::model::ipc::WorkerRecord>,
    pub locks: Arc<crate::core::objects::ObjectLocks>,
    pub mount: Arc<dyn crate::model::health::MountControl>,
    pub activation: Arc<crate::util::notify::Notify>,
    pub activator_running: std::sync::atomic::AtomicBool,
    pub status_generation: std::sync::atomic::AtomicU64,
    pub heartbeat: crate::util::snapshot::Snapshot<crate::model::ipc::WorkerHeartbeat>,
    pub status_cell: crate::util::snapshot::Snapshot<serde_json::Value>,
    pub sync_obs: std::sync::Mutex<BTreeMap<String, crate::model::health::PeerFeedCounters>>,
    pub sync_stores: SyncStorePool,                                 // checkout()->PooledStore (Deref<Target=Store>), max 8
    pub inflight_objects: std::sync::Mutex<BTreeMap<String, ObjectLease>>,
    pub stopping: Arc<std::sync::atomic::AtomicBool>,
    pub last_contact_ms: std::sync::atomic::AtomicU64,
    pub gauges: EngineGaugeCell,                                    // record_wait(ms), record_hold(ms), snapshot()->EngineGauges
}
pub struct ObjectLease { pub session: String, pub expires_ms: u64, pub offset: u64 }
```

- **Acceptance:** full suite; legacy fixtures; compat goldens; the qualify baseline is reproduced.
- **Estimate:** ~700.

---

### Wave 1 — relief on the legacy path, and standalone components

The legacy orchestrator (Supervisor, legacy lanes) stays live and wire-compatible. New orchestrator components are built and tested **standalone** against the model traits; WP2.1 wires them.

#### WP1.1 Catalog actor and published-catalog cache

- **Goal:** single-owner catalog with immutable snapshots; an O(1) published-folder cache.
- **Owned:** `src/orchestrator/catalog.rs`, `src/orchestrator/published.rs`, `tests/catalog_actor.rs`.
- **Depends on:** WP0.INT.
- **Provides:**
  - `pub fn Catalog::open(config:&Config) -> Result<Arc<Catalog>>`, implementing `CatalogApi`;
  - `pub fn validate_mount_path(snap:&CatalogSnapshot, data_dir:&Path, state_dir:&Path, requested:&Path) -> Result<PathBuf, ErrorWire>`;
  - `pub struct PublishedCatalog`, implementing `PublishedCatalogApi`, with `PublishedCatalog::new() -> Arc<Self>`.
- **Tasks:**
  1. Thread `tkfs-catalog`:
     - acquire `orchestrator.lock` (`orchestrator::lock`), open `folders.sqlite` with the existing pragmas, and run the legacy `CREATE IF NOT EXISTS` statements;
     - additive migrations guarded by `settings('catalog_schema','1')`: `folder_intents`, `mount_gates` (backfilled from `initial_sync`, resolving `endpoint` → `installation` through `peers`), `peer_meta`, and `settings('verifier_generation')`.
  2. Implement every `CatalogCmd` in one transaction, using cached prepared statements.
     - `CreateFolder` inserts the positional `projects` row (identical SQL), plus `folder_intents`, plus the optional `initial_sync` and `mount_gates` rows.
     - `ClearGate` deletes from both `initial_sync` and `mount_gates`.
     - `SetVerifier` bumps `verifier_generation`.
  3. `catch_unwind` per command; 3 retries on `SQLITE_BUSY`; errors returned as `ErrorWire`. If the thread has died, `apply` returns `CATALOG_UNAVAILABLE` and `snapshot()` keeps the last good snapshot.
  4. Publish the snapshot (sorted as the legacy queries are) and wake waiters.
  5. `PublishedCatalog`: a `Mutex<BTreeMap<state, PublishedEntry>>`, a generation counter, a cached `Arc<Vec>`, and `wait_newer`.
- **Acceptance** (`tests/catalog_actor.rs`):
  - the legacy datadir fixture opens with `projects`, `peers`, `initial_sync` and verifier equal to the legacy `Supervisor` view;
  - downgrade safety: after migration, the legacy `Supervisor::open`, `create()` and `Pair` SQL still work;
  - `snapshot()` p99 < 1 ms while a command is held by `TKFS_FAULT_POINT=catalog.slow_write` (500 ms);
  - a panicking command errors and the next one succeeds;
  - the generation increments exactly once per successful mutation;
  - `validate_mount_path` matches the legacy `mount_path` on 12 fixture cases;
  - `wait_newer` wakes ≤ 10 ms.
- **Invariants at risk:** 23, 26.
- **Estimate:** ~1,300.

#### WP1.2 FolderController and FolderRegistry

- **Goal:** per-folder lifecycle actors that never block the API or other folders (§3.3).
- **Owned:** `src/orchestrator/folder.rs`, `src/orchestrator/registry.rs`, `tests/folder_controller.rs`.
- **Depends on:** WP0.INT. Consumes only model traits; WP1.3 provides the real launcher.
- **Provides:**

```rust
pub struct RegistryDeps { pub catalog: Arc<dyn CatalogApi>, pub published: Arc<dyn PublishedCatalogApi>, pub status: Arc<dyn StatusSink>,
    pub launcher: Arc<dyn WorkerLauncher>, pub clock: Arc<dyn Clock>, pub limits: LifecycleLimits }
pub struct LifecycleLimits { pub executable: PathBuf, pub maximum_running: usize, pub startup_silence: Duration /*30s*/,
    pub stop_graceful: Duration /*5s*/, pub stop_exit_wait: Duration /*10s*/, pub backoff_base: Duration /*1s*/, pub backoff_cap: Duration /*300s*/,
    pub backoff_reset_after: Duration /*600s*/, pub breaker_failures: u32 /*5*/, pub breaker_window: Duration /*600s*/,
    pub heartbeat_timeout: Duration /*8s*/, pub hello_timeout: Duration /*2s*/, pub adopt_patience: Duration /*30s*/,
    pub attach_deadline: Duration /*60s*/, pub mount_retry_base: Duration /*2s*/, pub mount_retry_cap: Duration /*120s*/,
    pub shutdown_kill_starting: bool /*true*/ }
impl FolderRegistry { pub fn start(deps: RegistryDeps) -> Arc<FolderRegistry>; }   // implements FolderDirectory
```

- **Tasks:**
  1. Controller mailbox and phases exactly as in §3.3. Watchdog via `crate::startup::Watchdog`. Validate Ready (repo, device, `ipc == 3`, else `WORKER_IPC_MISMATCH`).
  2. Backoff and circuit breaker. Classify exits from the exit code and the FatalLine class.
  3. Stop ladder, StopBlocked, ForceStop, Shutdown outcomes. `desired=0` is persisted before the stop RPC.
  4. Watch thread: a dedicated client, `watch{after_seq, wait_ms:2000}`; liveness ladder (no heartbeat 8 s → hello 2 s → verified kill).
  5. Adoption through `launcher.adopt(record, sink)`, then hello identity and version checks. Legacy orphan path when there is no `worker.json`.
  6. Gate state machine and mount attach orchestration (`mount-attach` via `WorkerClient`). Clear the gate only after `Attached`. Retarget on unpair (on `CatalogChanged` when the gate source peer is gone).
  7. Published entries: `published-summary` on Ready and on `generations.branches` change; tip and content from the heartbeat.
  8. `StatusEvent::FolderLifecycle` and `WorkerHeartbeat` emission.
  9. Registry: `state → FolderHandle{tx, Snapshot<FolderView>}`, a by-repo index, controllers spawned and retired on catalog changes, an aggregate generation, `maximum_running` enforcement (`WORKER_LIMIT`), and `shutdown(deadline)` in parallel.
- **Acceptance** (`tests/folder_controller.rs`, with `FakeLauncher`, `FakeWorkerClient` and `ManualClock` defined inside the test file):
  - start → ready → running;
  - Ready mismatch → `Failed{WORKER_STATE_MISMATCH}`;
  - backoff schedule within jitter bounds; breaker after 5 failures → `Failed{CRASH_LOOP}`; Retry recovers;
  - watchdog expiry kills and backs off;
  - hung watch → Unresponsive → kill → restart;
  - stop ladder: graceful; `BUSY_VIEW` → StopBlocked with retries; ForceStop; a never-exiting worker is killed; the fake catalog records `desired=0` before the stop RPC;
  - Shutdown of 5 controllers (one hung, one busy) finishes by the deadline with outcomes `stopped`, `left_running` and `killed` (Starting only);
  - adoption: match → `Running{adopted}` with no spawn; version mismatch → stop then spawn; silent → Failed after 30 s; foreign image → no kill;
  - gate: no attach while Awaiting; evidence → attach → `ClearGate` only on `Attached`; a failed attach keeps the marker; NeedsUserAction attach → no retry; Transient retried;
  - `FolderDirectory::send` returns in < 5 ms while the controller is mid-stop.
- **Invariants at risk:** 8, 23, 26, 29.
- **Estimate:** ~1,900.

#### WP1.3 Worker process launcher, adoption FFI, JSON worker client

- **Goal:** real process control and the loopback client behind the model traits.
- **Owned:** `src/orchestrator/process.rs`, `src/orchestrator/worker_client.rs`, `tests/worker_process.rs`.
- **Depends on:** WP0.INT.
- **Provides:** `pub struct RealLauncher` (`RealLauncher::new(log_dir_policy) -> Arc<Self>`, implementing `WorkerLauncher`); `pub struct JsonWorkerClient` (`JsonWorkerClient::new(endpoint: WorkerEndpoint) -> Arc<Self>`, implementing `WorkerClient`); `pub fn process_identity(pid:u32) -> Option<(u64 /*process_start*/, PathBuf /*image*/)>`.
- **Tasks:**
  1. **`RealLauncher::launch`:**
     - spawns `current_exe daemon --state S --supervised` with `CREATE_NO_WINDOW` (0x08000000) on Windows, stdin null, stdout piped, stderr appended to `worker.log`;
     - a stdout reader thread parses `StartupLine` (`model::ipc::parse_line`), copies every line to `worker.log`, and calls the sink;
     - an exit-waiter thread owns the `Child` and serves `kill` requests through a channel. On exit it sends `ProcessEvent::Exited{code, fatal: last FatalLine, stderr_tail: last 4 KiB of worker.log}`.
  2. **`adopt(record)`:**
     - Windows: `OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION|SYNCHRONIZE|PROCESS_TERMINATE)`, `GetExitCodeProcess`, `GetProcessTimes`, `QueryFullProcessImageNameW`.
     - Unix: `kill(pid,0)`, `/proc/<pid>/stat` field 22, `/proc/<pid>/exe`.

     Returns `None` unless pid, `process_start` and executable all match the record. An adopted process has a 1 Hz liveness probe thread emitting `Exited`. `kill()` re-verifies identity before `TerminateProcess` or `SIGKILL`.
  3. **`JsonWorkerClient`:**
     - a pool of ≤ 8 `ws::Socket`, checked out per call and never held across a lock;
     - per-call read/write timeout equals the deadline; any non-Remote error drops the socket;
     - maps errors to `CallError` (`RemoteError` → `Remote(to_wire)`, timeout → `Timeout`, other → `ConnectionLost`, connect failure → `Unavailable`);
     - `call_bytes` → `Unavailable("BINARY_OPS_REQUIRE_SESSION")` (replaced in WP3.3).
- **Acceptance** (`tests/worker_process.rs`):
  - the stdout parser on scripted fake children (`cmd /c` and `sh -c` echoing Ready/Fatal lines and exit codes);
  - `worker.log` is appended;
  - the exit event carries the fatal line and stderr tail;
  - `process_identity` on its own pid and on a spawned sleeper; adoption returns `None` for a dead pid or mismatched start time; kill refuses a mismatched identity;
  - `JsonWorkerClient` against `runtime::start_rpc` (legacy wrapper): a Remote error keeps the socket, a timeout drops it, an uncertain request is not replayed.

  Real `--supervised` launch is checked in WP1.INT, because the flag lands in WP1.4.
- **Invariants at risk:** 16, 23.
- **Estimate:** ~1,400.

#### WP1.4 Worker lifecycle: RPC first, worker.json, hello/watch/stop, crash-only internals

- **Goal:** worker readiness means store recovered plus RPC up. Mount failures are reported, never fatal. Wedged internals exit cleanly (§4.1).
- **Owned:**
  - `src/worker/mod.rs`, `src/worker/record.rs`, `src/worker/heartbeat.rs`;
  - `src/runtime/server.rs`, `src/runtime/engine.rs`, `src/runtime/mod.rs`;
  - `src/main.rs` (Daemon flags), `src/startup.rs` (FatalLine helper);
  - `tests/worker_lifecycle.rs`, `tests/qual_wp1_4_worker.rs`.
- **Depends on:** WP0.INT. Uses `ctx.mount` (`MountControl`) and the WP0.INT `mount_ctl` stub; WP1.5 replaces its internals.
- **Provides:**
  - `pub struct WorkerArgs { pub state: PathBuf, pub mount: Option<PathBuf>, pub supervised: bool }`;
  - `pub fn worker::run(args: WorkerArgs) -> Result<()>`;
  - `pub fn runtime::server::start_rpc_ctx(ctx: Arc<WorkerCtx>) -> Result<Discovery>`;
  - `runtime::start_rpc(engine, state, mount)` kept as a wrapper that builds a minimal `WorkerCtx` (legacy semantics, no activator);
  - `worker::record::{write_atomic, read, remove}`.
  - Ops served: `hello`, `watch`, `stop{mode}`, `mount-attach`/`detach`/`status` (delegating to `ctx.mount`), and prefix routing (`feed-` → `sync_rpc::dispatch`, `objects.` → `objects_rpc::dispatch`).
  - `revalidate`: the arm returns `UNSUPPORTED_OPERATION` in this package. `Store::revalidate` comes from WP1.6 in the same wave, so WP1.INT wires the call.
- **Tasks:**
  1. The startup sequence of §4.1.
     - `--supervised` flag; `ReadyLine` and `FatalLine` printing.
     - `worker.json` written atomically: `process_start` from `GetProcessTimes` or `/proc/self/stat`, and `executable`.
     - Legacy mode: synchronous attach when `--mount` is given without `--supervised`, reporting the actual `mounted` and `mount_error`.
  2. `runtime::lock_engine` gets the poison policy: `PoisonPolicy::Exit` (set in `worker::run`) does `into_inner` → `retain_all_dirty` → `util::log::event!(fatal, ENGINE_POISONED)` → `exit(70)`. Tests use Panic. All `.lock().unwrap()` on the engine in `runtime/*` become `lock_engine`.
  3. **Accept loop:** never breaks; log, sleep 100 ms; 50 consecutive errors → exit 70. `catch_unwind` per request. `deliver_notifications` is removed from the reply path; the existing 250 ms recovery thread keeps calling it.
  4. **`stop{mode}`:**
     - graceful = `quiet()` → `ctx.mount.detach(false)` → set stopping;
     - force = `ctx.mount.detach(true)` → `retain_all_dirty` → set stopping;
     - reply `{stopped, persisted_dirty, detached}`;
     - main waits on a reply-flushed flag (≤ 2 s) instead of a 100 ms sleep.
  5. **Heartbeat thread (basic):** `{instance, seq, at_ms, uptime_ms, mount: ctx.mount.status(), engine gauges, store_health, pending_saves, open_handles, unflushed_files}` from atomics updated by `lock_engine` and `publish_observation`. Never takes the engine lock. `watch` waits on `ctx.heartbeat.wait_newer`.
  6. Supervised idle exit (`TKFS_UNSUPERVISED_EXIT_S`, default 600): with no hello or watch contact, stop gracefully if quiet.
  7. Fault hooks: `fault::set_scope(state path)` at startup. Points: `worker.accept_error`, `engine.poison`, `worker.watch_hang`, `worker.startup` (exit), and `worker.rpc.<op>` via `fault::trigger`, whose action comes from `TKFS_FAULT_ACTION`.
- **Acceptance** (`tests/worker_lifecycle.rs` and `qual_wp1_4_worker` un-ignored):
  - real binary with an existing non-empty mount path → Ready printed with `mounted:false` and `mount_error.code == MOUNT_PATH_MUST_NOT_EXIST`; `status`, `published-summary` and `import` work;
  - `rpc_after_recovery` (no listener before `Store::open` returns, via `TKFS_FAULT_POINT=worker.startup` timing);
  - graceful stop with an open session → `BUSY_VIEW` and the process stays alive; force stop with a dirty session → `pending_saves` row, exit 0, and a restart recovers the bytes;
  - `accept_error:3` → still serving; `engine.poison` → exit 70 and `pending_saves` holds the dirty session;
  - `watch` returns `seq+1` within 1.5 s;
  - `ReadyLine` and `WorkerRecord` match the goldens.
  - Existing `runtime_lanes`, `availability`, `partial_content` and orchestrator tests are unchanged.
- **Invariants at risk:** 8, 24, 28, 29.
- **Estimate:** ~1,700.

#### WP1.5 MountController and adapters (WinFsp, FUSE)

- **Goal:** a re-attachable mount whose failures stay mount-scoped (§4.3).
- **Owned:**
  - `src/worker/mount_ctl.rs`, `src/mount.rs`, `src/fuse_linux.rs`;
  - `native/mount.c` (only if needed; expected unchanged);
  - `tests/mount_lifecycle.rs`, `tests/qual_wp1_5_mount.rs`.
- **Depends on:** WP0.INT.
- **Provides:**
  - `MountController::new(engine, previous) -> Arc<Self>`, implementing `MountControl` with real semantics;
  - `pub fn mount::bind(engine:&Shared) -> Result<()>`, `pub fn mount::attach(path:&Path) -> Result<()>`, `pub fn mount::detach(force: bool) -> Result<()>`, `pub fn mount::inflight() -> usize` (WinFsp and FUSE with the same names);
  - `mount::checkout(engine, request, payload)` keeps its signature.
- **Tasks:**
  1. The state machine and preflight classification. FUSE `recover_managed` runs only when `previous` is a record of ours **and** the mount is a stale `fuse.tkfs-<instance>`.
  2. **WinFsp:**
     - `bind` sets `ENGINE` once (idempotent, `Arc::ptr_eq`) independent of path checks (fixes `mount.rs:72`);
     - `attach` creates a fresh native instance;
     - `tk_call` increments and decrements an in-flight `AtomicUsize`;
     - detach order: fence → drain (≤ 10 s) → [force: `retain_all_dirty`] → `tk_mount_stop` → clear handles and sessions → clear fence;
     - the close-path `e.health` write (`:339-347`) becomes `mount_errors` counter plus a rate-limited log;
     - `checkout` remount failure sets `MountState::Lost` (no `e.health`).
  3. **FUSE:** attach through `start_managed(engine, path, instance)`; dispatch death → `Lost` (not `e.health`); same fence and drain semantics; unmount refusal → `BUSY_VIEW`.
  4. Notification delivery only while Attached (gate in `deliver_notifications` stays in `runtime/notify.rs`; the controller sets `e.mounted`).
- **Acceptance** (`tests/mount_lifecycle.rs`; WinFsp on Windows, `/dev/fuse` in WSL, skip otherwise):
  - attach → write → detach → attach on the same pid; the file is visible;
  - graceful detach with an open handle → `BUSY_VIEW`, still Attached;
  - force detach with a dirty session → `pending_saves` row, Detached, re-attach shows the saved bytes after recovery;
  - a pre-existing non-empty path → `AttachFailed{NeedsUserAction}`, and engine health stays `None`;
  - a stale owned FUSE mount is recovered; a foreign mount or directory is untouched (`foreign_mount_untouched`);
  - a `fs.close` fault → no store health change, and stop still works;
  - existing `real_mount_*` orchestrator tests stay green (legacy synchronous path).
- **Invariants at risk:** 8, 27.
- **Estimate:** ~1,500.

#### WP1.6 Core inbox state machine, remote fast paths, classified quarantine

- **Goal:** O(delta), preemptible activation; remote linear catch-up on fast paths; quarantine stops bricking pending counts (§4.4, §5.1).
- **Owned:** `src/core/inbox.rs`, `src/core/commit.rs`, `src/paired_sync.rs`, `tests/activation_inbox.rs`, `tests/qual_wp1_6_activation.rs`.
- **Depends on:** WP0.INT.
- **Provides** (`impl Store`, plus types in `core::inbox`):

```rust
pub struct StagedEvent { pub event: Event, pub source: Option<(String /*peer replica*/, i64 /*seq*/)> }
pub struct StageOutcome { pub staged: Vec<String>, pub already_committed: Vec<String>, pub already_staged: Vec<String>,
                          pub rejected: Vec<(String, ErrorWire)> }
pub struct SliceBudget { pub max_events: usize, pub max_ms: u64, pub inline_slow: bool }
pub struct SliceOutcome { pub activated: Vec<String>, pub fast: u32, pub slow: u32, pub quarantined: Vec<(String, ErrorWire)>,
                          pub slow_ticket: Option<SlowTicket>, pub more: bool }
pub struct SlowTicket { pub id: String, pub branch: String, pub base_seq: i64 }
pub enum Prepared { Inline(SlowTicket), Computed(Box<PreparedCommit>) }   // PreparedCommit: opaque, WP2.4 fills
pub enum CommitOutcome { Committed { id: String, slow: bool }, Stale, Failed(ErrorWire) }
fn stage_incoming(&self, events: Vec<StagedEvent>, allowed: &BTreeSet<String>) -> Result<StageOutcome>;
fn activate_slice(&mut self, b: SliceBudget) -> Result<SliceOutcome>;
fn prepare_slow(read: &Store, t: &SlowTicket) -> Result<Prepared>;          // W1: Ok(Prepared::Inline(t.clone()))
fn commit_prepared(&mut self, p: Prepared) -> Result<CommitOutcome>;         // W1: Inline → commit_ready under caller's lock
fn inbox_counts(&self) -> Result<InboxCounts>;
fn missing_parents(&self, limit: usize) -> Result<Vec<String>>;
fn quarantined(&self, limit: usize) -> Result<Vec<(String /*id*/, String /*code*/, String /*message*/)>>;
fn revalidate(&mut self, ids: Option<&[String]>) -> Result<usize>;
fn requeue(&mut self, id: &str, delay_ms: u64) -> Result<()>;   // ready row -> retry with retry_at (used for Stale)
fn branch_has_unresolved_conflicts(&self, branch: &str) -> Result<bool>;
pub fn remote_fast_paths_enabled() -> bool;   // TKFS_REMOTE_FASTPATH != "0", read once (OnceLock)
```

- **Tasks:**
  1. **`inbox::ensure_schema`** (`schema:inbox=1`): `incoming_events` `ADD COLUMN` (guarded by `pragma table_info`); `incoming_waits`; `incoming_sources`; indexes on `incoming_waits(parent)` and `incoming_events(state)`; an `AFTER DELETE` trigger cleaning waits and sources.
  2. **`stage_incoming`:**
     - page-level checks as today (`REPOSITORY_MISMATCH`, `UNAUTHORIZED_EVENT`, `PRIVATE_EVENT_REJECTED`, `EVENT_ID_REUSED` against committed or staged payloads);
     - per-event validation failures become `rejected`;
     - each row is set to `ready`, `waiting` (with waits) or `blocked` (quarantined ancestor);
     - records `incoming_sources`.
  3. **`activate_slice`:**
     - `received_ms, rowid` order; window of 512; classify NULL rows first;
     - point-query parent presence; fast path or inline slow path (≤ 1 per slice when `inline_slow`, else return a `SlowTicket`);
     - per-event nested savepoint inside the slice savepoint;
     - release children (waits → ready);
     - budget measured after the window load;
     - classification: validation → `quarantined` (also sets `error` text) and descendants → `blocked` (`error` set to `QUARANTINED_CAUSAL_ANCESTOR: <id>`); `SQLITE_BUSY`/`LOCKED`/IO → `retry` with `retry_at = now + min(300 s, 2^attempts s)`; `CausalPrerequisitesPending` → `waiting`.
  4. **Compatibility:** `receive_with_budget(bundle, allowed, budget)` = stage, then slices until `budget` events (`Some(32)` keeps ≤ 32 activations, `max_ms` 50). `receive` and `drain_incoming` loop until no ready rows. `startup_drain(defer)`: `defer=false` activates (progress per slice); `defer=true` only classifies.
  5. **Remote fast paths** and `branch_has_unresolved_conflicts` (§5.1); `TKFS_REMOTE_FASTPATH`. If the oracle finds a mismatch, fall back to any conflict row of the **event's** branch.
  6. **`paired_sync`:**
     - `pending_events` and local pending = `inbox_counts().pending_active()` (excludes quarantined and blocked; includes waiting, which keeps the `received_mount_waits…` semantics);
     - `caught_up` from that;
     - remove `content_status()` from `receive_for_lane` and `published-apply` (the result `content` becomes `{incoming: InboxCounts, outgoing_unacknowledged}`).
  7. Keep the forced snapshot seal after slow-path commits (unchanged).
- **Acceptance** (`tests/activation_inbox.rs` and `qual_wp1_6_activation` un-ignored):
  - children delivered before parents become ready exactly when the parents commit;
  - a quarantined event is excluded from pending and `caught_up`, unrelated history activates, and descendants are blocked with a reason; `revalidate` re-queues;
  - an injected `SQLITE_BUSY` (`TKFS_FAULT_POINT=core.commit.busy`) is retried and committed, never quarantined;
  - the budget excludes window load (a cheap-event slice activates `min(N, 32)`);
  - the oracle is empty after every activation across 100 seeded two-replica scripts (create, rename, delete, move, resolve) with remote fast paths on;
  - `TKFS_REMOTE_FASTPATH=0` gives identical results;
  - release: remote linear create at 2K events ≤ 5 ms per event (baseline 15.9 s); a local create after only resolved conflicts takes the fast path (counter);
  - legacy fixture rows (`state` NULL) are classified and drained;
  - existing `replication_regressions`, `acknowledgements`, `runtime_lanes` (≤ 32 per round) and `startup_snapshots` are unchanged.
- **Invariants at risk:** 4, 9, 10, 11.
- **Estimate:** ~2,000.

#### WP1.7 Append-mode conflict derivation (oracle-gated)

- **Goal:** two-way and concurrent activation in about O(N) per event, with identical results and refusals (§5.2).
- **Owned:** `src/core/derive.rs`, `tests/derive_oracle.rs`, `tests/qual_wp1_7_derive.rs`.
- **Depends on:** WP0.INT.
- **Provides:**
  - `pub enum ConflictMode { Append, Full, Verify }` and `pub fn conflict_mode() -> ConflictMode` (`TKFS_CONFLICT_MODE`, OnceLock, default `Append`);
  - `persist_projection` keeps its signature;
  - an internal `NsMode`.
- **Tasks:**
  1. Location pairs, reviewed cuts and namespace history in append mode, with the §5.2 exactness precondition and full fallback. Counting over all antichains of touched old cones; new cones fully derived.
  2. Bitset memoization of `FactGraph::closure`.
  3. `Verify` mode (both modes in savepoints; mismatch → `CONFLICT_MODE_MISMATCH`).
  4. Fuzzer in `tests/derive_oracle.rs`:
     - 2–4 replicas; random create, mkdir, rename, directory moves (including cycles), delete and resurrect (tombstones), content saves with `alive:true`, resolve and recover reviews, temp replacement;
     - random delivery orders with duplicates and restarts;
     - after each commit, `verify_derived_state()` is empty **and** `Store::rebuild` succeeds (refusal parity).

     50 seeds in debug; 300 seeds `#[ignore]` in release.
  5. A refusal-parity test: a constructed history crossing 4,096 competing cuts is refused at the same event in both modes, with no ACK.
- **Acceptance:**
  - fuzzer green;
  - `scalability::compressed_presence_witnesses_match_exhaustive_causal_cuts` and `unrelated_namespace_chains_do_not_multiply_the_topology_budget` green;
  - `replication_regressions::namespace_history_budget_refuses_commit_without_erasing_records` green;
  - qual (release): `two_way_backlog` (A streams 2K creates; B has one unseen local edit) ≤ 100 ms per event at 2K and ≤ 300 ms at 6.4K (baseline 15.9 s / 218 s); 50 concurrent namespace ops on a 5K history ≤ 500 ms each;
  - `TKFS_CONFLICT_MODE=full` passes the same suites.
- **Invariants at risk:** 11 (highest determinism risk; mitigated by precondition, verify mode and fuzzer).
- **Estimate:** ~1,600.

#### WP1.8 Object indexes, startup markers, per-object outcomes

- **Goal:** remove O(repository) sweeps from hot paths; cheap restarts; one bad object no longer discards completed work (§5.3).
- **Owned:**
  - `src/core/objects.rs`, `src/core/demand.rs`, `src/core/status.rs`, `src/core/transfer.rs`;
  - `src/bulk_sync.rs`;
  - `src/runtime/published.rs` (bulk-receive error path only);
  - `tests/availability_index.rs`, `tests/qual_wp1_8_objects.rs`.
- **Depends on:** WP0.INT.
- **Provides** (`impl Store`):
  - `object_available(&self, h) -> bool` (point lookup; same signature);
  - `object_available_checked(&self, h) -> bool` (stat plus heal);
  - `content_counters(&self) -> Result<ContentCounters>`;
  - `content_status(&self) -> Result<Value>` (compatibility shape, ≤ 20 items);
  - `scrub_step(&self, budget_bytes: u64) -> Result<ScrubOutcome{verified, corrupt, bytes}>`;
  - `receive_parts_outcomes(&self, parts, events, allowed, locks) -> Result<Vec<PartOutcome>>`;
  - `pub enum PartOutcome { Completed{object, size}, Appended{object, offset}, Skipped{object}, Failed{object, error: ErrorWire} }`;
  - `record_object_error(&self, h, e:&ErrorWire, retry_at_ms: u64)`;
  - `object_backoff(&self, h) -> Result<Option<u64>>`;
  - `objects_referenced_committed(&self, h) -> Result<bool>`, `objects_referenced_staged(&self, h) -> Result<bool>`.
- **Tasks:**
  1. `objects::ensure_schema` (§9 tables and triggers); chunked, resumable backfills (`availability_v2`, `event_objects_v1`) with progress.
  2. Maintain availability in every write and quarantine path.
  3. `shared_object_inventory` becomes one SQL join; `request_object` becomes `EXISTS` on `event_objects`; `remember_requests` runs in one transaction with no journal parse; `wanted_objects` orders newest demand first with 24 h expiry (from the existing `requested_ms`).
  4. `content_counters` from SQL plus a projection walk with no stat.
  5. `startup_object_check(VerifyMode::Changed)` honours markers. `Store::open_existing` defaults to `Changed` (one-time full verify after upgrade). `Lazy` skips hashing.
  6. Transfer robustness:
     - per-object outcomes (the old function stays as a wrapper);
     - availability re-check after the object lock (skip only if **shareable**-available; a private physically present object is re-transferred and verified, per I4);
     - no spool for `offset > 0` when none exists;
     - drop the redundant second `sync_all`.
  7. `bulk_sync`:
     - `receive` registers completed objects even if a later part fails, and records `object_error_state`;
     - the sender skips objects in backoff;
     - part cap 4,096 and correct overhead accounting;
     - wire bulk protocol 1 unchanged.
  8. `runtime/published.rs`: the bulk-receive error path registers the completed objects first, then writes `object_error_state` (the positional `object_errors` insert stays as is).
- **Acceptance** (`tests/availability_index.rs` and `qual_wp1_8_objects` un-ignored):
  - indexes equal the legacy scans on fixtures (property test, 200 seeds);
  - startup with 2K objects performs 0 hashes (counter) and detects deleted and size-changed files;
  - a deleted object is marked missing on open and the request is queued;
  - a corrupt part mid-page keeps earlier completed objects registered;
  - a duplicate part for a shareable object is not re-hashed;
  - a private physically present object is re-transferred, verified and then registered (`paired_peer_cannot_disclose_private_cas…` including its restart leg stays green);
  - `crash_between_install_and_register_retransfers` (`TKFS_FAULT=object_installed`);
  - 12,000 tiny pending objects produce valid pages;
  - `content_counters` matches `content_status`;
  - release: `content_status` at 50K objects < 50 ms; restart to Ready at 20K objects ≤ 3 s.
  - Existing `availability`, `partial_content`, `bulk_lanes` and `orchestrator` tests are green.
- **Invariants at risk:** 1, 2, 3, 13, 28.
- **Estimate:** ~2,000.

#### WP1.9 Session core: frames, I/O thread, multiplexing

- **Goal:** the v3 connection layer (§6.2) minus the handshake.
- **Owned:** `src/session/mod.rs`, `src/session/frame.rs`, `src/session/io.rs`, `src/session/conn.rs`, `tests/session_core.rs`, `tests/qual_wp1_9_session.rs`.
- **Depends on:** WP0.INT.
- **Provides:** the real implementations of the WP0.4 session API (except `handshake.rs` and `auth.rs`).
- **Tasks:**
  1. Frame codec with all bounds (§6.2).
  2. The I/O thread: non-blocking socket (`Plain` and `NativeTls`), `polling::Poller` (oneshot re-arm), drain reads until `WouldBlock`, priority write queues, notify-wakeups, heartbeat and dead detection, Goaway.
  3. Pending map with absolute and idle deadlines (250 ms scan), CANCEL on timeout or drop.
  4. Server handler pool with a bounded queue (full → `BUSY`).
  5. `call`, `call_streaming`, `upload`, `Responder::chunk` with backpressure.
  6. Body chunking above 1 MiB.
  7. Metrics (`ConnHealth`).
- **Acceptance** (`tests/session_core.rs`, using `Conn::from_established` over in-process tungstenite pairs; WSS via `tests/fixtures/tls`):
  - 1,000 concurrent requests correctly paired;
  - a 20 s handler on stream A does not delay 50 fast calls on stream B (p99 < 50 ms);
  - heartbeat 200 ms / dead 1 s: a FaultProxy 2 s stall is detected and all in-flight calls fail `ConnectionLost`; 60 s idle stays up;
  - a timed-out call sends CANCEL, the handler sees `cancelled()`, and a late reply is dropped;
  - a Remote error keeps the connection;
  - `lost_request_not_replayed`;
  - 32 MiB each way concurrently with no deadlock (plain and WSS);
  - a control call answered < 50 ms while 30 MiB of data is queued;
  - backpressure: queue memory ≤ cap with a slow receiver;
  - protocol violations close the connection.
  - Release: loopback ≥ 300 MiB/s with 1 MiB chunks.
- **Invariants at risk:** 16, 17, 18, 22.
- **Estimate:** ~1,900.

#### WP1.10 Session handshake, HMAC auth, legacy detection

- **Goal:** authenticated, version-negotiated, fail-closed session establishment.
- **Owned:** `src/session/handshake.rs`, `src/session/auth.rs`, `src/ws.rs` (additive helpers only), `src/pairing_code.rs` (key derivation), `tests/session_handshake.rs`.
- **Depends on:** WP0.INT. `Conn::from_established` is the stub until WP1.9 merges; tests cover handshake functions up to the authenticated socket.
- **Provides:**
  - `handshake::{dial, accept, dial_loopback, accept_loopback, upgrade_kind}`;
  - `pub fn client_handshake(ws, hello, auth) -> Result<(WebSocket<..>, ServerHello), DialError>`;
  - `pub fn server_handshake(ws, policy) -> Result<(WebSocket<..>, PeerInfo), ErrorWire>`;
  - `pub struct CodeAuth { pub code: String }`, implementing `ClientAuth`;
  - `Verifier::key(&self) -> [u8;32]`, `Verifier::derive_key(salt:&str, code:&str) -> [u8;32]`;
  - `ws::connect_with(endpoint, headers:&[(&str,&str)], connector, timeout) -> Result<(WebSocket<..>, Response)>`;
  - `ws::accept_with(stream, max_message, on_request: impl FnOnce(&Request,&mut Response)) -> Result<WebSocket<..>>`.

  Existing `ws::{connect, accept, call, send, receive, reply}` keep their behavior.
- **Tasks:**
  1. The §6.2 handshake, nonces, transcript, mutual proofs and constant-time compare.
  2. Pre-auth limits (16 KiB, 5 s, then raised via `set_config`).
  3. Legacy detection both ways.
  4. Self-pair, installation-change and verifier-generation handling.
  5. The probe role.
  6. Dialing tries every resolved address (3 s each).
- **Acceptance:**
  - right code → mutual success for the control, bulk and probe roles;
  - wrong code → `Unauthorized(INVALID_PAIRING_CODE)` with no verifier leakage;
  - a replayed proof on a new connection fails;
  - self-pair refused; a rotated generation refuses old codes;
  - a v2 dialer (`ws::connect` + `{op:pair,version:2}`) gets the legacy-format `PEER_NEEDS_UPGRADE` and `legacy_attempt` is called;
  - a v3 dialer against an emulated v2 server gets `NeedsUpgrade`, and the emulated server **never receives a code**;
  - a 17 KiB pre-auth hello is rejected; a silent client is closed at 5 s;
  - the existing `ws.rs` unit tests stay green.
- **Invariants at risk:** 20, 21, 22.
- **Estimate:** ~1,100.

#### WP1.11 StatusHub and pure status rules

- **Goal:** one cheap, versioned status owner (§3.4, §7).
- **Owned:** `src/orchestrator/hub.rs`, `src/orchestrator/hub_rules.rs`, `tests/status_hub.rs`.
- **Depends on:** WP0.INT.
- **Provides:**
  - `pub fn StatusHub::new(cfg: HubConfig{stall_after: Duration, interval: Duration, activity: usize}, clock: Arc<dyn Clock>) -> Arc<StatusHub>`, implementing `StatusSink` and `StatusSource`;
  - `pub fn StatusHub::tick(&self)` (the caller spawns the 1 Hz ticker);
  - `pub fn legacy_folder_sync(&self, folder:&FolderRecord, peers:&[PeerRecord]) -> Value`, `pub fn legacy_peers(&self, peers:&[PeerRecord]) -> Value` (post-cutover projection, §7.3);
  - `hub_rules::{folder_state, peer_state, stall, Ewma, dedupe}` as pure functions.
- **Tasks:**
  1. State maps keyed by folder, (folder, peer installation), (peer, lane) and inbound session.
  2. Event application. Rates via EWMA (10 s, 60 s); ETA = bytes remaining / 10 s rate.
  3. Stall detection; error dedupe and display policy; activity ring.
  4. Lazy snapshot rebuild (≤ 4/s); `wait_newer`.
  5. Legacy projection built from `ExchangeFinished` summaries and lane events, with every `RepositoryStatus` key.
  6. Redaction: no pairing code ever reaches the hub (`PeerRecord.code` is not serialized).
- **Acceptance:**
  - table tests for every `FolderPeerState` and `FolderSyncState` and the precedence order;
  - a non-sharing offline peer is ignored; an inbound-only peer counts;
  - stall triggers and clears; rate and ETA within 5 %; errors clear and move to history;
  - `record()` p99 < 50 µs with 10 waiters;
  - legacy projection keys match `tests/fixtures/compat` goldens (shape);
  - `status_redacts_pairing_code`.
- **Invariants at risk:** 21, 30.
- **Estimate:** ~1,600.

#### WP1.12 Interim stabilizers on the legacy lanes and supervisor

- **Goal:** ship relief now on code that lives until WP2.1 or WP3.1 (deliberately small).
- **Owned:**
  - `src/orchestrator/lanes.rs`, `src/orchestrator/peering.rs`, `src/orchestrator/supervisor.rs`, `src/orchestrator/run.rs`, `src/orchestrator/status.rs`, `src/orchestrator/catalog_db.rs`, `src/orchestrator/api.rs` (the `Shutdown` arm only);
  - `tests/legacy_stabilizers.rs`, `tests/qual_wp1_12_legacy.rs`.
- **Depends on:** WP0.INT.
- **Tasks:**
  1. **`lanes.rs`:**
     - on `connection_failed`, record the failure on the in-flight repository before dropping the lane (fixes `:1350-1352`);
     - rotate the catalog start index every pass;
     - `ping` at least every 5 s while waiting between passes (`lane_wait`), so any `sync_interval_ms` ≤ 60,000 avoids the acceptor's 15 s idle drop;
     - reset per-repo backoff when the local worker address changes;
     - clear the bulk inventory cache on any bulk error;
     - reset connection backoff only after 30 s of healthy passes;
     - `connected=true` only after the first successful request.
  2. **`status.rs`:** `WORKER_BUSY` and `LOCAL_WORKER_UNAVAILABLE` are Contention: retry 50–200 ms, no `consecutive_failures` increment, no `caught_up` reset, a separate `busy_retries` counter.
  3. **`run.rs`:** a `peers()` error in the main loop is logged and the last list kept (fixes `:1458`). Poison-tolerant locks (`util::lock::lock`).
  4. **`supervisor.rs`:**
     - per-folder restart backoff 1 s → 60 s in `reconcile` (instead of every 2 s);
     - accept a Ready line with `mounted:false` plus `mount_error` when a mount was requested: record the folder error, do not restart, and set `mounted=false`;
     - `Shutdown` sets `stopping` first and continues past per-folder failures, reporting outcomes.
  5. **`peering.rs`:** `published()` uses a per-worker 3 s deadline (`RpcLane` with a short timeout), so one hung worker cannot exceed the acceptor's 15 s `list` budget.
- **Acceptance:**
  - a hung remote worker for repo A (`HungServer` via `runtime.json` redirect) is charged and backed off, while repo B on the same peer keeps syncing within 45 s;
  - unit: a `WORKER_BUSY` failure leaves `consecutive_failures` at 0 and `caught_up` unchanged;
  - a worker exiting immediately is restarted at 1, 2, 4… s, and `List` stays responsive;
  - a mount failure keeps the folder's sync running (headless) and reports the error;
  - release, ignored: `sync_interval_ms=20000` → 0 reconnects over 60 s;
  - existing `tests/orchestrator.rs` is green.
- **Invariants at risk:** 16, 26.
- **Estimate:** ~800.

#### WP1.INT — Wave 1 integration

- **Merge order:** 1.11, 1.1, 1.3, 1.2, 1.9, 1.10, 1.6, 1.7, 1.8, 1.4, 1.5, 1.12.
- **Tasks:**
  1. Wire `revalidate` in `server.rs` to `Store::revalidate`. This is the only cross-package line; it is pre-authorized here.
  2. `tests/w1_integration.rs`:
     - `RealLauncher` launches the real `daemon --supervised` → Ready → `FolderRegistry` with a temp `Catalog` → Running → adopt after a registry restart;
     - `session::dial` ↔ `session::accept` with real handshake and `Conn` (WP1.9 + WP1.10).
  3. Wave 1 qual floors; interoperability with a 6298d0b peer (pair, receive, bidirectional sync); legacy fixtures.
- **Estimate:** ~400.

---

### Wave 2 — actor orchestrator wired (legacy network bridged), background activation, feed, v3 peer stack standalone

The wire stays v2-compatible this wave: the bridged legacy lanes are the only network path. The v3 stack (WP2.6–2.8) and the feed (WP2.5) are built and tested standalone; WP3.1 switches to them.

#### WP2.1 Orchestrator wiring: actors, core management, legacy network bridge

- **Goal:**
  - Replace the global `Supervisor` mutex and the polling main loop with the Wave 1 actors.
  - Keep the legacy List, Inspect and Peers JSON and the legacy v2 lanes working through a bridge.
  - Add signal-safe Shutdown.
- **Owned:**
  - `src/orchestrator/{mod.rs, run.rs, ctx.rs, shutdown.rs, manage.rs, legacy_bridge.rs, lanes.rs, peering.rs, status.rs}`;
  - `tests/orchestrator_runtime.rs`, `tests/qual_wp2_1_lifecycle.rs`;
  - `tests/orchestrator.rs`, limited to two edits:
    - the comment in `large_pages_resume_after_daemon_crash…` ("adopt" instead of "stop/restart");
    - a rewrite of `unavailable_worker_does_not_fail_remote_discovery_and_reports_its_error`. Its `runtime.json` redirect no longer affects published entries, which now come from the controller's endpoint. The test now restarts the daemon with `TKFS_FAULT_POINT=worker.rpc.published-summary`, `TKFS_FAULT_ACTION=fail` and `TKFS_FAULT_SCOPE=<state>`, restarts that folder, and asserts that RemoteList excludes it while Inspect `publication_error` contains `PUBLISHED_SUMMARY_FAILED`. It then restarts without the fault and waits for both entries. The semantics are unchanged.
- **Depends on:** WP1.INT (WP1.1, 1.2, 1.3, 1.4, 1.5, 1.11, 1.12).
- **Provides:**
  - `orchestrator::run(config) -> Result<()>` (same signature);
  - `pub struct Daemon { pub ctx: Arc<Ctx>, pub control: TcpListener, pub network: Option<TcpListener> }` and `pub fn ctx::build(config: Config) -> Result<Daemon>`;
  - `pub fn manage::manage(ctx:&Arc<Ctx>, action: Action) -> Result<Value>`, covering the legacy actions with compatibility shapes (§7.3) and forwarding new variants to `manage_v2::handle`;
  - `pub struct LegacyBridge`, with `LegacyBridge::start(ctx: Arc<Ctx>) -> Arc<LegacyBridge>`, implementing `NetworkApi` (v2 `pair`, `remote_list`, `unpair`) and `LegacyNetworkView`, plus `LegacyBridge::serve_inbound(&self, stream: TcpStream)`;
  - `pub fn shutdown::install(ctx:&Arc<Ctx>)` and `pub fn shutdown::run(ctx:&Arc<Ctx>, deadline: Duration) -> Value`.
- **Tasks:**
  1. **`ctx::build`**, in this order:
     1. `Catalog::open` → `StatusHub::new` (+ 1 Hz tick thread) → `PublishedCatalog::new`;
     2. `FolderRegistry::start` (`RealLauncher`; limits derived from `Config`);
     3. `Ctx`;
     4. `LegacyBridge::start`, then `ctx.network.set(..)` and `ctx.legacy.set(..)`.
  2. **`run.rs`:**
     - blocking accept threads;
     - control connections are persistent (loop `receive → manage → reply`; up to 64);
     - network accept goes to `bridge.serve_inbound` (up to 128 concurrent);
     - `orchestrator.json` gets `{format:3, pid, version, installation, control, network, started_at_ms}` with a Drop guard;
     - the main thread waits on `ctx.shutdown`.

     The 10 ms loop, the 2 s reconcile and all `Supervisor` use are gone.
  3. **`manage.rs` legacy actions:**

     | Action | Behaviour |
     |---|---|
     | List, Inspect | §7.3 projection from `FolderView`, the catalog record, `ctx.legacy()?.folder_sync`, the published-entry error, plus a `status` member from the hub |
     | Folder | passthrough via `FolderDirectory::client`, 30 s deadline, same allowed op list as today |
     | Create | `validate_mount_path` → `Store::initialize` on the request thread → `CreateFolder` → `Start` |
     | ConfigureMount | `STOP_FOLDER_BEFORE_CONFIGURING` while Starting or Running (legacy semantics; asserted by `real_mount_*`) |
     | Start | `SetDesired(1)` → `FolderCmd::Start` |
     | Stop | `SetDesired(0)` → `Stop{graceful}`; wait ≤ 15 s unless `wait:false`; reply `{stopping, waiting_for}` when blocked |
     | Shutdown | `shutdown::run` |
     | ResetPairingCode | `SetVerifier` |
     | Pair, Unpair, RemoteList | through `ctx.network()` |
     | Receive | `remote_list` check → `CreateFolder` with `GateRecord{source_installation, source_endpoint}` |
     | Peers | `ctx.legacy()?.peers()` |
     | Create or Start beyond `maximum_running` | `WORKER_LIMIT` |

  4. **Legacy bridge**, a mechanical adaptation of `lanes.rs` and `peering.rs` to `Arc<Ctx>`:
     - `projects()` → catalog folders filtered by desired && `sync_enabled`;
     - `discovery()` → `FolderView.worker` plus the record;
     - verifier → snapshot (O(1));
     - `stopping` → `ctx.stopping`;
     - `links` and remote catalogs → the bridge's `LegacyLinks` (`Mutex<BTreeMap>`, map operations only);
     - `lane_wait` → `Cancel::sleep`;
     - `serve_peer` `list` → `PublishedCatalogApi::list()` mapped to `PublishedState`, running entries only;
     - paused folders → `FOLDER_SYNC_PAUSED`; not running → `LOCAL_WORKER_UNAVAILABLE`;
     - status: `StatusEvent::LegacyLink` passthrough, `ExchangeFinished` (v6 `metadata_progress` mapped to `ExchangeSummary`) and `LaneState`;
     - legacy gate rule: when `metadata_caught_up` && no error && within 15 s, send `FolderCmd::GateEvidence{installation, caught_up:true}`;
     - `LegacyLaneManager` spawns and cancels lane threads per `(peer, revision, lane)` on catalog generation changes;
     - `LegacyNetworkView::{folder_sync, peers}` reproduce today's JSON exactly (code moved from `Supervisor::inspect` and `Action::Peers`).
  5. **`shutdown.rs`:**
     - Windows: `SetConsoleCtrlHandler` FFI.
     - Unix: `libc::signal(SIGINT|SIGTERM)` sets a static flag, and a watcher thread cancels `ctx.shutdown`.
     - Sequence as in §3.6; the reply is flushed before exit.
  6. Stop using `supervisor.rs` and `catalog_db.rs`. The files stay compiled and unused; WP2.INT deletes them.
- **Acceptance:**
  - Full existing suite green (`tests/orchestrator.rs` changed only as listed under Owned).
  - `qual_wp2_1_lifecycle` un-ignored:
    - `List` p99 < 100 ms over 200 calls while folder A crash-loops (`TKFS_FAULT_POINT=worker.startup`, scoped) and folder B is StopBlocked (open handle);
    - A reaches `Failed{CRASH_LOOP}` (status member);
    - Shutdown with B busy returns within 30 s with outcome `left_running`, exit 0, `desired_running` unchanged; the next start **adopts** B (same pid);
    - kill -9 the daemon → restart adopts the running worker, and sync resumes;
    - a frozen worker (`watch` and `hello` stall) → Unresponsive → restart, other folder unaffected; `worker.watch_hang` alone keeps the folder Running, because a worker that answers the `hello` probe is never killed (§3.3, invariant 8) (CCR-WP2.1-6, ratified in WP2.INT);
    - a pre-created non-empty mount path → folder Running headless, `mount.state=attach_failed`, sync works;
    - `received_mount_waits_for_complete_metadata_across_daemon_restart` passes with **in-place attach** (worker pid unchanged);
    - SIGTERM (WSL) and the Shutdown action produce identical outcomes, and `orchestrator.json` is removed;
    - no `Tracked` orchestrator lock held > 50 ms (debug).
- **Invariants at risk:** 8, 23, 26, 29, 30.
- **Estimate:** ~2,200 (~600 of it mechanical lane adaptation).

#### WP2.2 Management API v2, ControlClient, CLI

- **Goal:** typed status and control actions; a persistent client; CLI status and watch (P4 in Wave 2).
- **Owned:** `src/orchestrator/manage_v2.rs`, `src/orchestrator/api.rs`, `src/main.rs`, `tests/management_api.rs`.
- **Depends on:** WP1.INT. Tested in-process with fake `Ctx` parts and a real `StatusHub`; daemon-level checks run in WP2.INT.
- **Provides:**
  - `manage_v2::handle(ctx, action)`. Each action returns `{state, status: FolderStatus}` unless noted:

    | Action | Implementation |
    |---|---|
    | Status | `ctx.status_source.snapshot()` |
    | Subscribe | `wait_newer(since, min(wait_ms, 25000))` |
    | Attach | `FolderCmd::Attach` (wait 60 s) |
    | Detach{force} | `FolderCmd::Detach{force}` |
    | PauseSync, ResumeSync | `CatalogCmd::SetIntents` + `FolderCmd::SetSync` |
    | OverrideGate | `CatalogCmd::OverrideGate` + `FolderCmd::OverrideGate` |
    | ForceStop | `SetDesired(0)` + `FolderCmd::ForceStop` |
    | Retry | `FolderCmd::Retry` |
    | Revalidate | worker `revalidate` → `{requeued}` |
    | Diagnostics | `diagnostics::collect` |

  - `pub struct ControlClient`, with `ControlClient::connect(config:&Config) -> Result<Self>` and `call(&mut self, action:&Action, deadline: Duration) -> Result<Value>`. It reconnects transparently only for read-only actions (List, Inspect, Peers, Status, Subscribe, RemoteList). A mutating action that hits a transport error returns `CONNECTION_LOST` and is never replayed.
  - `pub fn api::format_status(s:&StatusSnapshot) -> String`.
  - CLI: `tkfs manage status [--json]`, `watch [--json]`, `attach|detach [--force]|pause|resume|mount-now|force-stop|retry STATE`, `revalidate STATE [--id ID]...`, `diagnostics`.
- **Acceptance** (`tests/management_api.rs`):
  - every new action against fake `CatalogApi` and `FolderDirectory` with a real hub;
  - `subscribe` returns within 50 ms of a recorded event and on timeout returns the current snapshot;
  - ControlClient: a test server that drops the connection after receiving `Start` gives `CONNECTION_LOST` with no replay; `List` reconnects;
  - `format_status` golden output for a fixture snapshot.
- **Invariants at risk:** 16, 21 (no code in output), 30.
- **Estimate:** ~1,300.

#### WP2.3 Worker background activation, status cell, maintenance, notifier

- **Goal:** filesystem, sync, status and activation stop blocking each other (§4.2, §4.4, §4.5).
- **Owned:**
  - `src/worker/{mod.rs, activator.rs, status_cell.rs, maintenance.rs, heartbeat.rs}`;
  - `src/runtime/{engine.rs, server.rs, notify.rs, published.rs}`;
  - `src/core/status.rs`;
  - `tests/worker_concurrency.rs`, `tests/qual_wp2_3_worker.rs`;
  - `tests/availability.rs` (only `status_is_responsive_while_engine_is_busy`: it now asserts responsiveness plus present counts instead of `busy/stale`; justified because the status cell removed the busy fallback).
- **Depends on:** WP1.4, WP1.6, WP1.8.
- **Provides:**
  - `worker::activator::spawn(ctx: Arc<WorkerCtx>) -> JoinHandle<()>`;
  - `worker::status_cell::{refresh_cheap(&WorkerCtx), refresh_full(&WorkerCtx)}`;
  - `Engine::after_activation(&mut self, delta:&ActivatedDelta)`, where `ActivatedDelta { ids, entities: BTreeSet<String>, namespace_changed: bool, before: Option<Projection> }`;
  - the `status` RPC reply: legacy keys plus `inbox`, `activation`, `mount`, `store_health`, `heartbeat_seq`.
- **Tasks:**
  1. `worker::run` opens the store with `defer_activation=true` and starts the activator, maintenance and heartbeat after RPC.
  2. **Activator loop** (§4.4):
     - `SliceBudget{64, 25, inline_slow:false}`;
     - `SlowTicket` → `Store::prepare_slow` on a pooled `SyncStore` → `commit_prepared` under the lock (`Prepared::Inline` until WP2.4 merges);
     - `Stale` → `requeue(id, backoff)`;
     - yields to FS waiters;
     - after each slice: `after_activation` → refresh the cell → bump `generations.sync`;
     - sets `ctx.activator_running`.
  3. **`after_activation`:** refresh only clean, non-writer sessions whose entity's content revision changed (verified read handles survive unrelated receives, fixing `runtime.rs:526-549`). When attached and the namespace changed, notifications are computed from the `before` projection (captured only while mounted).
  4. **Status cell:**
     - `refresh_cheap` on every mutating path, synchronously;
     - `refresh_full` (`content_status` compatibility, `content_counters`, per-peer feed counters) at 1 Hz in maintenance;
     - the `status` RPC returns the cell; if `cell.generation < ctx.status_generation`, it first recomputes on a pooled `SyncStore` (read-your-writes, no engine lock).
  5. **Maintenance:** `retry_pending` (`try_lock`; per-entity backoff 1 s → 60 s); `scrub_step` ≤ 32 MiB/s when the activator is idle; `publish_observation`.
  6. **Heartbeat, full:** inbox, activation, content, journal, generations, engine gauges (wait p99, longest hold, stalled > 120 s), per-peer feed counters from `ctx.sync_obs`.
  7. **`notify.rs`:** a Windows notifier thread with a coalescing queue of 4,096 paths (overflow → root invalidation), running only while attached (detached: drop and set `needs_refresh`). Removed from the RPC path.
  8. **`server.rs` and `published.rs`:**
     - remove `WORKER_BUSY` from `status` and `published-summary`;
     - legacy `published-*` metadata path: when `ctx.activator_running`, stage only (budget 0), with `try_lock` retried ≤ 100 ms before `WORKER_BUSY`;
     - `start_rpc` (legacy wrapper, in-process tests) does **not** start the activator, so `runtime_lanes`' ≤ 32-per-round test keeps its semantics.
  9. **`core/status.rs`:** `Store::status` built from `inbox_counts` and `content_counters`. Compatibility keys are kept: `incoming_pending` = total rows; `incoming_quarantined` = rows with an `error` set.
- **Acceptance:**
  - FS-op simulator p99 < 50 ms (release; < 200 ms debug) while 2K remote events activate;
  - `status` RPC < 20 ms during activation, with full counts;
  - startup with 5K staged events → Ready < 3 s, and background drain completes (heartbeat counters move);
  - an unchanged entity's verified read handle is not re-hashed after an exchange (hash counter);
  - `published-exchange` never returns `WORKER_BUSY` with the activator running;
  - existing suites green; `qual_wp2_3_worker` un-ignored.
- **Invariants at risk:** 8, 24, 30.
- **Estimate:** ~2,100.

#### WP2.4 Core off-lock slow path and event cache

- **Goal:** a slow-path event never holds the engine lock or the SQLite writer while it derives (M5, I7).
- **Owned:** `src/core/commit.rs`, `src/core/derive.rs`, `src/core/inbox.rs`, `tests/offlock_commit.rs`, `tests/qual_wp2_4_offlock.rs`.
- **Depends on:** WP1.6, WP1.7.
- **Provides:**
  - `Prepared::Computed(Box<PreparedCommit>)`, where `PreparedCommit { event, base_seq, projection, derived: DeriveOutput, reviewed: Vec<Conflict> }`;
  - `derive::derive_conflicts(input:&DeriveInput) -> Result<DeriveOutput>` (pure);
  - `derive::write_projection(tx, branch, &DeriveOutput)`;
  - `EventCache` in `caches.commit`.
- **Tasks:**
  1. Split `persist_projection` into a pure `derive_conflicts` and a cheap `write_projection`. Stored rows needed for append mode and reviews are pre-read into `DeriveInput`.
  2. **`prepare_slow`:** branch events from the `EventCache` (keyed by `(branch, max seq, count)`; incremental for seq growth, full reload on deletes) on the read store, then `project` and derive.
  3. **`commit_prepared`:**
     1. savepoint;
     2. check the event is still staged and not committed, and the branch max seq equals `base_seq`; otherwise `Stale`;
     3. **re-run** `validate_commit_preconditions` (`EVENT_ID_REUSED`, `OBJECT_SIZE_CHANGED`, `BRANCH_DESCRIPTOR_CHANGED`, `UNREFERENCED_OBJECT_SIZE`, size limits);
     4. write, then forced snapshot seal, then release.
  4. `activate_slice{inline_slow:false}` returns a `SlowTicket`; `inline_slow:true` keeps the compatibility path.
- **Acceptance:**
  - over 300 release seeds, a prepared off-lock commit equals the inline commit (projection, conflict rows, `current_parts`, snapshot);
  - a forced `Stale` (local commit between prepare and commit) converges;
  - `size_change_between_prepare_and_commit_rejected`;
  - the engine lock is held < 50 ms for a slow event at 2K (release);
  - `EventCache` equals `events()` after inserts and deletes;
  - `replication_regressions`, `startup_snapshots` and `scalability` green.
- **Invariants at risk:** 4, 7, 11.
- **Estimate:** ~1,600.

#### WP2.5 Metadata feed (format 7), worker side

- **Goal:** the worker half of §6.3. O(delta) metadata with no engine lock, no re-sends, explicit rejections and needs, and per-id evidence acks.
- **Owned:** `src/core/sharing.rs`, `src/runtime/sync_rpc.rs`, `tests/feed_v7.rs`, `tests/qual_wp2_5_feed.rs`.
- **Depends on:** WP1.6, WP1.8, WP1.4.
- **Provides** (`impl Store`, all `&self` so they run on pooled secondary stores):

```rust
fn feed_epoch(&self)->Result<String>; fn feed_tip(&self)->Result<i64>; fn journal_id_at(&self, seq:i64)->Result<Option<String>>;
fn validate_cursor(&self, c:&Cursor)->Result<bool>; fn feed_state(&self)->Result<FeedState>;
fn feed_page(&self, after:Option<&Cursor>, for_peer:&str, max_entries:usize, max_bytes:usize)->Result<FeedPage>;
fn build_exchange_request(&self, route:&SyncRoute)->Result<MetaExchangeRequest>;                                   // feed-offer
fn handle_exchange_request(&self, route:&SyncRoute, req:&MetaExchangeRequest)->Result<(MetaExchangeReply, FeedApplyResult)>; // feed-exchange
fn apply_exchange_reply(&self, route:&SyncRoute, request_id:&str, reply:&MetaExchangeReply)->Result<FeedApplyResult>;        // feed-apply
fn peer_feed_counters(&self, peer:&str)->Result<PeerFeedCounters>;
fn events_by_ids(&self, ids:&[String], max_bytes:usize)->Result<(Vec<Event>, Vec<String>)>;
```

  `runtime::sync_rpc::dispatch` handles `feed-offer`, `feed-exchange` and `feed-apply`:
  - validates the route (repo, local replica, remote ≠ local; codes as in `paired_sync::binding`);
  - checks out a `SyncStore`;
  - pokes `ctx.activation`;
  - updates `ctx.sync_obs` and `ctx.status_generation`.
- **Tasks:**
  1. **`sharing::ensure_schema`** (`schema:sharing=1`): `journal_epoch` and its delete trigger; `feed_in`, `feed_out`, `feed_needs`, `feed_rejected`.
  2. Implement the §6.3 sender, receiver, regression, need and gate rules exactly.
  3. Staging through `stage_incoming` with sources.
  4. `peer_known` and outbox acks only from evidence with matching digests.
  5. Reset, re-queue and recompute of outbox acks (port of `core.rs:2173-2186`).
  6. Exact `to_send`/`to_receive` counts (indexed `COUNT` capped at 100,000).
- **Acceptance** (`tests/feed_v7.rs`: in-process stores exchanging through the three functions; activation by `activate_slice` loops):
  - two replicas converge on 5K events; a three-node relay (A↔B↔C, A and C never connected) converges;
  - `staged_events_not_resent` (activation paused: the second exchange has 0 payload entries);
  - idle exchange reads 0 payload rows and runs < 5 ms; echo after convergence < 100 B/event;
  - `staged_never_acks`; `rejected_never_acks` (an unauthorized event; 300 quarantined events); `third_peer_staged_not_acked`;
  - `forged_origin_rejected`, `private_never_served`, `event_id_reuse_rejected` → `feed_rejected`, the cursor advances, no ack;
  - `need_delivers_missing_parents` (orphaned-children field shape); `unknown_parent_reported`;
  - a 3 MiB event via `need`;
  - `peer_restore_requeues_withdrawn_events` (snapshot B's DB, advance, restore B, exchange: A clears `peer_known(B)`, outbox re-queued, events re-sent);
  - `evacuation_resets_once` (epoch change);
  - `cursor_mismatch_recovers`; `digest_mismatch_no_ack`;
  - gate view: not caught up while blocked or rejected, or while the remote's `pending_active > 0`.
  - Release qual: a 150K-event feed has no caps; an idle exchange at 50K events costs ≤ 10 ms CPU per side and ≤ 4 KiB.
- **Invariants at risk:** 6, 9, 10, 12, 14, 22.
- **Estimate:** ~2,200.

#### WP2.6 PeerSupervisor, lanes, catalog client, v3 NetworkApi

- **Goal:** per-peer supervision over v3 sessions (§3.7), built standalone.
- **Owned:** `src/orchestrator/peer.rs`, `tests/peer_supervisor.rs`.
- **Depends on:** WP1.9, WP1.10, WP1.11, WP1.1.
- **Provides:** `pub struct PeerManager`, with `PeerManager::start(ctx: Arc<Ctx>) -> Arc<PeerManager>`, implementing `NetworkApi`. It spawns `scheduler::run_control` and `objects::run_bulk_lane` (stubs until WP2.7 and WP3.3) per Ready lane, each with a child `Cancel`.
- **Tasks:**
  1. **Supervisor thread per catalog peer:** dial lanes (`session::dial`, `CodeAuth`, role control or bulk, `lane{index, count}`); the lane state machine and backoff; `Incompatible` and `Unauthorized` recheck timers; record `peer_meta` (seen, incompatible).
  2. **Catalog client:** `catalog.list{since}` every 2 s on the control `Conn` into `PeerContext.catalog`. The last good copy is kept across reconnects. Emits `PeerCatalog` events.
  3. **`NetworkApi`:**
     - `pair`: a probe session gives the remote installation (`CANNOT_PAIR_WITH_SELF`), then `CatalogCmd::UpsertPeer` with a new revision;
     - `remote_list`: a probe session plus `catalog.list` (`PEER_INSTALLATION_CHANGED` if the installation differs);
     - `unpair`: `RemovePeer`, cancel, join ≤ 5 s;
     - `reconfigure`: a catalog generation watcher;
     - `shutdown`: Goaway, cancel, join.
  4. Status events: `PeerConfigured`, `LaneState` (with RTT from `ConnHealth`), `PeerRemoved`.
- **Acceptance** (fake v3 server built from `session::accept` with a test handler):
  - all lanes reach Ready;
  - reconnect backoff within jitter bounds;
  - a fake v2 server gives `Incompatible{PEER_NEEDS_UPGRADE}` with no redial before the recheck interval;
  - a wrong code gives `Unauthorized`, with no tight retry loop;
  - unpair cancels and joins all lanes within 5 s;
  - the catalog survives a control reconnect;
  - pair to self → `CANNOT_PAIR_WITH_SELF`; installation change → `PEER_INSTALLATION_CHANGED`.
- **Invariants at risk:** 16, 20, 21, 29.
- **Estimate:** ~1,500.

#### WP2.7 MetaScheduler, meta exchange, interim bulk lane

- **Goal:** per-(peer, repo, op) slots with deadlines, so a slow repo never starves the others. Interim object transfer over v3.
- **Owned:** `src/orchestrator/scheduler.rs`, `src/orchestrator/meta_exchange.rs`, `src/orchestrator/objects.rs`, `tests/meta_scheduler.rs`.
- **Depends on:** WP1.9, WP1.10, WP1.2, WP1.3.
- **Provides:**
  - `scheduler::run_control` (real);
  - `pub fn meta_exchange::exchange(ctx:&Ctx, conn:&session::Conn, peer:&PeerContext, folder:&FolderView, remote:&PublishedEntry) -> Result<ExchangeSummary, CallError>`;
  - `objects::run_bulk_lane` (interim §6.5).
- **Tasks:**
  1. **Due queue:** triggers from `FolderDirectory::wait_newer` (heartbeat `generations.sync`), remote catalog tip changes, anti-entropy (`sync_interval_ms`), and backoff expiry. ≤ 4 in flight, ≤ 1 per repo, 30 s deadline.
  2. **Exchange:** worker `feed-offer`, then `conn.call("meta.exchange")`, then worker `feed-apply`. Map results to `ExchangeSummary`, emit `ExchangeStarted`/`Finished`, and send `GateEvidence` to the folder (the controller filters by gate source).
  3. **Error policy:** by class. Contention is not counted. Uncertain → a fresh request with a new id. `WaitingForWorker`, `WaitingForRemote` and `Paused` are not failures.
  4. **Interim bulk lane:** for repos shared with this peer (catalog ∩ local desired && sync), loop `published-bulk-offer` → `objects.exchange` → `published-bulk-apply`. Bulk protocol 1, `Lane{index, count}` from `LaneSpec`. Yield 10 ms after content moves, otherwise wait `sync_interval_ms`.
- **Acceptance** (test v3 server with a scripted handler, plus `FakeWorkerClient`):
  - a hung remote handler for repo A delays repo B's exchanges by < 2 s; A's timeout is charged only to A; the `Conn` stays Ready;
  - uncertain requests are never replayed (distinct request ids);
  - `WaitingForWorker` and Contention are not counted;
  - the 4-slot limit is honoured;
  - a heartbeat generation change triggers an exchange within 100 ms;
  - the anti-entropy interval is honoured with no reconnects over 90 s at `sync_interval_ms = 30000` (ignored, release);
  - interim bulk pages are routed.
- **Invariants at risk:** 16, 18, 19.
- **Estimate:** ~1,500.

#### WP2.8 Inbound server v3

- **Goal:** bounded, authenticated, observable serving side (§3.8), built standalone.
- **Owned:** `src/orchestrator/inbound.rs`, `src/orchestrator/inbound_objects.rs`, `tests/inbound_v3.rs`.
- **Depends on:** WP1.9, WP1.10, WP1.1.
- **Provides:**
  - `pub fn inbound::start(ctx: Arc<Ctx>, listener: TcpListener) -> Arc<InboundServer>` and `InboundServer::shutdown(&self, deadline: Duration)`;
  - `inbound_objects::handle` (interim: `objects.exchange` → worker `published-bulk-exchange` with route and lane checks).
- **Tasks:** §3.8 in full:
  - accept caps;
  - `AcceptPolicy` over the catalog snapshot;
  - handler pools;
  - routing and role checks;
  - `REMOTE_WORKER_STARTING` and `FOLDER_SYNC_PAUSED`;
  - rotation watcher;
  - legacy hello handling;
  - status events (`InboundSession`, `ExchangeFinished{direction: inbound}`, `InboundIncompatible`);
  - inbound `GateEvidence` from the `feed-exchange` `local` view.
- **Acceptance:**
  - the 129th session is refused < 100 ms;
  - 100 sockets × 1 MiB of pre-auth garbage → flat memory, and a legitimate dial succeeds within 1 s;
  - rotation → `GOAWAY{PAIRING_CODE_CHANGED}` to all older sessions within 1 s;
  - `catalog.list` < 100 ms with one hung worker;
  - role and route violations keep their codes;
  - a legacy hello → `PEER_NEEDS_UPGRADE` plus `InboundIncompatible`;
  - `route_binding` uses the authenticated installation.
- **Invariants at risk:** 15, 18, 21, 22.
- **Estimate:** ~1,400.

#### WP2.INT — Wave 2 integration

- **Merge order:** 2.4, 2.3, 2.5, 2.1, 2.2, 2.6, 2.7, 2.8.
- **Tasks:**
  1. Delete `src/orchestrator/supervisor.rs` and `catalog_db.rs` (and their mod lines).
  2. Daemon-level tests in `tests/w2_integration.rs`:
     - Retry recovers a Failed folder;
     - Attach after fixing a mount path (no worker restart);
     - pause and resume on legacy lanes;
     - `manage status` and `watch` (CLI) show folder lifecycle and mount;
     - Subscribe latency < 1 s.
  3. Wave 2 qual floors; interoperability with a 6298d0b peer (still v2 wire).
- **Estimate:** ~500.

---

### Wave 3 — cutover to v3, objects v3, GUI

#### WP3.1 Cutover to v3 networking and deletion of v2 networking

- **Goal:** v3 sessions, the feed and the interim bulk path become the only network path. Legacy peers fail closed with `PEER_NEEDS_UPGRADE` on both sides. The compatibility JSON comes from the hub.
- **Owned:**
  - `src/orchestrator/{run.rs, ctx.rs, mod.rs, compat.rs}`;
  - deleted: `src/orchestrator/{legacy_bridge.rs, lanes.rs, peering.rs, status.rs}`;
  - `tests/orchestrator.rs`, `tests/qual_wp3_1_transport.rs`.
- **Depends on:** WP2.INT.
- **Provides:** `pub struct HubLegacyView`, implementing `LegacyNetworkView` (wraps `StatusHub::legacy_folder_sync`/`legacy_peers` and catalog peers).
- **Tasks:**
  1. **`ctx`/`run`:**
     - `PeerManager::start` becomes `ctx.network`;
     - `HubLegacyView` becomes `ctx.legacy`;
     - `inbound::start` runs on the network listener;
     - `LegacyBridge` and `LegacyLaneManager` are removed;
     - Shutdown calls `network.shutdown` and `inbound.shutdown`.
  2. The new gate rule is live (evidence from `meta_exchange` and the inbound `local` view); the legacy rule is deleted with the bridge.
  3. **`tests/orchestrator.rs` migration** (15 tests; every changed assertion justified in the PR):

     | Test | Change |
     |---|---|
     | `one_repository_uses_all_bulk_lanes_and_keeps_control_free_of_objects` | `transport` expected `"tkfs-peer/3"`. Other assertions kept, served by the hub projection. |
     | `pairing_code_only_rotation_invalidates_persistent_connections_and_rejects_legacy_transports` | Rotation is checked with a raw v3 probe session (`session::dial` + `catalog.list`) instead of a raw v2 pair. The error is `PAIRING_CODE_CHANGED` (GOAWAY). |
     | `pairing_refuses_a_peer_that_does_not_reserve_lanes` | **Replaced** by `legacy_peer_fails_closed_with_needs_upgrade`: (a) a v3 node pairing to a fake v2 server gets `PEER_NEEDS_UPGRADE`, and the server never receives a code; (b) a raw v2 client to a v3 node gets `PEER_NEEDS_UPGRADE`; (c) both daemons' Status show it. |
     | `reserved_connection_delivers_events_while_bulk_is_blocked` | **Rewritten** with a v3 fake server (`session::accept`): the bulk handler stalls 20 s (longer than the old 15 s timeout), while control `meta.exchange` is served by an in-process feed store. The event arrives within 8 s and no lane resets. |
     | `received_mount_waits_for_complete_metadata_across_daemon_restart` | Unchanged semantics (new gate rule includes the source's `pending_active`). The worker pid stays the same across attach. |
     | others | unchanged, or only fields renamed per §7.3 |

  4. `tests/qual_wp3_1_transport.rs`:
     - a hung remote worker for repo A (`worker.rpc.feed-exchange` hang, scoped) never stalls repo B by > 2 s;
     - `sync_interval_ms=20000` → 0 reconnects over 60 s;
     - a v2 peer fails closed on both daemons;
     - the acceptor's Status shows the inbound peer with lanes, rates and per-folder state;
     - rotation → sessions closed → the dialer is Unauthorized until re-paired.
- **Acceptance:**
  - the full suite is green on the interim bulk path (WP3.3 replaces it at WP3.INT);
  - `grep` finds no `published-summary` fan-out, `RpcLane` use, `ws::call` peer use, or `X-Tkfs-Wire` dependency in `src/orchestrator/`.
- **Invariants at risk:** 15, 18, 20, 21, 26.
- **Estimate:** ~2,000.

#### WP3.2 Objects v3, worker side

- **Goal:** the worker half of §6.4.
- **Owned:**
  - `src/runtime/objects_rpc.rs`, `src/runtime/server.rs`;
  - `src/core/transfer.rs`, `src/core/objects.rs`, `src/core/demand.rs`;
  - `tests/objects_v3_worker.rs`, `tests/qual_wp3_2_objects.rs`.
- **Depends on:** WP2.INT.
- **Provides** (`impl Store`):

```rust
fn want_objects(&self, lane: LaneSpec, max_items: usize, max_bytes: u64, leased_elsewhere: &dyn Fn(&str)->bool) -> Result<Vec<WantItem>>;
fn read_object_range(&self, hash:&str, offset:u64, len:usize) -> Result<(u64, Vec<u8>), ErrorWire>; // authz triad; verify-on-first-serve
fn write_object_items(&self, items:&[WriteItem], body:&[u8], locks:&ObjectLocks) -> Result<Vec<WriteResult>>; // spool_state durability
fn report_corrupt(&self, hash:&str) -> Result<ReportAction>;
pub struct WriteItem { pub hash: String, pub offset: u64, pub total: u64, pub len: u64, pub last: bool }
pub struct WriteResult { pub hash: String, pub outcome: ObjectOutcome, pub durable_offset: u64 }
```

  `objects_rpc::dispatch` serves `objects.want`, `objects.read`, `objects.write` and `objects.report`, with leases in `ctx.inflight_objects`. `server.rs` accepts loopback sessions (`upgrade_kind == Loopback` → `session::accept_loopback` → `Conn::serve`). JSON RPC is unchanged.
- **Tasks:** §6.4 in full: `spool_state`; durable offsets; overlap verification; per-write fsync then commit; full re-read verification; install; batched registration; leases; authorization; verify-on-first-serve (heals a file that reappears); sender quarantine on report; legacy spool adoption; startup spool reconciliation (truncate to `durable`).
- **Acceptance:**
  - want ordering (demand > current > history, smaller first);
  - objects of staged-only events are wanted (activation paused);
  - `hash_only_request_not_permitted`; `unsolicited_write_rejected`; `duplicate_sessions_transfer_once` (`IN_FLIGHT_ELSEWHERE`);
  - `crash_mid_object_resumes_from_durable` (fault between write and commit);
  - `corrupt_spool_never_installed`; `crash_between_install_and_register_retransfers`;
  - report → sender quarantines its copy; a good file restored by the user heals on read;
  - 10K tiny objects with one data fsync per object and one registration transaction per batch;
  - legacy spool adoption.
  - Release: a 256 MiB object worker-to-worker over loopback sessions (test pump) ≥ 100 MiB/s.
- **Invariants at risk:** 1, 2, 3, 13, 19.
- **Estimate:** ~2,000.

#### WP3.3 Objects v3, orchestrator side

- **Goal:** the orchestrator half of §6.4: download and upload pipelines on bulk lanes, and inbound streaming.
- **Owned:** `src/orchestrator/objects.rs`, `src/orchestrator/inbound_objects.rs`, `src/orchestrator/worker_client.rs`, `tests/objects_v3_lanes.rs`.
- **Depends on:** WP2.INT. Tested against a fake worker session implementing the frozen W0 op contract; daemon e2e runs in WP3.INT.
- **Provides:**
  - `objects::run_bulk_lane` (v3);
  - `inbound_objects::handle` (`objects.pull`/`wants`/`push`/`report`);
  - `pub struct SessionWorkerClient`, with `SessionWorkerClient::connect(ep:&WorkerEndpoint) -> Result<Arc<Self>, CallError>`, implementing `WorkerClient`, plus `read_stream(..)` and `write_batch(..)`;
  - `pub fn worker_client::session_for(ep:&WorkerEndpoint) -> Result<Arc<SessionWorkerClient>, CallError>` (a per-instance cache).
- **Tasks:**
  1. **Download:** want → pull (16 MiB budget, ≤ 2 pulls per lane in flight, re-pull at half) → pipe chunks into ≤ 4 in-flight writes → per-object results.
  2. **Upload:** wants → read → push stream → results.
  3. Both directions interleave; ≤ 2 repos per lane; `Lane::owns`.
  4. `ExchangeFinished` for each pull or push, with bytes and objects (rates); `ObjectError` per failed object.
  5. Inbound: pull → reads (≤ 4 in flight, 1 MiB each) → CHUNK stream; push → writes; per-object results.
- **Acceptance:**
  - both directions complete with per-object results; one `CorruptAtSource` object does not stop the others;
  - `control_carries_no_object_bytes`;
  - a stalled push does not block pulls on the same lane;
  - ≤ 16 MiB in flight per lane (measured);
  - after a connection loss the resume starts from the worker's durable offset (no blind replay);
  - release, ignored: through FaultProxy (50 ms RTT, 100 Mbit) ≥ 80 % of link rate.
- **Invariants at risk:** 16, 18, 19.
- **Estimate:** ~1,900.

#### WP3.4 Desktop GUI and view model

- **Goal:** the UI of §7.4, driven by Subscribe, with commands never blocked.
- **Owned:** `src/ui_model.rs`, `src/gui.rs`, `ui/app.slint`, `tests/gui_model.rs`.
- **Depends on:** WP2.INT.
- **Provides:** `tkfs::ui_model::{folder_rows(&StatusSnapshot), peer_rows(&StatusSnapshot), folder_detail(&StatusSnapshot, state), FolderRowModel, PeerRowModel, FolderDetailModel, chip_text(..), format_eta(..), format_bytes(..)}`, all pure and cross-platform.
- **Tasks:**
  1. A subscription thread: `ControlClient` `subscribe(since, 10 s)` with a 15 s deadline, reconnect with backoff, and daemon respawn with output to `data/logs/orchestrator.log` if the control endpoint is gone.
  2. A command thread with its own `ControlClient`; busy state per action and per row.
  3. The Slint UI per §7.4.
  4. Remove the `sync_label` heuristics (`gui.rs:181-237`); move the 4 existing `gui.rs` unit tests into `tests/gui_model.rs` against the new model.
- **Acceptance:**
  - `ui_model` golden tests for every `FolderSyncState`, `FolderPeerState`, lifecycle phase, mount state and gate kind;
  - a non-sharing peer is ignored; a needs-upgrade peer and a stalled folder render as specified; errors disappear when absent from a snapshot;
  - command latency < 200 ms while a slow subscribe is in flight (fake client);
  - Slint compiles (Windows); `tests/gui_model.rs` runs in WSL;
  - the manual checklist is delivered to WP4.3.
- **Invariants at risk:** 21 (never render a code beyond the one-time Reset result).
- **Estimate:** ~1,800.

#### WP3.INT — Wave 3 integration and end-to-end

- **Merge order:** 3.2, 3.3, 3.1, 3.4. WP3.3 replaces the interim `objects.rs` and `inbound_objects.rs`; re-run `tests/orchestrator.rs` on objects v3.
- **Tasks:**
  1. Fix compatibility-projection mappings for v3 object events via CCRs to `hub.rs`/`hub_rules.rs`.
  2. Own `tests/qual_wp3_int_e2e.rs` (two or three daemons):
     - a 256 MiB object ≥ 40 MiB/s on loopback;
     - 1 GiB through FaultProxy (50 ms RTT, 100 Mbit) ≥ 80 % of link rate;
     - 10K × 4 KiB files ≥ 300 files/s;
     - objects flow before activation (activator slowed by a fault);
     - a relay A↔B↔C with objects;
     - mutual pairing transfers ≤ 1.1× the object bytes;
     - a 10K-event catch-up ≤ 60 s;
     - a two-way backlog (2K creates with one unseen local edit) ≤ 3 min end to end;
     - a v2 peer fails closed;
     - legacy fixture upgrade, then sync.
  3. Evidence: `test-evidence/qualification-wave-3.json`.
- **Estimate:** ~700.

---

### Wave 4 — hardening, operations, docs

#### WP4.1 Security hardening: per-peer credentials, revocation, local tokens

- **Owned:**
  - `src/session/auth.rs`, `src/session/handshake.rs`;
  - `src/orchestrator/{inbound.rs, catalog.rs, peer.rs, api.rs, worker_client.rs}`;
  - `src/runtime/server.rs`, `src/runtime/mod.rs`;
  - `src/worker/mod.rs`, `src/worker/record.rs`;
  - `src/ws.rs`;
  - `tests/runtime_lanes.rs` (raw `ws::connect(&info.address)` → `runtime::connect_worker(&info)`; justified by mandatory tokens), `tests/security.rs`.
- **Depends on:** WP3.INT.
- **Tasks:**
  1. **Per-peer secret:** after the first code-authenticated pairing, the dialer generates 32 random bytes `S` and sends `peer.enroll{secret}` over the authenticated probe session.
     - Both sides store `K_peer = SHA-256("tkfs-peer-secret-v1" ‖ S)` in `peer_credentials`.
     - Later sessions use scheme `peer-hmac-v1` (`ClientHello.features`) with `K_peer`.
     - A missing or revoked credential → `GOAWAY{PEER_REVOKED}`.
  2. **Unpair:** the dialer sends a best-effort `peer.revoke` before deleting. The acceptor revokes and closes that installation's sessions within 1 s.
  3. **ResetPairingCode:** keeps today's semantics. It revokes all code sessions **and** all per-peer credentials (invariant 21).
  4. **Local tokens:**
     - `orchestrator.token` (data dir) is required as `X-Tkfs-Token` on the control upgrade; `client()` and `ControlClient` read it.
     - A per-instance worker token goes in `worker.json`, `STATE/runtime.token`, the `ReadyLine` and `WorkerEndpoint.token` (fields frozen in W0 as `Option<String>`, None until now). It is required on worker JSON RPC and loopback session upgrades.
     - `runtime::rpc` reads `runtime.token`.
     - Files are owner-only (Unix 0600; Windows inherits the profile ACL, documented).
- **Acceptance:**
  - unauthenticated local connections are rejected at the upgrade;
  - CLI, GUI and the test harness work;
  - unpair revokes inbound access within 1 s;
  - a replayed handshake fails;
  - rotation invalidates live sessions and per-peer credentials;
  - a grep test finds no pairing code, secret or token in status JSON or log files.
- **Invariants at risk:** 21, 25.
- **Estimate:** ~1,600.

#### WP4.2 Operations: logs, diagnostics, qualification floors

- **Owned:**
  - `src/util/log.rs`, `src/orchestrator/diagnostics.rs`, `src/orchestrator/process.rs` (`worker.log` rotation at spawn);
  - `tests/logging.rs`, `tests/qual_scale.rs`, `tests/qualification-floors.json`;
  - `scripts/qualify.ps1`, `scripts/qualify.sh`;
  - `test-evidence/qualification-*.json`.
- **Depends on:** WP3.INT.
- **Tasks:**
  1. **Logs:** JSON-line files with rotation (`data/logs/orchestrator.log`, 10 MiB × 5; `worker.log` rotated at spawn when > 10 MiB); per-code rate limits.
  2. **`diagnostics::collect`:** writes `data/diagnostics/<ts>/{status.json, activity.json, folders/*.heartbeat.json, catalog.redacted.json, logs/*.tail, versions.json}` and returns the path.
  3. **`qual_scale.rs` floors** (release, ignored):
     - remote linear activation at 2K and 6.4K events;
     - two-way backlog;
     - 150K-event feed;
     - 10K tiny files;
     - 1 GiB object;
     - startup to Ready at 50K objects < 10 s;
     - management API p99 < 100 ms under lifecycle churn;
     - idle worker CPU ≤ 1 % at 5K events / 10K objects.
  4. `qualify.ps1 -All` enforces every wave's floors and records evidence with hardware info.
- **Acceptance:**
  - log rotation test;
  - a diagnostics bundle with no secrets (grep);
  - `qualify.ps1 -All` green on the dev machine; `qualify.sh` (Linux subset) green in WSL.
- **Estimate:** ~1,300.

#### WP4.3 Documentation

- **Owned:** `README.md`, `docs/ARCHITECTURE.md`, `docs/CONFIGURATION.md`, `docs/CLI.md`, `docs/VALIDATION.md`, `docs/README.md`, `docs/LINUX.md`, `tests/docs_cli.rs`.
- **Depends on:** WP3.INT. Written against this plan; the integrator reconciles with WP4.1 and WP4.2.
- **Tasks:**

  | Doc | Content |
  |---|---|
  | ARCHITECTURE | failure domains and actors; worker lifecycle, mount and gate; inbox and activation (fast paths, append mode, off-lock); protocol v3 (sessions, auth, feed, objects); status v2 and errors; invariants and their guards. The lifecycle study becomes the implemented design. |
  | CONFIGURATION | `sync_interval_ms` = anti-entropy interval (50–60,000; keepalive makes any value safe); `connections` = lanes; env vars `TKFS_REMOTE_FASTPATH`, `TKFS_CONFLICT_MODE`, `TKFS_STALL_AFTER_MS`, `TKFS_UNSUPERVISED_EXIT_S`, `TKFS_FAULT_*`; logs and tokens |
  | CLI | all `manage` commands, including `status` and `watch` |
  | VALIDATION | qualification procedure and floors, GUI manual checklist (from WP3.4), soak procedure |
  | README | upgrade notes (v3 fails closed: upgrade both ends; peers show "Peer needs upgrade"); status meanings; runbooks for peer needs upgrade, quarantined events (revalidate), mount failed, crash loop (retry), gate blocked (mount now), unauthorized after rotation (re-pair), adoption after a daemon crash |

- **Acceptance:**
  - `tests/docs_cli.rs` parses `docs/CLI.md` command blocks and checks `tkfs <cmd> --help` succeeds;
  - review checklist: no doc describes offer/exchange/apply pages, list polling or 15 s timeouts as current behaviour.
- **Estimate:** ~1,200.

#### WP4.INT — final integration and production qualification

- **Merge order:** 4.2, 4.1, 4.3.
- **Tasks:**
  1. Full suite on Windows; WSL test subset.
  2. Release oracle with 1,000 seeds (`derive_oracle`, `feed_v7` fuzz).
  3. `qualify.ps1 -All` (every floor of waves 1–4).
  4. **2-hour soak:** two daemons through FaultProxy with periodic stalls, resets and latency changes, continuous edits on both sides. At the end: equal feeds, empty oracle on both, and no stalled or blocked state.
  5. Upgrade test from the 6298d0b legacy fixtures.
  6. `test-evidence/qualification-final.json` with the before/after table of §15.
  7. Docs reconciled.
- **Estimate:** ~600.

---

### 14.6 Qualification scenario catalog (created by WP0.5; owner un-ignores)

| File | Scenarios (floors in §15) |
|---|---|
| `qual_wp1_4_worker` | mount failure keeps the worker serving; accept loop survives faults; poison exit persists dirty data; RPC before mount; `watch` latency |
| `qual_wp1_5_mount` | attach/detach/attach on the same pid; force detach persists dirty data; foreign mount untouched |
| `qual_wp1_6_activation` | remote linear create at 2K and 10K; quarantine not pending; transient retry; local create after resolved conflict |
| `qual_wp1_7_derive` | two-way backlog at 2K and 6.4K; 50 concurrent namespace ops on 5K |
| `qual_wp1_8_objects` | corrupt mid-page keeps completed objects; restart with no re-hash at 20K; 12K tiny pending objects; `content_status` at 50K |
| `qual_wp1_9_session` | slow does not block fast; idle 60 s; dead-peer detection; 32 MiB duplex; loopback throughput |
| `qual_wp1_12_legacy` | hung remote repo charged; interval 20 s with no reconnects; WORKER_BUSY not counted; crash backoff |
| `qual_wp2_1_lifecycle` | API latency under churn; crash loop; Shutdown with a busy folder; adoption; hung worker; mount failure end to end; SIGTERM |
| `qual_wp2_3_worker` | FS p99 during catch-up; status latency; startup with 5K staged events |
| `qual_wp2_4_offlock` | lock hold < 50 ms for a slow event at 2K; Stale convergence |
| `qual_wp2_5_feed` | 150K-event feed; idle exchange at 50K; staged events not re-sent; peer restore |
| `qual_wp3_1_transport` | hung remote repo; interval 20 s; v2 peer fails closed; inbound status; rotation |
| `qual_wp3_2_objects` | 256 MiB worker-to-worker; 10K tiny objects; crash resume |
| `qual_wp3_int_e2e` | §14 WP3.INT list |
| `qual_scale` | WP4.2 list |

---

## 15. Expected impact, floors, risks

### 15.1 Floors (release, Windows dev box; enforced by `scripts/qualify.ps1`; hardware recorded with evidence)

| Metric | Baseline (6298d0b) | Floor | Enforced from |
|---|---|---|---|
| Remote linear create, per event | 6.2 s @1K, 15.9 s @2K, 218 s @6.4K | ≤ 5 ms @10K | W1 |
| Two-way backlog (one unseen local edit), per event | same as above (slow path) | ≤ 100 ms @2K, ≤ 300 ms @6.4K | W1 |
| 50 concurrent namespace ops on 5K history | O(N²) seconds to minutes | ≤ 500 ms each | W1 |
| Local create after a resolved conflict | 4.2 s | ≤ 10 ms | W1 |
| Restart to Ready at 20K objects | full re-hash | ≤ 3 s | W1 |
| `content_status` at 50K objects | ≈ 6.5 s (0.13 ms/object) | < 50 ms | W1 |
| Hung remote repo vs a healthy repo on the same peer | lane-wide resets, 30 s backoff | healthy repo progresses within 45 s (W1, legacy); < 2 s extra delay (W3, v3) | W1 / W3 |
| `sync_interval_ms = 20000` reconnects over 60 s | every idle cycle | 0 | W1 |
| Management API p99 during lifecycle churn | locks held 23–41 s | < 100 ms | W2 |
| FS-op p99 during 2K-event catch-up | engine held for seconds | < 50 ms | W2 |
| Status RPC latency during activation | blanked (`busy`/`stale`) | < 20 ms with full counts | W2 |
| Engine-lock hold for a slow event at 2K | full O(N²) derivation | < 50 ms | W2 |
| Idle metadata exchange at 50K events | 1.13 s and 251 KB @6.4K | ≤ 10 ms CPU per side, ≤ 4 KiB | W2 (in-process) / W3 (daemon) |
| 150K-event inventory | `INVENTORY_TOO_LARGE` | syncs | W2 / W3 |
| Event ≥ 349,525 B | permanent stall | up to 8 MiB syncs | W2 / W3 |
| Single large object on loopback (daemons) | 2.0 MiB/s | ≥ 40 MiB/s | W3 |
| 100 Mbit / 50 ms RTT link | — | ≥ 80 % of link | W3 |
| 10K × 4 KiB files | page cliff above 10,031 parts | ≥ 300 files/s | W3 |
| 10K-event catch-up between daemons | hours | ≤ 60 s | W3 |
| Objects before activation | 0-byte bulk rounds | objects flow while the activator is paused | W3 |
| Mutual pairing duplicate bytes | up to 2× | ≤ 1.1× | W3 |
| Startup to Ready at 50K objects | minutes | < 10 s | W4 |
| Idle worker CPU (5K events / 10K objects) | 4 sweeps + 2 parses per 500 ms per lane × repo | ≤ 1 % | W4 |
| Subscribe/watch latency | 5 s poll | ≤ 1 s | W2 |

### 15.2 Impact by pain point

- **P1 (slow or stuck object sync):**
  - **Activation:** fast paths for remote linear history and append mode for two-way and concurrent history in **Wave 1**. Background activation, off-lock slow path and no `WORKER_BUSY` in **Wave 2**.
  - **Metadata:** the cursor feed removes re-sends, caps and the event-size cliff (Wave 3 on the wire).
  - **Objects:** objects v3 removes the activation gate, stop-and-wait, hex and page atomicity (Wave 3).
  - **Stuck mechanisms:** the 15 s timeout as liveness, HOL livelock, quarantine cascades and poison objects are each contained to their own domain.
- **P2 (processes too bound together):**
  - every coupling edge in synthesis §3 P2 becomes an explicit seam (`Ctx` traits, generation counters, heartbeats, separate mount, inbox and status states);
  - no lock is held across I/O;
  - the worker engine is no longer shared by sync, status and activation in long holds.
- **P3 (failures brick unrelated things):** every row of the synthesis P3 table now has a contained outcome and a qual test:
  - mount failure → headless sync plus Retry;
  - hung worker → detected and restarted;
  - crash loop → breaker;
  - busy Shutdown → per-folder outcomes and adoption;
  - bad event or object → scoped;
  - SQLite or poison errors → no daemon death;
  - unpair → gate retargets;
  - flood → bounded;
  - legacy peer → explicit `PEER_NEEDS_UPGRADE`.
- **P4 (UI shows too little):**
  - **Wave 2:** typed StatusHub plus CLI `status`/`watch`.
  - **Wave 3:** GUI with per-(folder, peer) states, rates, ETA, stall culprit, typed errors with actions, mount and lifecycle truth, inbound peers and an activity log.
  - **Wave 4:** diagnostics bundle and rotating JSON logs.

### 15.3 Risk register

| Risk | Likelihood / impact | Mitigation |
|---|---|---|
| Append mode diverges from full mode (invariant 11) | M / H | Exactness precondition with full fallback; `TKFS_CONFLICT_MODE=verify` in fuzz and qualification; refusal-parity and rebuild checks; `full` kill switch |
| Remote fast path changes conflict rows | L / H | Oracle after every commit in fuzz; per-branch-any-row fallback gate; `TKFS_REMOTE_FASTPATH=0` |
| Non-blocking native-tls (SChannel) edge cases | M / M | Drain reads until `WouldBlock`; WSS tests with `tests/fixtures/tls`; pre-auth timeouts only during the handshake |
| Feed regression handling gaps (restored DBs) | M / H | `(epoch, seq, id)` validation, the epoch trigger, the `peer_restore_requeues_withdrawn_events` test, cursor-mismatch handling |
| Compatibility projection drift breaks `tests/orchestrator.rs` | M / M | Baseline goldens (WP0.5); legacy JSON served verbatim until cutover; WP3.1 owns the test migration |
| Intra-wave seam mismatch | M / M | All seams stubbed by WP0.INT with exact signatures; CCR process; integrator authority |
| Timing-flaky daemon tests in parallel worktrees | H / L | `suite-lock.ps1`; per-worktree target dirs; generous debug thresholds; strict thresholds only in release qual runs |
| Adoption kills or double-owns a worker | L / H | pid, `process_start` and image path verification; `owner.lock` probe; never spawn while the lock is held |
| WinFsp drain re-entrancy on force detach | M / M | Fence before drain; drain counter in `tk_call`; force path tested with an open handle |
| Package over budget (WP2.1, WP2.5) | M / L | Declared fallbacks: WP2.1 may defer the `pause-sync` filter to WP2.INT; WP2.5 may defer exact `to_send`/`to_receive` counts to estimates |
