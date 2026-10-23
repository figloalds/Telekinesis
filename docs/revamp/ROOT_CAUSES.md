# Telekinesis (tkfs): root-cause map for slow/stuck sync, coupling, failure spread and missing UI status

Historical analysis: the security proposals here are not current requirements.
The 2026-10-22 [personal-use authority scope](AUTHORITY_SCOPE.md) supersedes
broader hostile-user defenses, custom authentication and token/ACL prescriptions.
DVFS authority, integrity, resource bounds and filesystem correctness still apply.

This map covers the current working tree: 18 modified files, plus the untracked `src/bulk_sync.rs` and `src/wire.rs`. I modified no files. Line numbers refer to the working tree.

---

## 1. Spot-check of the most important claims

I re-read the code for 13 claims. All 13 hold. I dropped none. A few smaller details are corrected after the table.

| # | Claim | Verdict | What I read |
|---|---|---|---|
| 1 | Remote events never use the incremental create/rename/delete fast paths, and any conflict row (even a resolved one) also turns those fast paths off for local events | **CONFIRMED** | `core.rs:1061-1067` (`if fast.is_none() && local`). `core.rs:1306` and `:1412` (`event.device != self.device` → None). `core.rs:1349` and `:1450` (`!self.conflicts()?.is_empty()`). `conflicts()` returns rows with `resolved_by` set (`core.rs:1932-1946`). |
| 2 | The slow path holds the SQLite writer while it rebuilds all namespace history and writes a full snapshot | **CONFIRMED** | Journal/outbox INSERTs (`core.rs:1114-1124`) run before `persist_projection` (`core.rs:1143`), which calls `namespace_history` (`core.rs:2976`, body `3304-3433`, `project()` per cut near `:3422`). `snapshot::maybe_persist(.., !incremental)` (`core.rs:1159`) leads to a forced full persist (`snapshot.rs:100-101`). The whole drain runs inside one SAVEPOINT (`core.rs:2329-2348`). |
| 3 | After one expensive activation, the 32-event / 50 ms budget stops the drain | **CONFIRMED** | `started` is set at `core.rs:2352`, before every incoming row and the whole journal are loaded (`:2354-2368`). The cutoff `attempts>0 && elapsed>=50ms` is at `core.rs:2380-2387`. Callers: `paired_sync.rs:296` (`Some(32)`) and `runtime.rs:1012`. |
| 4 | Objects are offered only for events the peer has already activated | **CONFIRMED** | `bulk_sync.rs:155-163` (`known_by_peer`). The receiver accepts only objects of committed events (`core.rs:2764-2776`). |
| 5 | A transport error drops the whole lane, is never charged to the repository that caused it, and repositories are always visited in the same order | **CONFIRMED** | `orchestrator.rs:1350-1352` returns before the per-repository status update at `:1353-1362`. Errors are classified as transport failures at `:1143-1146` and `:1314-1319`. Backoff resets only after a complete pass (`:1373`). Catalog order is `ORDER BY label,state` (`:368`). Every socket has a fixed 15 s read/write timeout (`ws.rs:145-152`). |
| 6 | The one supervisor lock is held across worker RPCs, multi-second waits and process spawns | **CONFIRMED** | `manage` takes the lock at `:844` and holds it through Start, Stop and Shutdown (`:857-874`). `stop()` sends an RPC and then polls every 25 ms for up to 5 s (`:621-634`). `start()` sends an orphan status RPC, then a stop RPC, then waits up to 5 s (`:500-518`). `reconcile` runs on the accept thread under the lock (`:1453`). The main loop runs `peers()?` every 10 ms (`:1458`). `lane_active` takes the lock and runs SQL every ≤50 ms (`:1105-1122`). |
| 7 | The mount starts before the worker's RPC, and failed workers restart every 2 s with no backoff | **CONFIRMED** | `main.rs:288-298`. `mount.rs:72` (`ENGINE.set`) runs before the `MOUNT_PATH_MUST_NOT_EXIST` check at `:73-77`. `reconcile` removes the failed worker and restarts it in the same pass (`orchestrator.rs:689-695`, `:732-737`), on a 2 s cadence (`:1452-1456`). Every start re-hashes every stored object (`core.rs:434-451`). |
| 8 | One bad part fails the whole bulk page; objects already installed in that page stay unregistered; and receive runs before offer, so the other direction is lost too | **CONFIRMED** | Objects are installed inside the per-part loop (`core.rs:2855`) and the loop bails on a later part (e.g. `:2844-2847`). Registration happens only if the whole page succeeded (`bulk_sync.rs:210-227`). Receive comes before offer at `bulk_sync.rs:268-270` (bulk) and `paired_sync.rs:361-362` (control). |
| 9 | Hard inventory caps and an event-size cliff | **CONFIRMED** | Caps at `paired_sync.rs:217-224` and `bulk_sync.rs:105-111`. Each part is charged 256 bytes of overhead (`core.rs:2702`), so a lane with ≥10,031 pending objects of ≤81 B always produces a page over the 10,000-part cap. The metadata budget is `maximum/4` (`paired_sync.rs:98-102`) and each event must be `< budget/3` (`core.rs:2494`), i.e. 349,525 B, while `MAX_EVENT` allows 8 MiB. |
| 10 | The full `content_status()` scan runs on every metadata page, twice on apply, and on every status call | **CONFIRMED** | `paired_sync.rs:329` and `:403`. Status goes through `runtime.rs:951-953` to `core.rs:2553-2561`. The scan itself is `core.rs:638-686`. |
| 11 | Mount health is one sticky string that blocks stop | **CONFIRMED** | `mount.rs:339-347` sets it. It is cleared only at `runtime.rs:199-202`. `quiet()` requires it to be empty (`runtime.rs:496-507`), and stop calls `quiet()` (`runtime.rs:987-992`, `mount.rs:94-97`). |
| 12 | Quarantined events still count as pending, and quarantine spreads to every later event | **CONFIRMED** | Pending is `count(*) FROM incoming_events` (`paired_sync.rs:180-184`, `:320-327`). Quarantine is permanent (`core.rs:2416-2421`) and spreads through `validate_direct_links` (`core.rs:3223-3245`). Every new event depends on the whole branch frontier (`core.rs:1574-1582`). |
| 13 | The serving side records no sync status, and the worker has no liveness checks after ready | **CONFIRMED** | `inspect` builds its sync list from `self.peers()` only (`orchestrator.rs:653-672`). `serve_peer` (`:1002-1101`) never writes `links`. The worker accept loop stops on any other error (`runtime.rs:912: Err(_) => break`). Notifications are delivered on the reply path (`runtime.rs:899-901`). Reader sessions are reset on every receive (`runtime.rs:544`). |

Field evidence I re-read (`test-evidence/live-sync-remote-*.json`): the remote journal went 920 → 993 → 1027 events between timestamps 1791856374594 → …754896 → …978848, with pending at 0 → 368 → 367. After recovery that is 34 events in 224 s, about 0.15 events/s, while pending stayed level at one page window. `missing_content_ids` was 0 while 367 events were pending: the bytes for all activated history were present, and the remaining files were waiting on activation.

**Corrections and caveats (nothing dropped):**
- Count of `.lock().unwrap()` call sites: `orchestrator.rs` has **24** and `runtime.rs` has **19**. The reports said "28" and "≈40".
- Worst-case supervisor lock hold: `stop()` ≈ 3 + 15 + 5 = **23 s**. `start()` against an orphan ≈ (3+15)×2 + 5 = **41 s**. The "≈70 s" figure assumes the write timeouts also expire, which small requests rarely hit.
- Gzip: compressing the whole buffer is sent only if `compressed+32 < raw` (`wire.rs:89`). With a large inventory this usually passes even over random object bytes, but it is not guaranteed. **PLAUSIBLE.**
- "The first bulk page after a reconnect carries nothing" is true only for the dialer's upload (`orchestrator.rs:1192` with `bulk_sync.rs:155`). The acceptor's reply uses a fresh inventory (`bulk_sync.rs:270`), so downloads do flow on that page.
- The 50 ms `WORKER_BUSY` retry is effectively replaced by the 500 ms end-of-pass wait whenever nothing moved (`orchestrator.rs:1376-1384`), because `retry_at` is only consulted on the next pass.
- **Not re-run, consistent with the code:** the reader benchmarks (6.2 s / 15.9 s / 40.1 s / 218 s per remote create at 1K / 2K / 3K / 6.4K events; status at 0.13 ms per object; idle control round of 1.13 s at 6.4K), the Windows refused-connect delay of about 1–2 s, and a possible `println!` panic on a broken pipe (`main.rs:297`).

---

## 2. Root causes, deduplicated and ranked by impact

Each entry lists the merged locations and which reports raised it.

### RC1. Activating a remote namespace event replays the whole journal: cost grows roughly with N², it cannot be preempted, and it holds both the engine lock and the SQLite writer *(metadata-engine, tests-evidence, lane-sync)*
- **Fast-path gating:** remote events only get the content fast path (`core.rs:1061-1067`, `1306`, `1412`). Any conflict row, resolved or not, also disables the local fast paths (`core.rs:1346-1351`, `1448-1453`, `1932-1946`).
- **Slow path:** load all branch events and project (`core.rs:1069-1084`) → INSERTs (`1089-1124`) → `persist_projection` (`1143`) → `namespace_history` (`2976`, `3304-3433`), which calls `project()` for each ancestor cut → full snapshot (`1159`, `snapshot.rs:100-101`).
- **Budget:** `core.rs:2350-2430` (cutoff `2380-2387`). The startup drain has no budget at all (`core.rs:466-467`).
- **Measured:** ≈0.15–0.18 events/s in the field. Reader benchmarks put one remote create past the 15 s socket timeout at about 2,000 events. A local create jumps from 1.4 ms to 4.2 s once any conflict row exists.

### RC2. Nothing is kept incrementally: every exchange, idle round and status poll recomputes whole-repository state *(all readers)*
- **Recomputed each time:**
  - full journal parse: `events()` (`core.rs:791-801`);
  - CAS availability sweep, one `stat` plus one SQL query per object (`core.rs:2431-2443`, `527-532`);
  - `content_status` (`core.rs:638-686`, called at `paired_sync.rs:329`, `:403` and from status);
  - a full rewrite of `peer_known` (`core.rs:2177-2187` via `paired_sync.rs:305-309`);
  - `remember_requests`, which autocommits per row and re-parses the journal for each wanted object (`core.rs:618-637`, `533-541`);
  - a causal sort of the whole journal (`core.rs:2475-2500`);
  - a new SQLite connection for every bulk or summary RPC (`bulk_sync.rs:58-60`, `242`; `core.rs:594-610`; `runtime.rs:932-944`).
- **Per bulk round:** 6 availability sweeps and 4 journal parses across the two peers. Idle rounds still do 4 sweeps and 2 parses, every 500 ms, for each lane × repository.
- **Side effect:** every receive replaces the verified read handles of clean open files (`runtime.rs:526-549`), so the next read re-hashes the whole file.

### RC3. Inventories are sent as full sets every page, with fixed caps, and only committed events are advertised *(lane-sync, bulk, metadata, transport, tests)*
- **Full sets:** `known` (`paired_sync.rs:166`); `known_objects`, `partial` and `wanted` (`bulk_sync.rs:186-201`). None of this counts against the page budget.
- **Caps:** `paired_sync.rs:217-224` and `bulk_sync.rs:105-111`. The part cliff comes from `core.rs:2702`. The event cliff comes from `paired_sync.rs:98-102` with the 4 MiB frame set at `orchestrator.rs:1075` and `:1296`, checked at `core.rs:2494`.
- **Resend:** staged-but-unactivated events are left out of `known`, so the sender re-sends the same ~350 KB window every round (`core.rs:2488-2496`).
- **Demand queue:** `wanted` takes the oldest 1,024 requests (`LIMIT 1024`), and requests never expire (`core.rs:545`, `539`).

### RC4. Objects can only flow after the peer activates the metadata, and only for repositories in lane 0's catalog *(lane-sync, bulk, metadata, tests)*
- **Activation gate:** `bulk_sync.rs:155-163` and `core.rs:2764-2776`. `peer_known` is written only by the control lane (`paired_sync.rs:305-309`).
- **Catalog gate:** bulk lanes read `remote_catalogs[lane0]` (`orchestrator.rs:1220-1226`). It is empty until lane 0 lists the remote, and stale while lane 0 is down.

### RC5. Transport is synchronous request/response, and one 15 s socket timeout serves as liveness check, request deadline and server idle timeout *(all readers)*
- **Timeout and call:** `ws.rs:145-152` (applies to every socket) and `ws.rs:238-254`.
- **Server idle drop:** `serve_peer` blocks in receive at `orchestrator.rs:1049`.
- **Lane-wide failure:** the transport-error path at `:1143-1146`, `:1314-1319` and `:1350-1352` drops the lane; the connection counter is a local variable (`:1174`) reset only after a full pass (`:1373`); backoff is at `:1399-1402`.
- **Repeated order:** catalog order is fixed (`:368`, `:955`).
- **Pass preamble:** any error, including a `RemoteError`, on `list` (`:1209`) or `ping` (`:1219`) also resets the lane.
- **Interval vs. idle drop:** `sync_interval_ms` may be 50–60,000 ms (`:73-76`) while the server drops idle sockets after 15 s.
- **No keepalive:** worker connections reconnect after 10 s idle (`runtime.rs:826`); peer lanes have no equivalent.

### RC6. The bulk data path is stop-and-wait with about 2 MiB per page and passes through several hex/JSON hops *(lane-sync, bulk, transport, tests)*
- **Page size:** `bulk_sync.rs:13` and `128-131`; `core.rs:2686-2694`.
- **Ordering:** wanted objects first, then hash order (`core.rs:2678`); each object is pinned to one lane (`bulk_sync.rs:29-32`).
- **Round trips per page:** a ping round trip (`orchestrator.rs:1219`), the objects round trip, 3 loopback RPCs (`:1135-1153`) and a 10 ms yield (`:1380`).
- **Copies:** bytes travel as hex (`core.rs:2593-2597`, `2708`; `wire.rs:20-33`, `142`) with `Value` clones (`ws.rs:190`, `244`; `orchestrator.rs:1081`). Gzip is also negotiated on loopback hops, with a probe that samples only the metadata prefix (`wire.rs:77-89`, `ws.rs:120-123`, `163-177`).
- **Receiver cost per object:** 2 fsyncs, a full re-read hash and a write-through rename (`core.rs:2836-2858`, `3892-3911`).
- **Measured:** 2.0 MiB/s for one 8 MiB object and 2.76 MiB/s for 3 × 4 MiB, on loopback.

### RC7. Pages succeed or fail as a whole: no per-object result or quarantine, and the two directions are coupled *(lane-sync, bulk, transport, metadata)*
- **Abort:** `core.rs:2774-2862`; the sender never re-verifies its own objects (`:2695-2701`).
- **Coupled directions:** `bulk_sync.rs:209-230` and `268-270`; `paired_sync.rs:361-362`; a failed local offer also kills the exchange (`orchestrator.rs:1135-1137`).
- **Registration timeout:** registration waits up to 5 s for the SQLite writer *after* the files are installed (`bulk_sync.rs:219`, `core.rs:596`), and RC1 holds that writer for long periods.
- **Race:** availability is snapshotted before the per-object lock is taken (`core.rs:2773` vs `:2777-2779`), leaving orphan spool files that are never cleaned up.
- **Redundant hashing:** an incoming part for an object that is already available triggers a full re-hash (`:2779-2781`).
- **No error record:** the bulk path never writes `object_errors`.

### RC8. One supervisor lock covers the catalog database, the process table, telemetry and the remote catalogs; it is held across blocking I/O, and coordination works by polling *(orchestrator, lane-sync, lifecycle, transport, gui, tests)*
- **Structure:** `orchestrator.rs:294-306`.
- **Locked lifecycle work:** `start` and `stop` (`490-561`, `606-637`); `reconcile` (`687-740`, called at `1452-1457`); management actions (`844-890`).
- **Polling under the lock:** `peers()?` every 10 ms (`1458`); `lane_active` every 50 ms (`1105-1122`); `serve_peer` takes the lock twice per request (`1051-1070`, `1083-1091`) and re-reads `runtime.json` each time (`477-489`).
- **Catalog fan-out:** `list` calls every worker one after another inside a single 15 s request (`949-993`).
- **Scaling:** `project()` scans all projects, so several callers are O(n²) (`372-377`).
- **Blast radius:** a panic while holding the lock poisons it, and all 24 `.lock().unwrap()` sites then panic.

### RC9. One worker engine lock is shared by filesystem callbacks, sync, status, the recovery thread and notifications *(metadata, lifecycle, gui, bulk, tests)*
- **Lock users:**
  - sync: `runtime.rs:793`, `1037-1046`;
  - status: `951-958`;
  - other control calls: `1003`;
  - recovery thread: `876-885`;
  - notifications: `899-901` and `1205-1231`, which takes a blocking lock at `1228`;
  - filesystem callbacks: `mount.rs:196-198` and `331`.
- **Busy handling:** sync uses `try_lock` and returns `WORKER_BUSY`. The orchestrator counts that as a failure and clears `caught_up` (`orchestrator.rs:227-239`, matched by string).
- **Poisoning:** a poisoned engine lock is reported as `WORKER_BUSY` forever (`runtime.rs:1038-1040`).

### RC10. The worker process lifecycle is tightly coupled and effectively unsupervised *(lifecycle, orchestrator, tests, gui)*
- **Mount and adapters:**
  - the mount must succeed before RPC starts (`main.rs:288-298`);
  - WinFsp sets its global engine before checking the path (`mount.rs:72-77`) and keeps a permanent fence after stop (`mount.rs:98`, `108`);
  - the FUSE managed start/recover functions are never called (`fuse_linux.rs:105-108`, `174-222`).
- **Readiness and restart:**
  - readiness is only noticed by the 2 s `reconcile` (`orchestrator.rs:562-605`);
  - failed workers restart every 2 s with no backoff (`689-695`, `732-737`);
  - each start re-hashes every object and runs an unbounded drain (`core.rs:434-451`, `466-467`);
  - after ready, the only liveness check is process exit (`565`);
  - the RPC accept loop can die while the process keeps running (`runtime.rs:912`).
- **Orphans:** after a daemon crash, the orphan worker is stopped rather than adopted (`499-518`). Workers have no parent-death binding (stdin is null, `530`).
- **Stop and Shutdown:** Stop writes `desired=0` only after a successful stop (`863-868`); Shutdown aborts at the first failure and sets `stopping` last (`869-874`); stop never escalates to a kill (`632`).
- **Initial-sync gate:**
  - it is keyed by the peer's endpoint string;
  - it requires a success within the last 15 s, sampled every 2 s;
  - it requires the remote's pending count to be zero;
  - it hands off with a stop plus restart;
  - Unpair never clears it (`697-731`, `441-446`, `880-883`).

### RC11. Error states are global, sticky and unclassified *(metadata, lifecycle, gui, tests)*
- **Health string:** one `health` string is set by mount operations (`mount.rs:339-347`; FUSE at `fuse_linux.rs:90-100`, `266-268`) and cleared only at `runtime.rs:199-202`. It gates `quiet()` (`496-507`), which in turn gates stop, semantic operations (`695`) and `caught_up` (`585`, `paired_sync.rs:403`).
- **Incoming events:**
  - quarantine is permanent, even for transient errors (`core.rs:2416-2421`);
  - it spreads to every later event (`3223-3245`, because of the whole-frontier dependencies at `1574-1582`);
  - quarantined events count as pending (`paired_sync.rs:180-184`, `320-327`);
  - events with missing parents are skipped silently (`core.rs:2393-2401`).
- **Supervisor errors:** `failures` is cleared only when a worker becomes ready (`orchestrator.rs:593-595`); `publication_errors` is replaced wholesale by whichever peer listed last (`991`).
- **Field impact:** 1,018–1,082 parent IDs were missing on both peers, and the received mount was never published.

### RC12. Status is produced in the wrong place and in the wrong shape *(gui, orchestrator, lane-sync, transport, lifecycle, tests)*
- **Orchestrator side:**
  - only outbound lanes produce status, and only as the last observation, with string errors (`orchestrator.rs:197-282`); `succeeded()` drops `content`;
  - connection backoff is invisible (`1174`, `1399-1402`); `connected=true` is set right after the dial, before any request succeeds (`1199-1205`);
  - the serving side records nothing (`1002-1101`);
  - `mounted` comes from launch arguments (`525`, `557`, `647`); `running=false` during initial sync (`643-646`).
- **Worker side:** the busy fallback carries no counts (`runtime.rs:87-93`, `955-958`).
- **GUI side:**
  - one serial thread opens a new connection per call and polls every 5 s (`gui.rs:394-445`);
  - the folder label is chosen by checks across every paired peer (`181-237`);
  - errors stay on screen after recovery (`731-741`, `836`);
  - the daemon it launches has its output discarded (`399-403`).
- **Logs:** one `eprintln` per failed filesystem operation, with no timestamp or rate limit (`mount.rs:338`).

### RC13. Session topology and peer identity *(orchestrator, lane-sync, transport, bulk)*
- **Re-pair:** creates a complete second set of lanes (`orchestrator.rs:1102-1104`, `1459-1468`), and the old set's status is lost.
- **Mutual pairing:** produces two uncoordinated sessions per peer pair; differing pool sizes shard objects differently.
- **Inventory cache:** keyed by repository only (`1166`, `1192`), so a replica change gives `BULK_ROUTE_MISMATCH` on a healthy socket until reconnect (`bulk_sync.rs:245-248`).
- **Unpair:** does not revoke inbound access (`1002-1055`, `880-883`).
- **Resources and auth:** inbound threads are unbounded (`1444-1451`) and a 64 MiB message is read before authentication (`ws.rs:48`); the pairing code is sent as a replayable bearer secret on every lane (`907`); local endpoints are unauthenticated (`runtime.rs:845-873`).

---

## 3. Causal chains from root cause to symptom

### P1: very slow object sync that sometimes gets stuck (highest impact first)
1. **Activation throttles everything (RC1 → RC3 → RC4).**
   1. A remote create, rename or delete triggers a full replay (seconds at 1K events, 15.9 s at 2K).
   2. The budget then allows one activation per control round (`core.rs:2380-2387`).
   3. The sender re-sends the same window of about 370 events (pending held near 367 in the field).
   4. Bulk offers include only objects of activated events (`bulk_sync.rs:155-163`).
   5. Bulk lanes exchange pages with 0 object bytes.
   6. Observed: 0.15–0.18 events/s, and file content stalls behind metadata.
2. **Slow turns into stuck (any step over 15 s → RC5).**
   - Triggers: RC1 at about 2K events; RC2 sweeps at roughly 50K objects; the final SHA-256 of a multi-GB object (`core.rs:2841-2848`); a 5 s SQLite busy wait; RC8 lock waits; a slow `list` fan-out.
   - The dialer's read times out, the whole lane is dropped, and nothing is charged to the repository.
   - The lane backs off 1→30 s and reconnects, and the same repository is first again: a livelock.
   - Meanwhile the remote worker is still holding its engine, so retries get `WORKER_BUSY`, and the first page after reconnect uploads nothing.
3. **Throughput ceiling (RC6 + RC2 fixed costs).** Per lane, throughput ≈ 2.09 MB ÷ (2·RTT + 3 loopback RPCs + O(repository) scans + per-object fsyncs + 10 ms). That measures 2.0–2.76 MiB/s on loopback. A large object monopolizes its single lane for its whole transfer.
4. **Permanent per-repository stalls:**
   - inventory caps and the >10K tiny-parts cliff (RC3);
   - an event of 349,525 B or more (RC3);
   - a poison object stalls its lane shard and the opposite direction (RC7);
   - a quarantine cascade or orphaned pending events (RC11);
   - a stale inventory cache (RC13).
5. **Contention stalls (RC8, RC9):**
   - lifecycle actions freeze every lane for up to about 41 s;
   - filesystem activity and GUI status polls produce `WORKER_BUSY`, which inflates the failure counter so the next real error jumps to a 30 s backoff;
   - a worker restart leaves lanes failing with `LOCAL_WORKER_UNAVAILABLE` and escalating backoff, so sync can resume up to 30 s after the worker is ready (no reset on readiness).
6. **Restart cost (RC10):** every restart re-hashes all content and runs an unbounded drain, so the folder can be offline for minutes to hours at scale.

### P2: processes are too bound together; dependencies are implicit and not resolved correctly
These are the edges that should not exist:
- **Lifecycle ↔ data plane:** one mutex (RC8).
- **Filesystem ↔ sync ↔ status:** one engine lock (RC9).
- **Mount ↔ readiness ↔ sync:** one process start sequence (RC10).
- **Repository ↔ repository:** one lane socket and one failure domain (RC5).
- **Objects ↔ metadata activation ↔ lane 0 catalog** (RC4).
- **Upload ↔ download:** one request (RC7).
- **Mount errors ↔ stop, semantic operations and `caught_up`** (RC11).
- **Initial mount ↔ one endpoint string ↔ the remote's unrelated pending events** (RC10, RC11).
- **Status computation ↔ the sync hot path** (RC2, RC12).
- **Each folder's catalog entry ↔ every other folder's worker:** the `published()` fan-out (RC8).

The common root is that there are no explicit state machines or dependency graph. Dependencies are implicit, through shared locks, shared sockets and polled booleans. Readiness is inferred by sampling (2 s reconcile, 50 ms `lane_active`, 15 s freshness window) instead of being signalled as events.

### P3: failures that brick things that should not depend on them
| Failure origin | What else stops working | Mechanism |
|---|---|---|
| One folder's Stop, Start, Shutdown, crash loop or orphan probe | All lanes, all inbound peers, the GUI, the accept loop, and remote peers' lanes | RC8 lock held 23–41 s, then remote 15 s timeouts and reconnects |
| One slow or hung worker | Metadata for every folder shared with that peer | The `list` fan-out exceeds 15 s and the control lane resets (RC8, RC5) |
| One slow repository exchange | Every repository after it on that lane | RC5 head-of-line blocking with a fixed order |
| A mount error | That folder's sync; Stop; Shutdown (aborts); semantic operations; `caught_up` | RC10 and RC11 |
| One bad incoming event | Every later event on that branch; `caught_up` on **both** peers; the initial mount | RC11 cascade, plus the remote's pending count in `metadata_caught_up` |
| One bad object | Its lane shard, and the opposite direction of that exchange | RC7 |
| One SQLite error in the main loop | The daemon exits and its workers are orphaned | `orchestrator.rs:1458` |
| A panic while holding a lock | Whole daemon (supervisor lock) or permanent `WORKER_BUSY` (engine lock) | RC8, RC9 |
| An accept error in the worker | That folder looks "running" but cannot be reached or stopped | `runtime.rs:912` |
| Unpairing the peer an initial sync came from | That folder stays "starting" forever | RC10 |
| `sync_interval_ms` ≥ 15 s | Every lane reconnects on every idle cycle | RC5 |
| Unauthenticated connection floods | Daemon runs out of memory | RC13 |

### P4: the UI does not show enough sync status
- **Producers:**
  - only outbound lanes report;
  - each lane keeps only its last observation;
  - connection backoff and in-flight work are invisible;
  - there are no rates, remaining bytes, ETA or stall detection;
  - errors are unclassified strings;
  - the serving side records nothing (RC12).
- **Cost:** status is O(repository) under the engine lock. When the lock is busy, the counts are blanked (RC2, RC9). Polling for status also slows sync and makes it look broken.
- **Semantics:** `mounted` comes from launch arguments; `running=false` during initial sync; old failures stay on screen; the folder label depends on unrelated peers; the daemon's diagnostics are discarded (RC12).

---

## 4. Invariants any rewrite must preserve

**Integrity and durability**
1. An object enters the CAS and the advertised inventory only after a whole-object SHA-256 check and a durable install.
2. Overlapping resume data is verified byte-for-byte before appending; gaps are never accepted; a spool offset is advertised only after fsync.
3. Registration never precedes the install. A crash between install and registration must be recoverable, by re-registering or by safe re-transfer.
4. SQLite runs with `synchronous=FULL`. The journal insert, projection, outbox, frontier, conflicts and snapshot commit atomically.
5. Startup snapshots are disposable caches; the journal is authoritative.
6. Journal events are immutable: one ID maps to one payload (`EVENT_ID_REUSED`). Existing payloads, including their whole-frontier dependencies, cannot be rewritten, so any causal index must be derived.
7. Object sizes are immutable per hash (`OBJECT_SIZE_CHANGED`).
8. Dirty file sessions survive detach, stop and restart, either committed or held in `pending_saves`.

**Causality and acknowledgement**
9. An event activates only after its dependencies and per-register parents are committed. Children that arrive early are durably staged.
10. ACKs and advertised `known` sets cover committed events only, bound to the request and to the sent set (`UNBOUND_OR_EARLY_ACK`, `UNBOUND_PEER_RESPONSE`). A new "staged" ack must be a distinct field and must never be treated as an activation ack.
11. Conflict records are deterministic and independent of delivery order, and must match full replay, including historical register and namespace cuts. The `NAMESPACE_HISTORY_LIMIT` refusal happens before ACK.

**Authorization and privacy**
12. Private branches and events never leave the replica.
13. Only objects referenced by shared events, and present in `published_objects` or `received_shared_objects`, may be sent. A hash-only manifest or request never grants access.
14. Relayed pages cannot rewrite this replica's own events (`FORGED_EVENT_ORIGIN`). Only allowed devices may author accepted events.
15. Pages stay bound to repository, sender, receiver and lane (route binding).

**Protocol and transport**
16. Never replay an uncertain request after a transport error. Retries must be idempotent through request IDs and receipts.
17. A complete `RemoteError` leaves the session usable.
18. The control lane carries no object bytes; bulk lanes never activate or ACK metadata; control traffic never queues behind content.
19. Exactly one owner per object per session (`Lane::owns`), plus per-object locks.
20. Compatibility fails closed. Today's markers are hello `version 2`, `PEER_FORMAT 6`, bulk `PROTOCOL 1` and `X-Tkfs-Wire: tkfs-binary-gzip-v1`; with no reserved-lane support the connection is refused (`RESERVED_LANES_UNSUPPORTED`), never silently downgraded. A v3 protocol must be negotiated, keep a v2 path or an explicit incompatible state, and never quietly mix semantics.
21. Pairing stores only a salted verifier. Rotating the code invalidates live sessions. The installation ID must match the stored one (`PEER_INSTALLATION_CHANGED`). Self-pairing is refused. The code never appears in status output.
22. Untrusted input stays bounded: frame and message size, inflated length, attachment layout, offsets ≤ `MAX_CONTENT`. If the full-set caps are removed, equivalent bounds must replace them.

**Process and lifecycle**
23. One orchestrator per data directory (`orchestrator.lock`) and one worker per state (`owner.lock`). Adoption or restart must never create a second owner.
24. Store recovery finishes before any RPC, sync page or filesystem callback reaches the engine.
25. Workers never dial peers. Peer networking stays in the orchestrator, and workers are reached over loopback only.
26. `desired_running` and the initial-sync marker survive daemon restarts. A stopped folder is never auto-restarted. A gated folder is never mounted before its gate is satisfied or explicitly overridden.
27. Mount paths never overlap the state or data directories or another mount. Never delete or unmount anything that is not proven to be ours.
28. Watchdog semantics: real progress may take arbitrarily long, and progress must come from the recovering thread itself.
29. Waits and backoff stay interruptible by shutdown and unpair. The Shutdown reply is flushed before exit.
30. Status keeps three claims separate: metadata caught up, local content complete, and remote backup. Status never blocks on lifecycle or engine locks and never writes durable state.

---

## 5. Revamp pillars

### Pillar 0: qualification harness (a prerequisite gate, not architecture)
- **What it needs:**
  - a fault-injecting TCP proxy (latency, bandwidth, stall longer than 15 s, connection resets);
  - a stub worker that hangs;
  - scale fixtures: 2K–20K namespace events, 50K–100K objects, 10K tiny files, 1–8 GB objects;
  - lock-hold and throughput instrumentation;
  - release-mode floors in CI.
- **Scenarios that become regression tests (all untested today):**
  - transport-level head-of-line blocking;
  - `sync_interval_ms` ≥ 15 s;
  - a corrupt object on the sending side;
  - quarantine cascades and orphaned pending events;
  - an unbudgeted startup drain and full re-hash on restart;
  - a mount failure while sync should continue;
  - Shutdown with one busy folder;
  - a crash loop's lock occupancy;
  - inbound-only status;
  - mutual pairing.

### Pillar A: incremental semantic store (O(delta) engine)
- **Goal:** activation and per-exchange bookkeeping cost that does not grow with history. No whole-repository scans on hot paths.
- **Scope:**
  - `core.rs` `commit_ready` (972-1191): allow the creation and namespace fast paths for remote events when their dependencies cover the frontier or a slot index proves there is no concurrent placement; gate them on unresolved conflicts that touch the same cone, not the global table.
  - `persist_projection` and `namespace_history` (2866-3433): replace with an incremental per-cone conflict index that evaluates only antichains containing the new event.
  - Compute the projection before opening the write transaction. Seal snapshots on their normal cadence (`core.rs:1159`, `snapshot.rs:85-104`).
  - `drain_incoming` (2318-2430): a work-unit budget that excludes load time, run by a background activator (startup included, `core.rs:466`).
  - New derived tables: `event_objects`, `object_availability(hash, size, state, verified_at, lane_bucket, generation)`, per-origin sequence/version vectors (built on `journal_sequence`), content counters, and an `incoming_events` status (`staged`, `blocked_on{parents}`, `quarantined{class}`).
  - Replace the scans in `core.rs:527-686`, `2431-2443`, `533-541` and `618-637`.
  - Replace startup verification (`core.rs:405-451`) with recorded verification markers plus a background scrubber.
- **Replaces:** activation by full replay; stat sweeps on every call; `content_status` in the sync path; full re-hash on startup; string-only quarantine.
- **Resolves:** RC1 and RC2 on the worker side; RC11 (error classification and pending accounting); RC10 startup cost.
- **Risks:**
  - Conflict determinism is the biggest risk. Mitigate with differential fuzzing (incremental vs. full replay across random delivery orders) plus the existing equivalence tests: `replication_regressions`, `scalability::compressed_presence_witnesses_match_exhaustive_causal_cuts`, and `startup_snapshots`.
  - Schema migration (precedent: `availability_v1`, `core.rs:407-455`).
  - Drift between derived tables and the CAS, which needs a reconciliation pass.
- **Depends on:** nothing. It feeds Pillars B, E and F.

### Pillar B: sync protocol v3, cursor and delta anti-entropy with streamed objects
- **Goal:** steady-state exchanges cost O(delta); objects flow independently of activation; results are per object; throughput is bound by bandwidth rather than RTT.
- **Scope:** `paired_sync.rs:28-409`, `bulk_sync.rs:63-277`, `core.rs:2475-2863`, and `orchestrator.rs` `exchange_bulk` and the per-repository exchange (1125-1363).
- **B1, metadata:**
  - per-origin high-water marks or version vectors, plus a digest so idle repositories short-circuit;
  - a staged ack separate from the activation ack, so senders stop resending;
  - a `need` list for missing parents and a `rejected` set for quarantined events;
  - oversized events sent alone or chunked up to `MAX_EVENT`;
  - push and pull as independent, idempotent operations;
  - per-object results and quarantine with backoff on both sides.
- **B2, objects:**
  - authorization from validated, staged manifests (shared branch, allowed device) or an explicit content-descriptor list, keeping invariant 13;
  - receiver-driven credit and range requests, which also deduplicate multiple sources and mutual pairing;
  - binary `Bytes` chunks instead of hex;
  - several chunks in flight per lane, with incremental SHA-256 state per spool;
  - priority classes: demand > current projection > history, with fair sharing by size;
  - group-commit durability for small objects;
  - optional range striping of large objects across lanes (sparse spool plus range bitmap).
- **Negotiation:** a single capability handshake replaces the scattered version fields (`orchestrator.rs:907`, `1006-1013`; `paired_sync.rs:204`; `bulk_sync.rs:92`; `ws.rs:120-123`).
- **Replaces:** full-set inventories and caps (RC3), the activation gate (RC4), stop-and-wait pages and hex copies (RC6), page atomicity (RC7), the stale inventory cache (RC13).
- **Risks:**
  - relaxing authorization; must keep passing `bulk_inventory_cannot_authorize_private_bytes` and `paired_peer_cannot_disclose_private_cas`;
  - spool format and resume across versions (`lane_reassignment_and_overlapping_resumes`);
  - mixed-version peers;
  - memory bounds for streaming.
- **Depends on:** A (indices) for both parts; C (framing, streams, credits) for B2. B1 can run on today's transport.

### Pillar C: peer session transport (multiplexed, heartbeat-based, authenticated)
- **Goal:** separate liveness from request latency; one slow request never resets others; per-request deadlines and cancellation; one session per installation pair; inbound sessions become observable.
- **Scope:** `ws.rs:98-254`, `wire.rs` (binary payloads; compression negotiated per link and off on loopback), `orchestrator.rs` `dial_role`, `serve_peer` and `run_lane` (897-1101, 1170-1408), and `runtime.rs` `RpcLane` (814-844).
- **Components:**
  - frames carrying request and stream IDs, with a dedicated reader and writer per connection (tokio plus tokio-tungstenite, or a thread pair);
  - WebSocket ping heartbeat about every 5 s, declared dead after 3 misses; progress frames during long operations;
  - deadlines sized to the payload;
  - error classes {local pre-send, remote application, transport}, each attributed to (repository, operation);
  - TCP keepalive; jittered backoff that resets only after a sustained healthy period; try every resolved address;
  - deterministic session ownership (for example, the lower installation ID dials) and a negotiated partition count;
  - pre-authentication bounds: a hello of at most 16 KiB, a 5 s handshake, compression only after authentication, connection caps;
  - per-peer credentials issued at pairing (challenge-response, Noise or pinned TLS) with individual revocation;
  - a token or ACL on local IPC.
- **Replaces:** the 15 s `SO_RCVTIMEO` request/response model, the lane-wide failure domain, the per-pass ping and list polling, and the bearer pairing code.
- **Resolves:** RC5 and RC13; the pipelining base for RC6; inbound visibility for RC12.
- **Risks:** an async migration touches every I/O path; the no-replay rule must hold; lane 0's priority must survive multiplexing (a separate control session or a strict priority queue); a compatibility window for v2 peers.
- **Depends on:** nothing hard. Co-design with D, whose peer supervisors own the sessions.

### Pillar D: orchestrator as actors (folder controllers, peer supervisors, catalog snapshot, status hub)
- **Goal:** no I/O under shared locks; folder failures stay isolated; explicit state machines with backoff; readiness signalled as events.
- **Scope:** `orchestrator.rs` 294-306, 441-446, 490-740, 768-891, 949-993, 1105-1122 and 1410-1479.
- **Components:**
  - **FolderController** (one actor per folder):
    - states: Stopped, Starting{phase, progress}, Running{instance, discovery}, Stopping{deadline}, Backoff{until, attempt}, Failed{class};
    - stop escalates from RPC to wait to kill to reap;
    - classified restart backoff with jitter;
    - adopts orphans through `worker.json{pid, instance, version}`, plus a Job Object or `PR_SET_PDEATHSIG`;
    - readiness is pushed by the stdout reader;
    - separate `sync_intent` and `mount_intent`;
    - the initial-sync gate becomes a mount gate keyed by installation ID, satisfiable by any peer, with override and timeout, and cleared on unpair.
  - **Catalog actor** publishing an immutable `Arc` snapshot with a generation number (arc-swap). The verifier hash is cached with a rotation generation.
  - **PeerSupervisor** per installation: owns the sessions and lanes, cancels and joins them on reconfiguration, keys status by installation and lane index.
  - **Published-catalog cache:** `list` answers in O(1) with per-folder health and a generation; bulk lanes read the snapshot without depending on lane 0.
  - **Demand scheduler** driven by change generations instead of 500 ms polling of every repository.
  - **Shutdown:** set `stopping` first, stop folders in parallel under one deadline, report per-folder outcomes; install signal handlers; remove `orchestrator.json` through a Drop guard; make per-tick catalog errors non-fatal; use poison-tolerant locks.
- **Replaces:** the global supervisor mutex, the 10 ms poll loop, 2 s reconcile sampling, 50 ms SQL polling, and the serial fan-out.
- **Resolves:** RC8; RC10 on the orchestrator side; the supervisor part of RC11; parts of RC12 and RC13.
- **Risks:**
  - single ownership during adoption (invariant 23);
  - Stop must persist intent first and still satisfy invariant 26;
  - the initial-sync marker may be deleted only after a successful attach;
  - the Shutdown reply must still be flushed.
- **Depends on:** nothing. Co-design with C (sessions) and E (worker event, heartbeat and mount-attach RPC).

### Pillar E: worker decomposition (RPC first, detachable mount, split engine, typed health, liveness)
- **Goal:** worker readiness means "store recovered and RPC up"; a mount failure degrades only the mount; filesystem, sync and status do not block one another; hung workers are detectable.
- **Scope:** `main.rs:269-322`, `mount.rs:59-110` and `178-377`, `fuse_linux.rs:90-222`, and `runtime.rs` at 41-59, 496-551, 845-1073 and 1205-1231.
- **Components:**
  - **Startup order:** lock → open the store (bounded, with lazy verification) → `start_rpc` → `ready{instance, pid, address, version}` → mount attached later by RPC, with its own state machine and backoff.
  - **Mount adapters:** re-attachable (bind the engine once; create a fresh native instance per attach; clear the fence after draining); FUSE recovers stale mountpoints it can prove it owns.
  - **Engine split:**
    - a sync/activation actor that owns the SQLite writer and works in bounded slices;
    - an arc-swapped projection snapshot for filesystem lookups, enumeration and status;
    - per-session byte I/O, hashing and fsync outside the semantic lock;
    - bounded, fair waits instead of `try_lock` returning `WORKER_BUSY`.
  - **Typed health:** {store, pending_saves, mount, notifications}. `quiet()` gates only on store health, dirty sessions and open handles. A forced detach persists dirty sessions first.
  - **Liveness and resilience:** a poisoned lock exits the process; the accept loop survives errors; a stdout heartbeat plus a probe RPC.
  - **Notifications:** delivered on a mount-owned thread, only while attached, bounded and coalesced.
  - **Read handles:** keep verified handles unless the content hash actually changed.
- **Replaces:** mounting before RPC; a single `Arc<Mutex<Engine>>` as the only concurrency tool; the sticky health string; busy errors.
- **Resolves:** RC9; RC10 on the worker side; RC11 health; status off the lock (part of RC2).
- **Risks:** read-your-writes semantics and notification ordering under snapshot reads; WinFsp re-entrancy; checkout's fenced remount; durability of dirty sessions during a forced detach.
- **Depends on:** A (without it, activation slices stay long); D (drives attach and readiness).

### Pillar F: status model and UI
- **Goal:** one owned, versioned, cheap status per folder, peer and transfer that explains slow or stuck sync, pushed to the UI.
- **Scope:** `orchestrator.rs` 197-283, 638-676 and 884-888 plus `serve_peer` accounting; `runtime.rs` 87-93, 563-608 and 951-958; `gui.rs` 61-485 and 721-843; `ui/app.slint`; logging.
- **Components:**
  - a **StatusHub**, separate from lifecycle state;
  - an **error taxonomy**: {code, class (Transient / Contention / NeedsUserAction / Incompatible / Fatal), scope, first_at, last_at, count, next_retry_at, culprit path or object};
  - a **per-(folder, peer) state**: not_shared / offline / discovering / metadata(n/m) / objects(bytes remaining, rate, ETA) / blocked{reason} / backoff / stalled_since / up_to_date;
  - **folder aggregation** that ignores peers not sharing the folder; inbound sessions included;
  - O(1) **worker counters** from Pillar A;
  - **per-lane in-flight** {operation, repository, since, bytes} with exponentially weighted rates;
  - a **stall detector**: work remaining and no progress for more than max(60 s, 20 × interval);
  - an **activity ring buffer**;
  - **one persistent control connection** with push subscription and asynchronous operations with IDs;
  - **logging:** timestamped, rate-limited structured logs (`tracing`), and a daemon log file in GUI mode.
- **Replaces:** the `sync_label` heuristics, three fresh connections every 5 s, blanking counts when busy, raw error strings.
- **Resolves:** RC12, and the status-polling slowdown described under RC2 and RC9.
- **Risks:** low for integrity, but invariants 21 and 30 must hold.
- **Depends on:** D (hub), A (counters), C (inbound session events), E (heartbeat). The schema can be designed first; the GUI comes last.

### Which pillar resolves which root cause
| | A | B | C | D | E | F |
|---|---|---|---|---|---|---|
| RC1 activation | ● | | | | ◐ | |
| RC2 recomputation | ● | ◐ | | | ◐ | ◐ |
| RC3 inventories and caps | ◐ | ● | | | | |
| RC4 object gating | | ● | | ◐ | | |
| RC5 transport timeout and HOL | | | ● | ◐ | | |
| RC6 data plane | | ● | ◐ | | | |
| RC7 page atomicity | ◐ | ● | | | | |
| RC8 supervisor lock | | | | ● | | |
| RC9 engine lock | ◐ | | | | ● | ◐ |
| RC10 lifecycle | ◐ | | | ● | ● | |
| RC11 sticky errors | ● | ◐ | | ◐ | ● | ◐ |
| RC12 status | ◐ | | ◐ | ◐ | ◐ | ● |
| RC13 topology and auth | | ◐ | ● | ◐ | | |

● = primary fix, ◐ = contributes.

Pillars **A + C + D** remove the main mechanisms behind P1 (RC1, RC2, RC5), P2 (RC8, most of RC10) and the widest-reaching P3 failures. **B** removes the throughput ceilings and the permanent stalls. **E** isolates the filesystem from sync. **F** closes P4.

### Order of work
1. **Phase 0.** Build Pillar 0. In parallel, apply tactical stabilizers that already fit the target design:
   - charge transport failures to the in-flight repository and rotate the starting repository (`orchestrator.rs:1234`, `1350-1352`);
   - stop counting `WORKER_BUSY` as a failure or as an error (`:227-239`);
   - keep the server idle timeout above the polling interval: either require interval < idle timeout or ping during `lane_wait` (`:73-76`, `:1115-1122`);
   - remove `content_status` from `paired_sync.rs:329` and `:403`;
   - allow the fast paths for remote events whose dependencies equal the frontier, gated on unresolved conflicts only (`core.rs:1062`, `1306`, `1349`, `1412`, `1450`);
   - register each object as it completes, re-check availability after taking the object lock, and never create a spool for a part with offset > 0 (`core.rs:2773-2858`, `bulk_sync.rs:209-230`);
   - release the supervisor lock around stop/start RPCs and waits, and add crash backoff;
   - start RPC before the mount (`main.rs:288-298`);
   - exclude quarantined rows from pending and classify transient errors (`paired_sync.rs:180-184`, `320-327`; `core.rs:2416-2421`);
   - clear the inventory cache on any bulk error;
   - keep the worker accept loop alive on errors (`runtime.rs:912`) and make the main loop's `peers()` error non-fatal (`orchestrator.rs:1458`).
2. **Phase 1.** A, C and D in parallel. They are largely independent.
3. **Phase 2.** E (needs A's short slices and D's controller) and B1 (needs A).
4. **Phase 3.** B2 (needs C) and F (needs D, E and A).
