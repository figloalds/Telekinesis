# Orchestration Core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restore the orchestrator daemon, supervised per-folder workers, lane-structured orchestrator↔worker and orchestrator↔orchestrator transport, the `manage` CLI, pairing, signals and clock discipline on top of the Wave 6 runtime, so headless multi-folder sync works on Linux and Windows from `tkfs -f orchestrator.toml start`.

**Architecture:** One `tkfs orchestrator` process per data directory owns `orchestrator.toml`, the catalog (`DATA/folders.sqlite`), a loopback control endpoint for `manage`, and the peer network. It spawns/adopts one `tkfs daemon --state S --supervised` worker per folder and talks to it over the existing frame protocol (`src/session/*`) on loopback: lane 0 control, lanes 1..N bulk. Workers never listen for peers; the orchestrator routes `meta.exchange` and `objects.*` between peer lanes and worker lanes and never stores objects itself. Workers signal object demand by completing the orchestrator's parked `watch` long-poll.

**Tech Stack:** Rust 2024, existing crates only (`tungstenite`, `rusqlite`, `serde`, `clap`, `toml`, `libc` on Unix, `extern "system"` on Windows). No new dependencies.

**Spec:** `docs/superpowers/specs/2026-10-28-orchestration-restoration-design.md` (§7.1–7.4, 7.7, 7.9 local part, 7.10, 7.11, 7.12). Mounts/attach/detach/freshness (§7.5, 7.8), branches (§7.6), remote conflicts (§7.9 remote) and the GUI client are later plans.

## Global Constraints

- `AGENTS.md`: no crash/termination dependency on served-namespace handles; every wait in tests and daemon code is bounded; a stranded process is a test failure; no mounts in these tests (all folders headless, never pass `--mount`).
- Spec §5: reuse `src/session/*` framing for loopback and peer lanes; no new crates; tokens, catalog and `worker.json` are created owner-only (`0600` on Unix).
- Spec §7.2: the orchestrator is the session client on every lane; only the client originates requests (`src/session/conn.rs` `ensure_client`).
- Spec §7.7: all stored/transmitted times are UTC ms; `CLOCK_INVALID` below `2020-01-01T00:00:00Z` = `1_577_836_800_000` ms.
- Spec §7.4 config: `format_version` must be `2`; `maximum_running` 1–64 (default 8); `[network] listen` default `"127.0.0.1:43180"`, `""` = dial only; `advertise` default `""`; `connections` 1–16 (default 4, effective pool ≥ 2); `worker_lanes` 1–8 (default 2); `sync_interval_ms` 50–60000 (default 500); `[freshness] open_wait_ms` 0–5000 (default 250). Unknown fields → `INVALID_CONFIG`.
- Spec §7.4: no legacy import; unknown catalog schema → `CATALOG_INCOMPATIBLE`.
- Master numbers (spec §6, §8): heartbeat 1 Hz; `watch` wait 2 s, deadline 4 s; no heartbeat 8 s or 3 failed watches → `hello` probe → kill; backoff `1 s·2^(n−1)` cap 300 s ±20 %; breaker 5 failures / 10 min → `CRASH_LOOP`; `stop` waits 13 s, `force-stop` 15 s; unsupervised quiet exit 600 s (`TKFS_UNSUPERVISED_EXIT_S`); lane dial 3 s; lane backoff 1→60 s ±20 %, reset after 5 s Ready; `Incompatible` 10 min; `Unauthorized` 60 s; ≤ 4 concurrent exchanges, one per (peer, repo); inbound caps 32 pre-auth / 128 sessions.
- Before every commit: `cargo fmt`, `cargo clippy --all-targets -- -D warnings`, and the task's tests. Linux uses `CARGO_TARGET_DIR=$HOME/tkfs-target/linux`.
- Commit messages end with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`.

## Review Focus

1. A system clock step (NTP correction of +1 h) while a folder is in `Backoff` must not shorten or extend the backoff; timers are monotonic (`util::time::Clock::instant`). Test in Task 9.
2. A stale `worker.json` whose pid now belongs to another process (same pid, different start time) must not be adopted or killed; the controller spawns fresh once `owner.lock` is free. Test in Task 7.
3. An `orchestrator.toml` with an unknown field fails `INVALID_CONFIG`; a relative `data_directory` resolves against the file's directory, not the cwd. Test in Task 5.
4. A second orchestrator on the same data directory exits `ALREADY_RUNNING` (code 75) and leaves the running one's `orchestrator.json`/`orchestrator.token` untouched. Test in Task 10.
5. `docker stop` while a worker refuses `shutdown` (files open or a hung worker) must still end within the 30 s bound, exit 0 and log `FORCED_STOP`; the next start recovers the store. Test in Task 10 (fault `shutdown.hang`).

## File Structure

| File | Responsibility |
|---|---|
| `src/clock.rs` (new) | Clock floor check, NTP-style offset sample, smoothing |
| `src/model/protocol.rs` | Protocol/IPC version 4, lane op policy |
| `src/orchestrator/worker.rs` (new) | Worker process entry: runtime files, Ready line, loopback server, `hello`/`watch`/`shutdown`, quiet exit |
| `src/orchestrator/demand.rs` (new) | `Seq` (watch wake-up) and `Demand` (`ObjectFetcher` that waits for the orchestrator) |
| `src/orchestrator/control.rs` | Worker-side control ops (`execute`) and the loopback client `request`; filesystem ops unchanged |
| `src/orchestrator/sync.rs` | Feed/objects ops: `feed.outgoing`, `feed.apply`, `meta.exchange`, `objects.fetch`, `objects.write`, `objects.missing` |
| `src/orchestrator/settings.rs` (new) | Worker store `config` table accessors (`label`) — the remains of today's worker `Catalog` |
| `src/orchestrator/config.rs` (new) | `orchestrator.toml` format 2 |
| `src/orchestrator/catalog.rs` | Orchestrator catalog `DATA/folders.sqlite`: folders, settings, verifier, peers, clock offsets; `AcceptPolicy` |
| `src/orchestrator/process.rs` (new) | Spawn, Ready/Fatal line parsing, `worker.log`, identity, adoption, `kill_verified`, `ObservedProcess` |
| `src/orchestrator/worker_client.rs` (new) | `WorkerLink`: 1 control + N bulk loopback lanes, `watch` |
| `src/orchestrator/folder.rs` (new) | `Controller` state machine per folder |
| `src/orchestrator/hub.rs` (new) | `Daemon`: run loop, actions, shutdown sequence, status snapshot |
| `src/orchestrator/endpoint.rs` (new) | Orchestrator control endpoint (`orchestrator.json`/`.token`) and `ControlClient` |
| `src/orchestrator/manage.rs` (new) | `manage` command → action; `status`/`watch` rendering |
| `src/orchestrator/signals.rs` (new) | SIGINT/SIGTERM, Ctrl-C/console close → `Cancel` |
| `src/orchestrator/network.rs` (new) | Peer lanes, dial state machine, inbound accept, `PeerService` router, sync scheduler, object pump, pairing, clock probe |
| `src/main.rs` | CLI: global flags, `init`, `orchestrator|start`, `manage`, `create`, `daemon`, filesystem commands |
| `src/desktop.rs` | Uses `process::ObservedProcess`; otherwise unchanged until the GUI plan |
| `tests/support/daemon.rs` (new) | `TestDaemon` harness around the real binary |
| `tests/worker_lifecycle.rs`, `tests/orchestrator.rs`, `tests/peer_sync.rs`, `tests/docs_cli.rs` (new) | Integration tests |
| `docs/CLI.md`, `docs/CONFIGURATION.md`, `docs/LINUX.md`, `README.md`, `examples/docker-compose.yml`, `deploy/tkfs-orchestrator.service.example` | Updated to the restored commands |

---

### Task 1: Clock module

**Files:**
- Create: `src/clock.rs`
- Modify: `src/lib.rs` (add `pub mod clock;`)

**Interfaces:**
- Produces:
  ```rust
  pub const FLOOR_MS: u64 = 1_577_836_800_000;
  pub const WARN_OFFSET_MS: i64 = 5_000;
  pub fn check() -> anyhow::Result<()>;              // uses util::time::now_ms()
  pub fn check_ms(now_ms: u64) -> anyhow::Result<()>; // Err "CLOCK_INVALID: system clock reads {now_ms} ms (before 2020-01-01T00:00:00Z); fix the clock (NTP) before running tkfs"
  #[derive(Clone, Copy, Debug, Serialize, Deserialize)]
  pub struct Sample { pub t1: u64, pub t2: u64, pub t3: u64, pub t4: u64 }
  #[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
  pub struct Offset { pub offset_ms: i64, pub rtt_ms: u64 }
  impl Sample { pub fn offset(&self) -> Offset; } // offset = ((t2−t1)+(t3−t4))/2; rtt = (t4−t1) − (t3−t2), saturating at 0
  pub fn smooth(previous: Option<i64>, sample: i64) -> i64; // None → sample; Some(p) → p + (sample − p)/4
  ```

- [ ] **Step 1: Write the failing tests** in `src/clock.rs` `#[cfg(test)] mod tests`

```rust
#[test]
fn floor_is_2020_01_01_utc() { assert_eq!(FLOOR_MS, 1_577_836_800_000); }
#[test]
fn clock_before_floor_is_invalid() {
    let error = check_ms(FLOOR_MS - 1).unwrap_err().to_string();
    assert!(error.starts_with("CLOCK_INVALID"), "{error}");
    assert!(check_ms(FLOOR_MS).is_ok());
    assert!(check_ms(1_790_000_000_000).is_ok());
}
#[test]
fn ntp_offset_and_rtt() {
    let s = Sample { t1: 1000, t2: 1600, t3: 1610, t4: 1020 };
    assert_eq!(s.offset(), Offset { offset_ms: 595, rtt_ms: 10 });
}
#[test]
fn smoothing_takes_first_sample_then_quarter_steps() {
    assert_eq!(smooth(None, 400), 400);
    assert_eq!(smooth(Some(400), 800), 500);
    assert_eq!(smooth(Some(0), -400), -100);
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --lib clock::`
Expected: compile error, `clock` module missing.

- [ ] **Step 3: Implement `src/clock.rs`** with the signatures above; `check()` calls `check_ms(crate::util::time::now_ms())`.

- [ ] **Step 4: Run tests**

Run: `cargo test --lib clock::`
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add src/clock.rs src/lib.rs
git commit -m "feat: clock floor check and NTP-style peer offset"
```

---

### Task 2: Protocol version 4 and lane op policy

**Files:**
- Modify: `src/model/protocol.rs:4-10`, `src/model/protocol.rs:83-85`
- Modify: `tests/session_handshake.rs`, `tests/session_core.rs` (string expectations `"3"`, `"3-loopback"`, version arrays)

**Interfaces:**
- Produces:
  ```rust
  pub const PROTOCOL_VERSION: u32 = 4;
  pub const SUPPORTED_VERSIONS: &[u32] = &[4];
  pub const UPGRADE_V3: &str = "4";            // identifier kept, value bumped
  pub const UPGRADE_LOOPBACK: &str = "4-loopback";
  pub const IPC_VERSION: u32 = 4;              // new
  pub fn op_allowed(op: &str, role: LaneRole) -> bool;
  ```
  Policy: `Probe` → `ping | peer.enroll | catalog.list | clock.probe`; `Bulk` → `ping` or `op.starts_with("objects.")`; `Control` → everything that does not start with `objects.`.

- [ ] **Step 1: Write the failing tests** in `src/model/protocol.rs` tests module

```rust
#[test]
fn control_lane_never_carries_objects_and_bulk_only_does() {
    assert!(!op_allowed("objects.fetch", LaneRole::Control));
    assert!(!op_allowed("objects.write", LaneRole::Control));
    assert!(op_allowed("meta.exchange", LaneRole::Control));
    assert!(op_allowed("clock.probe", LaneRole::Control));
    assert!(op_allowed("objects.fetch", LaneRole::Bulk));
    assert!(!op_allowed("meta.exchange", LaneRole::Bulk));
    assert!(op_allowed("clock.probe", LaneRole::Probe));
    assert!(!op_allowed("meta.exchange", LaneRole::Probe));
}
#[test]
fn protocol_is_version_4() {
    assert_eq!(SUPPORTED_VERSIONS, &[4]);
    assert_eq!(UPGRADE_V3, "4");
    assert_eq!(UPGRADE_LOOPBACK, "4-loopback");
    assert_eq!(IPC_VERSION, 4);
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --lib protocol::`
Expected: FAIL (`clock.probe` refused on Probe, `objects.fetch` allowed on Control, version 3).

- [ ] **Step 3: Implement** the constants and `op_allowed`. Confirm with `grep -n op_allowed src/session` where the server enforces it (the handler dispatch in `src/session/conn.rs` or `handshake.rs`); if it is not enforced server-side, enforce in `Conn::serve` before calling the handler with error `OP_NOT_ALLOWED`.

- [ ] **Step 4: Update test expectations** in `tests/session_handshake.rs` and `tests/session_core.rs` that assert the literal `"3"`/`"3-loopback"` header values or `[3]` version lists.

- [ ] **Step 5: Run tests**

Run: `cargo test --lib protocol:: && cargo test --test session_handshake --test session_core`
Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add src/model/protocol.rs tests/session_handshake.rs tests/session_core.rs src/session
git commit -m "feat: protocol v4 and per-lane op policy (control never carries objects)"
```

---

### Task 3: Worker runtime files, Ready line and loopback control server

**Files:**
- Create: `src/orchestrator/worker.rs`, `src/orchestrator/demand.rs`, `src/orchestrator/settings.rs`
- Modify: `src/orchestrator/control.rs` (replace plain-WS server/client with loopback sessions; add `hello`, `watch`, `shutdown{force}`), `src/orchestrator/mod.rs` (export modules; `run` delegates to `worker::run`), `src/main.rs` (`daemon --supervised`; call `clock::check()` first), `src/desktop.rs:325-336` (no change to arguments; it still passes `--state … daemon --listen`; remove `--listen` there)
- Test: `tests/worker_lifecycle.rs`

**Interfaces:**
- Consumes: `session::accept_loopback(stream, Some(token), ConnOptions)`, `session::dial_loopback(address, Some(token), ConnOptions::for_role(LaneRole::Control))`, `Conn::serve(Arc<dyn Handler>)`, `Conn::call(op, header, body, Priority, Duration)`, `Engine::{request_shutdown, writable_tokens, stopping, error}`, `util::log::{register_secret, redact_line}`, `clock::check`.
- Produces:
  ```rust
  // src/orchestrator/demand.rs
  pub struct Seq;                                   // Mutex<u64> + Condvar
  impl Seq { pub fn new() -> Arc<Self>; pub fn bump(&self) -> u64; pub fn current(&self) -> u64;
             pub fn wait_past(&self, after: u64, timeout: Duration) -> u64; }
  pub struct Demand;                                // wanted: Mutex<BTreeMap<Digest, Kind>>, cv, seq, packs
  impl Demand { pub fn new(seq: Arc<Seq>, packs: PackStore) -> Arc<Self>;
                pub fn pending(&self) -> Vec<(Digest, Kind)>;
                pub fn arrived(&self, digests: &[Digest]);   // removes and notifies
                pub const FETCH_WAIT: Duration = Duration::from_secs(10); }
  impl crate::content::ObjectFetcher for Demand;    // register → seq.bump() → wait until packs.contains all or FETCH_WAIT → "OBJECT_MISSING: …"

  // src/orchestrator/settings.rs  (was worker-level Catalog get/set)
  pub struct Settings { pub engine: Arc<Engine> }
  impl Settings { pub fn get(&self, key: &str) -> Result<Option<String>>; pub fn set(&self, key: &str, value: &str) -> Result<()>; }

  // src/orchestrator/worker.rs
  pub const EXIT_INTERNAL: i32 = 70; pub const EXIT_STORE: i32 = 73; pub const EXIT_LOCKED: i32 = 75; pub const EXIT_CONFIG: i32 = 78;
  pub const UNSUPERVISED_EXIT_DEFAULT_S: u64 = 600; // env TKFS_UNSUPERVISED_EXIT_S
  pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);
  pub const WATCH_MAX_WAIT_MS: u64 = 5000;
  #[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
  pub struct WorkerRecord { pub format: u32 /*1*/, pub pid: u32, pub start_ms: u64, pub image: PathBuf,
                            pub version: String, pub instance: String, pub address: String, pub ipc: u32 }
  #[derive(Clone, Debug, Serialize, Deserialize)]
  pub struct Ready { pub ready: bool, pub repo: String, pub device: String, pub instance: String, pub pid: u32,
                     pub address: String, pub token: String, pub version: String, pub ipc: u32, pub mounts: Vec<PathBuf> }
  pub struct Options { pub supervised: bool, pub mount: Option<PathBuf> }
  pub fn run(engine: Arc<Engine>, options: Options) -> Result<()>;   // the worker main; see steps
  pub struct ControlService { /* engine, settings, demand, seq, supervised_contact: Mutex<Instant> */ }
  impl session::Handler for ControlService;         // dispatches to control::execute
  ```
  Files in `STATE/`: `runtime.token` (0600) written only after a previous `runtime.json` is removed; `runtime.json` `{"address","pid","instance","ipc":4,"mounts":[]}`; `worker.json` = `WorkerRecord`. All three removed on clean exit. `control.json` is gone.
  - `control::request(root: &Path, operation: &str, payload: Value) -> Result<Value>`: reads `runtime.json` + `runtime.token`, dials loopback, `call(op, payload, vec![], Priority::Control, 10 s)`, parses body as JSON; a missing `runtime.json` → `WORKER_NOT_RUNNING`.
  - Reply convention for every worker op: success = header `{}` + JSON body; failure = `responder.error(ErrorWire::coded(code, message))` where `code` is the message's leading `SCREAMING_CASE` token (before `:`) or `WORKER_ERROR`.
  - New ops in `control::execute`: `hello` → `{repo, device, instance, pid, version, ipc, mounts}`; `watch {after_seq, wait_ms}` → `{seq, heartbeat:{at_ms, entries, conflicts, writable_handles, error}, demand:[[digest,kind],…]}` (returns at once if `seq > after_seq` or demand non-empty, else waits ≤ min(wait_ms, 5000)); `shutdown {force}` → `ensure!(force || engine.writable_tokens().is_empty(), "BUSY_VIEW: {n} files open")` then `request_shutdown`.
  - Every op records `supervised_contact = now`; with `supervised`, a 1 Hz thread exits the process (`request_shutdown`) when `writable_tokens().is_empty()` and no contact for `UNSUPERVISED_EXIT_S`.
  - Fault names honoured by `inject-fault`: `shutdown.hang` (the `shutdown` op sleeps 60 s before replying).

- [ ] **Step 1: Write the failing tests** `tests/worker_lifecycle.rs` (spawns `env!("CARGO_BIN_EXE_tkfs")`, every wait ≤ 30 s, kills and fails on timeout)

```rust
fn spawn_worker(state: &Path, supervised: bool) -> (Child, Ready) // runs `tkfs --state S init` then `daemon [--supervised]`, reads stdout until a line with "ready":true (≤30 s)
#[test]
fn worker_writes_runtime_files_and_ready_line_then_removes_them_on_shutdown() {
    let home = tempfile::tempdir().unwrap(); let state = home.path().join("s");
    let (mut child, ready) = spawn_worker(&state, true);
    assert_eq!(ready.ipc, 4); assert!(ready.address.starts_with("ws://127.0.0.1:"));
    let runtime: Value = json(&state.join("runtime.json")); assert_eq!(runtime["address"], ready.address);
    let record: WorkerRecord = json(&state.join("worker.json")); assert_eq!(record.pid, ready.pid);
    assert_eq!(fs::read_to_string(state.join("runtime.token")).unwrap(), ready.token);
    #[cfg(unix)] assert_eq!(fs::metadata(state.join("runtime.token")).unwrap().permissions().mode() & 0o777, 0o600);
    let hello = control::request(&state, "hello", json!({})).unwrap(); assert_eq!(hello["repo"], ready.repo);
    control::request(&state, "shutdown", json!({"force": false})).unwrap();
    wait_exit(&mut child, Duration::from_secs(30)); // asserts exit code 0
    for f in ["runtime.json", "runtime.token", "worker.json"] { assert!(!state.join(f).exists(), "{f}"); }
}
#[test]
fn watch_returns_immediately_when_seq_advanced_and_waits_otherwise() {
    // after Ready: watch{after_seq:0, wait_ms:2000} returns seq ≥ 1 within 1.5 s (heartbeat bumps at 1 Hz);
    // a second watch{after_seq: seq, wait_ms: 300} takes ≥ 250 ms and ≤ 1500 ms.
}
// In src/orchestrator/control.rs `#[cfg(test)]` (in-process Engine, no binary):
#[test]
fn shutdown_without_force_is_refused_while_a_writable_handle_is_open() {
    // engine.open_file("f", true, false, true) (writable) held in `h`;
    // execute(&engine, &settings, "shutdown", &json!({"force": false})) → Err whose message starts with "BUSY_VIEW: 1 files open";
    // engine.stopping is still false; execute(... "shutdown", {"force": true}) → Ok({"stopping":true}); engine.close(h) afterwards.
}
#[test]
fn unsupervised_worker_exits_quietly_after_no_contact() {
    // TKFS_UNSUPERVISED_EXIT_S=2 in the child's env, --supervised, no requests after Ready → exits 0 within 10 s,
    // runtime files removed.
}
#[test]
fn worker_refuses_to_start_when_the_clock_is_invalid() {
    // env TKFS_FAKE_NOW_MS=1_000_000_000_000 (only honoured when cfg!(debug_assertions)); `daemon` exits 78 and prints
    // a line containing "CLOCK_INVALID" on stdout as {"fatal":true,"code":"CLOCK_INVALID",...}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --test worker_lifecycle`
Expected: compile errors (`worker::Ready`, `WorkerRecord` missing).

- [ ] **Step 3: Implement `demand.rs`, `settings.rs`, `worker.rs`; rework `control.rs`**. In `worker::run`: `clock::check()` (fatal 78, printed as `{"fatal":true,"code","message","exit"}`); `util::time::now_ms` honours `TKFS_FAKE_NOW_MS` under `cfg!(debug_assertions)` only; bind `127.0.0.1:0`; generate token (`ws::tokens::generate()`), `util::log::register_secret(&token)`; remove stale `runtime.json`; write token (0600 via `OpenOptions` + `PermissionsExt` on Unix); write `runtime.json`, `worker.json` (`process::process_identity` comes in Task 7 — for now `start_ms = now_ms()`, `image = current_exe()`; Task 7 replaces this); print Ready (one line, stdout, flushed); accept loop with `accept_loopback` + `conn.serve(Arc<ControlService>)`; heartbeat thread bumps `Seq` every second; wait for `engine.stopping`; `engine.shutdown()`; remove the three files. Keep `Command::Rpc` and the filesystem commands working through the new `control::request`.

- [ ] **Step 4: Remove the worker's peer listener arguments** `--listen`/`--advertise` from `daemon` in `src/main.rs` and from `src/desktop.rs:325-336`; `orchestrator::run` becomes `worker::run`. (The peer loop in `orchestrator/mod.rs` is deleted in Task 4; until then it is simply not started.)

- [ ] **Step 5: Run tests**

Run: `cargo test --test worker_lifecycle --test desktop && cargo test --lib orchestrator::control`
Expected: pass. `tests/desktop.rs` still passes because `Desktop` uses `--state … daemon` and `rpc`.

- [ ] **Step 6: Commit**

```bash
git add src/orchestrator src/main.rs src/desktop.rs src/lib.rs tests/worker_lifecycle.rs
git commit -m "feat: worker runtime files, Ready line and loopback control lane"
```

---

### Task 4: Worker feed and object ops; workers stop dialing peers

**Files:**
- Modify: `src/orchestrator/sync.rs` (ops), `src/orchestrator/mod.rs` (delete `serve`/`prefetch_pass` peer loop), `src/orchestrator/control.rs` (route the new ops; remove `pairing-code`, `pair`, `peers`, `unpair`, `peer-info` from the worker)
- Modify: `src/main.rs` (remove `PairingCode`, `Pair`, `Peers`, `Unpair` commands — they return in Task 11 under `manage`)

**Interfaces:**
- Consumes: `Demand` (Task 3), `Engine::apply_remote`, `Store::events_after`, `PackStore::{put, get, contains, flush}`, `Digest::bytes`.
- Produces (worker ops, header = JSON payload, body = JSON unless stated):
  ```rust
  #[derive(Serialize, Deserialize)] pub struct FeedPage { pub cursor: u64, pub events: Vec<Event>, pub more: bool }
  // control lane:
  //   feed.outgoing {after}             → FeedPage (≤ 256 events after `after`)
  //   feed.apply    {}  body Vec<Event> → {"applied": usize}          (engine.apply_remote)
  //   meta.exchange {repo} body FeedRequest → FeedReply                (existing Service::feed_reply; REPOSITORY_MISMATCH)
  //   objects.missing {limit}           → Vec<(Digest, Kind)>: content refs of live entries (roots, then manifest chunks) not in packs; limit ≤ 64
  // bulk lane:
  //   objects.fetch {repo} body Vec<(Digest, Kind)> → Vec<Object>     (existing)
  //   objects.write {repo} body Vec<Object> → {"stored": usize}       verify Digest::bytes(bytes)==digest else INVALID_PEER_OBJECT; packs.put; packs.flush; demand.arrived
  pub struct Service { pub engine: Arc<Engine>, pub demand: Arc<Demand> }
  ```
  `DigestFetch`, `SessionSource`, `ObjectSource`, `exchange` and `FeedCursor` move out of the worker: keep `FeedCursor` and `FeedRequest/FeedReply` (the orchestrator uses them in Task 12); delete `DigestFetch`, `SessionSource`, `ObjectSource`, `exchange`, `prefetch_pass`. The engine's fetcher is `Demand` (`engine.working.content().set_fetcher(demand)` in `worker::run`).

- [ ] **Step 1: Write the failing tests** in `src/orchestrator/sync.rs` tests (in-process engines, as the existing tests do)

```rust
#[test]
fn feed_outgoing_pages_and_feed_apply_replicates() {
    // source: 300 creates; feed.outgoing{after:0} → 256 events, more=true, cursor=256; {after:256} → 44, more=false
    // target: feed.apply(body = all 300) → applied=300 and journals equal
}
#[test]
fn objects_write_stores_verified_objects_and_rejects_bad_digest() {
    // Object{digest: Digest::bytes(b"abc"), kind: Chunk, bytes: b"abc"} → stored=1, packs.contains → true
    // Object{digest: Digest::bytes(b"abc"), bytes: b"abd"} → error code INVALID_PEER_OBJECT, nothing stored
}
#[test]
fn objects_missing_lists_unfetched_content_refs_then_demand_wakes_waiters() {
    // target has an event with content ref whose chunk is absent → objects.missing{limit:64} contains that digest.
    // In a thread: Demand::fetch(&[(digest, Chunk)]) blocks; objects.write of the object → fetch returns Ok within 1 s.
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --lib orchestrator::sync`
Expected: FAIL to compile (`FeedPage`, `Service.demand` missing).

- [ ] **Step 3: Implement** the ops in `sync.rs` (`Service::handle_request` match) and wire `ControlService` (Task 3) to forward ops named `feed.*`, `meta.exchange`, `objects.*` to `Service`. Delete the peer loop and the worker pairing ops; delete the `PairingCode/Pair/Peers/Unpair` CLI variants.

- [ ] **Step 4: Run tests**

Run: `cargo test --lib orchestrator:: && cargo test --test worker_lifecycle`
Expected: pass; `cargo clippy --all-targets -- -D warnings` clean (dead code removed).

- [ ] **Step 5: Commit**

```bash
git add src/orchestrator src/main.rs
git commit -m "refactor: worker serves feed/object ops on loopback lanes and never dials peers"
```

---

### Task 5: Orchestrator configuration

**Files:**
- Create: `src/orchestrator/config.rs`
- Test: inline `#[cfg(test)]`

**Interfaces:**
- Produces:
  ```rust
  #[derive(Clone, Debug, Serialize, Deserialize)] #[serde(deny_unknown_fields)]
  pub struct Config { pub format_version: u32, pub data_directory: PathBuf, pub default_mount_directory: PathBuf,
                      #[serde(default = "default_limit")] pub maximum_running: usize,
                      #[serde(default)] pub network: Network, #[serde(default)] pub freshness: Freshness }
  #[derive(Clone, Debug, Serialize, Deserialize)] #[serde(default, deny_unknown_fields)]
  pub struct Network { pub listen: String, pub advertise: String, pub connections: usize, pub worker_lanes: usize, pub sync_interval_ms: u64 }
  #[derive(Clone, Debug, Serialize, Deserialize)] #[serde(default, deny_unknown_fields)]
  pub struct Freshness { pub open_wait_ms: u64 }
  impl Config {
      pub fn load(path: &Path) -> Result<Self>;          // CONFIG_NOT_FOUND | INVALID_CONFIG | CONFIG_VERSION_2_REQUIRED | INVALID_WORKER_LIMIT | INVALID_CONNECTION_COUNT | INVALID_WORKER_LANES | INVALID_SYNC_INTERVAL | INVALID_OPEN_WAIT; relative dirs resolved against the file's parent
      pub fn initialize(path: &Path) -> Result<Self>;    // create_new; CONFIG_EXISTS if present
      pub fn load_or_init(path: &Path) -> Result<Self>;  // auto-init for `orchestrator`
      pub fn listen(&self) -> Option<&str>;              // None when ""
      pub fn advertise(&self) -> Option<&str>;           // None when ""
      pub fn pool(&self) -> usize;                       // max(connections, 2)
  }
  ```
  Defaults: `data_directory = "data"`, `default_mount_directory = "mounts"`, `maximum_running = 8`, `listen = "127.0.0.1:43180"`, `advertise = ""`, `connections = 4`, `worker_lanes = 2`, `sync_interval_ms = 500`, `open_wait_ms = 250`.

- [ ] **Step 1: Write the failing tests**

```rust
#[test] fn initialize_writes_defaults_and_refuses_to_overwrite() { /* initialize twice → second Err contains CONFIG_EXISTS; loaded.maximum_running == 8; network.worker_lanes == 2; freshness.open_wait_ms == 250 */ }
#[test] fn unknown_field_is_invalid_config() { /* "format_version=2\ndata_directory='d'\ndefault_mount_directory='m'\nbogus=1" → INVALID_CONFIG */ }
#[test] fn format_3_is_refused() { /* format_version=3 → CONFIG_VERSION_2_REQUIRED */ }
#[test] fn relative_directories_resolve_against_the_file() { /* file in tempdir/sub/orchestrator.toml with data_directory="data" → tempdir/sub/data, independent of cwd */ }
#[test] fn ranges_are_enforced() { /* worker_lanes=9 → INVALID_WORKER_LANES; open_wait_ms=5001 → INVALID_OPEN_WAIT; connections=1 → pool()==2 */ }
#[test] fn empty_listen_and_advertise_mean_none() { /* listen="" → listen()==None; advertise="" → None */ }
```

- [ ] **Step 2: Run** `cargo test --lib orchestrator::config` → compile error.
- [ ] **Step 3: Implement** `config.rs` (model: `git show master:src/orchestrator/config.rs`).
- [ ] **Step 4: Run** `cargo test --lib orchestrator::config` → 6 passed.
- [ ] **Step 5: Commit** `git commit -m "feat: orchestrator.toml format 2 with advertise, worker_lanes and freshness"`

---

### Task 6: Orchestrator catalog

**Files:**
- Rewrite: `src/orchestrator/catalog.rs` (the worker-level pairing catalog became `settings.rs` in Task 3; this file now holds the orchestrator catalog)
- Test: inline

**Interfaces:**
- Consumes: `util::sqlite::CachedConnection`, `pairing_code::Verifier`, `clock::Offset`, `session::AcceptPolicy`, `core::id()`.
- Produces:
  ```rust
  pub const SCHEMA_VERSION: u32 = 1;
  pub const FOLDER_LIMIT: usize = 64;
  #[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
  pub struct FolderRecord { pub id: String, pub label: String, pub state: PathBuf, pub repo: String, pub mount: Option<PathBuf>,
                            pub desired_running: bool, pub paused: bool, pub created_ms: u64 }
  #[derive(Clone, Debug, Serialize, Deserialize)]
  pub struct Peer { pub endpoint: String, pub installation: String, credential: String, pub connections: Option<usize> }
  impl Peer { pub fn key(&self) -> Result<[u8; 32]>; }
  pub struct Catalog { /* db: Mutex<CachedConnection>, pub data_directory: PathBuf */ }
  impl Catalog {
      pub fn open(data_directory: &Path) -> Result<Arc<Self>>;   // creates dir; folders.sqlite 0600; tables; CATALOG_INCOMPATIBLE if schema.version != 1
      pub fn installation(&self) -> Result<String>;              // settings.installation, uuid created once
      pub fn get(&self, key: &str) -> Result<Option<String>>; pub fn set(&self, key: &str, value: &str) -> Result<()>;
      pub fn folders(&self) -> Result<Vec<FolderRecord>>;
      pub fn create_folder(&self, label: &str, repo: &str, mount: Option<PathBuf>) -> Result<FolderRecord>; // FOLDER_NAME_REQUIRED, FOLDER_LABEL_TAKEN, FOLDER_LIMIT; state = data_directory/<id>; desired_running=true
      pub fn resolve(&self, selector: &str) -> Result<FolderRecord>; // exact id | unique prefix ≥ 4 chars | exact label; FOLDER_NOT_FOUND | AMBIGUOUS_FOLDER
      pub fn set_desired(&self, id: &str, running: bool) -> Result<()>;
      pub fn set_paused(&self, id: &str, paused: bool) -> Result<()>;
      pub fn set_mount(&self, id: &str, mount: Option<PathBuf>) -> Result<()>;
      pub fn verifier(&self) -> Result<(Verifier, u64)>;         // created on open; generation starts at 1
      pub fn reset_code(&self) -> Result<String>;                // new verifier, generation+1, deletes all `paired` rows (revocation; the GOAWAY is Task 11)
      pub fn peers(&self) -> Result<Vec<Peer>>; pub fn peer(&self, endpoint: &str) -> Result<Peer>;
      pub fn save_peer(&self, endpoint: &str, installation: &str, key: [u8; 32], connections: Option<usize>) -> Result<()>; // writes `peers` and `paired`
      pub fn save_paired(&self, installation: &str, key: [u8; 32]) -> Result<()>;  // inbound enroll: `paired` only
      pub fn unpair(&self, installation: &str) -> Result<()>;
      pub fn paired_key(&self, installation: &str) -> Result<[u8; 32]>;          // PEER_REVOKED
      pub fn set_clock(&self, installation: &str, offset: Offset, measured_at_ms: u64) -> Result<()>;
      pub fn clock(&self, installation: &str) -> Result<Option<(Offset, u64)>>;
  }
  impl AcceptPolicy for Catalog { /* installation(), verifier(), admit → Ok, legacy_attempt → (), peer_key → paired_key */ }
  ```
  Tables: `schema(version)`, `settings(key PRIMARY KEY, value)`, `folders(id PRIMARY KEY, label UNIQUE, state, repo, mount, desired_running, paused, created_ms)`, `peers(endpoint PRIMARY KEY, installation, credential, connections)`, `paired(installation PRIMARY KEY, credential)`, `peer_meta(installation PRIMARY KEY, clock_offset_ms, clock_rtt_ms, clock_measured_at)`.

- [ ] **Step 1: Write the failing tests**

```rust
#[test] fn open_creates_schema_and_stable_installation() { /* open twice → same installation(); file mode 0600 on unix */ }
#[test] fn incompatible_schema_is_refused() { /* write schema.version=99 → open Err CATALOG_INCOMPATIBLE */ }
#[test] fn create_resolve_by_id_prefix_and_label() {
    // create "Project" and "Projects"; resolve("Project") → first; resolve(&id[..4]) → by prefix; resolve(&id[..3]) → FOLDER_NOT_FOUND;
    // two ids sharing a 4-char prefix (insert directly) → AMBIGUOUS_FOLDER; create("Project") again → FOLDER_LABEL_TAKEN
}
#[test] fn folder_limit_is_64() { /* 64 creates ok, 65th → FOLDER_LIMIT */ }
#[test] fn reset_code_revokes_paired_and_bumps_generation() { /* save_paired; verifier().1 == 1; reset_code; paired_key → PEER_REVOKED; verifier().1 == 2 */ }
#[test] fn clock_offsets_round_trip() { /* set_clock("x", Offset{595,10}, 1_700_000_000_000) → clock("x") == Some((Offset{595,10}, 1_700_000_000_000)); clock("y") == None */ }
```

- [ ] **Step 2: Run** `cargo test --lib orchestrator::catalog` → compile error.
- [ ] **Step 3: Implement** `catalog.rs`.
- [ ] **Step 4: Run** `cargo test --lib orchestrator::catalog` → 6 passed.
- [ ] **Step 5: Commit** `git commit -m "feat: orchestrator catalog (folders, pairing, clock offsets)"`

---

### Task 7: Worker process launcher and adoption

**Files:**
- Create: `src/orchestrator/process.rs`
- Modify: `src/desktop.rs:641-760` (replace local `terminate`/`ObservedProcess` with `process::{terminate, ObservedProcess}`), `src/orchestrator/worker.rs` (use `process::process_identity(std::process::id())` for `worker.json`)
- Test: `tests/worker_lifecycle.rs` (add)

**Interfaces:**
- Consumes: `worker::{Ready, WorkerRecord}`, `util::log::{redact_line, rotate_if_larger}`.
- Produces:
  ```rust
  pub const READY_TIMEOUT: Duration = Duration::from_secs(30);
  pub const WORKER_LOG_ROTATE_BYTES: u64 = 10 * 1024 * 1024; pub const WORKER_LOG_FILES: usize = 5;
  pub const TAIL_BYTES: usize = 4096;
  pub struct LaunchSpec { pub executable: PathBuf, pub state: PathBuf, pub mount: Option<PathBuf> } // log = state/worker.log
  #[derive(Debug)] pub enum ProcessEvent { Ready(Ready), Fatal { code: String, message: String }, Exited { code: Option<i32>, tail: String } }
  pub struct WorkerProcess { pub pid: u32, /* owned Child or adopted ObservedProcess */ }   // Drop never kills
  pub fn spawn(spec: &LaunchSpec, sink: mpsc::Sender<ProcessEvent>) -> Result<WorkerProcess>;
  //   `exe daemon --state S --supervised [--mount M]`; stdin null; stdout piped → reader thread: redact_line → worker.log, parse Ready/Fatal; stderr appended to worker.log;
  //   Windows CREATE_NO_WINDOW; Unix pre_exec setpgid(0,0); rotate worker.log at spawn if > 10 MiB; exit waiter sends Exited last.
  pub fn adopt(state: &Path, sink: mpsc::Sender<ProcessEvent>) -> Result<Option<(WorkerRecord, WorkerProcess)>>;
  //   None unless worker.json exists, pid alive and process_identity(pid) == (record.start_ms, record.image)
  pub fn process_identity(pid: u32) -> Result<(u64, PathBuf)>;   // Windows GetProcessTimes creation FILETIME→ms + QueryFullProcessImageNameW; Linux /proc/<pid>/stat field 22 (ticks→ms via btime+CLK_TCK) + /proc/<pid>/exe
  pub fn kill_verified(record: &WorkerRecord) -> Result<bool>;    // false when identity no longer matches
  pub fn terminate(child: &mut Child) -> Result<()>;              // moved from desktop.rs; STRANDED_PROCESS after 10 s
  pub struct ObservedProcess { pub pid: u32, /* handle / pidfd */ }
  impl ObservedProcess { pub fn open(pid: u32) -> Result<Self>; pub fn exited(&self) -> Result<bool>; pub fn wait(&self, timeout: Duration) -> Result<bool>; pub fn success(&self) -> Result<()>; }
  ```

- [ ] **Step 1: Write the failing tests** in `tests/worker_lifecycle.rs`

```rust
#[test] fn spawn_reports_ready_then_exited_and_logs_redacted_ready_line() {
    // spawn → first event Ready within 30 s; worker.log contains "\"ready\":true" and NOT ready.token; shutdown via control::request → Exited{code:Some(0)} within 30 s
}
#[test] fn adopt_matches_identity_and_rejects_a_stale_record() {
    // spawn, drop the WorkerProcess (no kill), adopt(state) → Some(record, proc) with record.pid == ready.pid;
    // rewrite worker.json with start_ms+1 → adopt → None; kill_verified(&stale) == false and the worker is still answering `hello`;
    // kill_verified(&real) == true → Exited observed ≤ 10 s; runtime files may remain (crash path) — the next spawn must still Ready.
}
#[test] fn process_identity_of_self_is_stable() { let a = process_identity(std::process::id()).unwrap(); let b = ...; assert_eq!(a, b); assert!(a.1.ends_with(exe name)); }
```

- [ ] **Step 2: Run** `cargo test --test worker_lifecycle` → compile error.
- [ ] **Step 3: Implement** `process.rs` (reference: `git show master:src/orchestrator/process.rs`, trimmed to the above). Point `desktop.rs` at the shared `terminate`/`ObservedProcess`.
- [ ] **Step 4: Run** `cargo test --test worker_lifecycle --test desktop` → pass on this OS.
- [ ] **Step 5: Commit** `git commit -m "feat: worker launcher, identity-checked adoption and verified kill"`

---

### Task 8: WorkerLink (orchestrator side of the loopback lanes)

**Files:**
- Create: `src/orchestrator/worker_client.rs`
- Test: inline tests that start `worker::run` in-process? No — `worker::run` owns the process (prints Ready, exits). Test against a spawned binary in `tests/worker_lifecycle.rs`.

**Interfaces:**
- Consumes: `session::dial_loopback`, `Conn`, `LaneSpec::owns`, `sync::{Object, FeedPage}`, `worker::Ready`.
- Produces:
  ```rust
  pub const CALL_DEADLINE: Duration = Duration::from_secs(10);
  pub const WATCH_WAIT_MS: u64 = 2000; pub const WATCH_DEADLINE: Duration = Duration::from_secs(4);
  pub struct WorkerLink { pub ready: Ready, control: Conn, bulk: Vec<Conn> }
  #[derive(Debug, Deserialize)] pub struct Watch { pub seq: u64, pub heartbeat: Value, pub demand: Vec<(Digest, Kind)> }
  impl WorkerLink {
      pub fn connect(ready: &Ready, bulk_lanes: usize) -> Result<Arc<Self>, CallError>;   // 1 + bulk_lanes dials, roles Control / Bulk
      pub fn call(&self, op: &str, payload: Value, body: Vec<u8>, deadline: Duration) -> Result<Reply, CallError>; // control lane; refuses op.starts_with("objects.") locally with CallError::Remote(OP_NOT_ALLOWED)
      pub fn call_json(&self, op: &str, payload: Value) -> Result<Value, CallError>;
      pub fn watch(&self, after_seq: u64) -> Result<Watch, CallError>;
      pub fn fetch(&self, wanted: &[(Digest, Kind)]) -> Result<Vec<Object>, CallError>;   // bulk lane chosen by first digest
      pub fn write(&self, objects: Vec<Object>) -> Result<usize, CallError>;                // bulk lane chosen by first digest
      pub fn lane_for(&self, digest: &Digest) -> usize;                                      // digest.0[0] as usize % bulk.len()
      pub fn health(&self) -> Vec<(LaneRole, ConnHealth)>;
      pub fn alive(&self) -> bool; pub fn close(&self);
  }
  ```

- [ ] **Step 1: Write the failing tests** (`tests/worker_lifecycle.rs`)

```rust
#[test] fn worker_link_uses_control_for_rpc_and_bulk_for_objects() {
    // spawn worker; link = WorkerLink::connect(&ready, 2); link.call_json("hello", {}) ok;
    // control bytes_before = health()[0].1.bytes_up; link.write(vec![Object 1 MiB]) → stored 1;
    // health()[0].1.bytes_up - bytes_before < 4096 (control carried no object bytes); one bulk lane's bytes_up ≥ 1 MiB;
    // link.call("objects.fetch", ...) → Err Remote OP_NOT_ALLOWED
}
#[test] fn watch_long_poll_returns_demand_promptly() {
    // link.watch(0).seq ≥ 1 within 2 s; in a thread: control::request(state,"cat",{path}) for a file whose chunk is missing
    //   (create the entry via feed.apply of an event referencing a digest not present); link.watch(seq) returns within 500 ms with demand containing that digest
}
```

- [ ] **Step 2: Run** → compile error. **Step 3: Implement** `worker_client.rs`. **Step 4: Run** `cargo test --test worker_lifecycle` → pass.
- [ ] **Step 5: Commit** `git commit -m "feat: WorkerLink with control and bulk loopback lanes"`

---

### Task 9: Folder controller

**Files:**
- Create: `src/orchestrator/folder.rs`
- Test: inline (state machine with `util::time::ManualClock` and a `Launcher` trait stub) + `tests/orchestrator.rs` later

**Interfaces:**
- Consumes: `process::{spawn, adopt, kill_verified, LaunchSpec, ProcessEvent, WorkerProcess}`, `WorkerLink`, `FolderRecord`, `util::time::Clock`.
- Produces:
  ```rust
  pub const HEARTBEAT_LOST: Duration = Duration::from_secs(8); pub const FAILED_WATCHES: u32 = 3;
  pub const STOP_WAIT: Duration = Duration::from_secs(13); pub const FORCE_STOP_WAIT: Duration = Duration::from_secs(15);
  pub const BACKOFF_CAP: Duration = Duration::from_secs(300); pub const BREAKER_FAILURES: usize = 5; pub const BREAKER_WINDOW: Duration = Duration::from_secs(600);
  pub fn backoff_delay(attempt: u32, jitter: f64 /* −0.2..=0.2 */) -> Duration;   // 1 s·2^(attempt−1) capped at 300 s, ×(1+jitter)
  #[derive(Clone, Debug, Serialize, Deserialize, PartialEq)] #[serde(tag = "phase")]
  pub enum Phase { Stopped, Starting { since_ms: u64, attempt: u32, adopting: bool },
                   Running { since_ms: u64, instance: String, pid: u32, address: String, adopted: bool },
                   Stopping { since_ms: u64, force: bool, deadline_ms: u64 },
                   Backoff { until_ms: u64, attempt: u32, error: String },
                   Failed { since_ms: u64, code: String, message: String } }
  #[derive(Clone, Debug, Serialize, Deserialize, PartialEq)] pub enum StopOutcome { Stopped, LeftRunning(String), Killed, NotRunning }
  pub trait Launcher: Send + Sync { fn spawn(&self, spec: &LaunchSpec, sink: Sender<ProcessEvent>) -> Result<WorkerProcess>;
                                    fn adopt(&self, state: &Path, sink: Sender<ProcessEvent>) -> Result<Option<(WorkerRecord, WorkerProcess)>>;
                                    fn connect(&self, ready: &Ready, lanes: usize) -> Result<Arc<WorkerLink>, CallError>; }
  pub struct RealLauncher; impl Launcher for RealLauncher;
  pub struct Controller { /* record, phase, launcher, clock, executable, lanes, process, link, events rx, heartbeat, failures: VecDeque<Instant>, watch thread handle */ }
  impl Controller {
      pub fn new(record: FolderRecord, executable: PathBuf, lanes: usize, launcher: Arc<dyn Launcher>, clock: Arc<dyn Clock>) -> Self;
      pub fn record(&self) -> &FolderRecord; pub fn set_record(&mut self, record: FolderRecord);
      pub fn phase(&self) -> &Phase; pub fn link(&self) -> Option<Arc<WorkerLink>>;
      pub fn start(&mut self);                 // Stopped|Failed|Backoff → Starting (adopt first, else spawn)
      pub fn retry(&mut self);                 // clears failures, start()
      pub fn stop(&mut self, force: bool) -> Result<StopOutcome>;  // shutdown{force} → wait exit ≤ STOP_WAIT/FORCE_STOP_WAIT; BUSY_VIEW → LeftRunning; timeout+force → kill_verified → Killed
      pub fn tick(&mut self);                  // drains ProcessEvent/watch results; liveness; Backoff expiry → start(); breaker → Failed{CRASH_LOOP}
      pub fn view(&self) -> Value;             // {record, phase, heartbeat, lanes:[{role, health}]}
  }
  ```
  Liveness in `tick`: a watch thread per Running folder calls `link.watch(after)` in a loop and sends `Result<Watch, CallError>`; no successful heartbeat for `HEARTBEAT_LOST` or 3 consecutive watch errors → `call_json("hello")` probe (2 s); failure → `kill_verified`, record failure, `Backoff`. Exited while `Running` → failure, `Backoff{attempt+1}`. 5 failures within 10 min → `Failed{CRASH_LOOP}`. All durations via `clock.instant()`.

- [ ] **Step 1: Write the failing tests** in `folder.rs` (stub `Launcher` that returns scripted events; `ManualClock`)

```rust
#[test] fn backoff_curve() { assert_eq!(backoff_delay(1, 0.0), 1 s); assert_eq!(backoff_delay(4, 0.0), 8 s); assert_eq!(backoff_delay(12, 0.0), 300 s); assert_eq!(backoff_delay(3, 0.2), 4.8 s); }
#[test] fn exit_while_running_backs_off_then_restarts_on_monotonic_time_only() {
    // stub: Ready then Exited{1}; tick → Backoff{attempt:1}; clock.advance_wall(1 h) without advancing instant → tick → still Backoff;
    // clock.advance(1 s) → tick → Starting{attempt:2}
}
#[test] fn five_failures_in_ten_minutes_trip_the_breaker() { /* script 5 Exited; → Failed{code:"CRASH_LOOP"}; retry() → Starting */ }
#[test] fn stop_without_force_passes_busy_view_through_as_left_running() { /* stub link answering BUSY_VIEW → StopOutcome::LeftRunning("BUSY_VIEW…"), phase Running */ }
#[test] fn lost_heartbeat_probes_then_kills() { /* no watch results for 8 s (ManualClock) and hello fails → kill_verified called once; Backoff */ }
```

- [ ] **Step 2: Run** `cargo test --lib orchestrator::folder` → compile error. **Step 3: Implement**. **Step 4: Run** → 5 passed.
- [ ] **Step 5: Commit** `git commit -m "feat: folder controller with adoption, liveness, backoff and breaker"`

---

### Task 10: Daemon, control endpoint, `manage`, CLI and signals

**Files:**
- Create: `src/orchestrator/hub.rs`, `src/orchestrator/endpoint.rs`, `src/orchestrator/manage.rs`, `src/orchestrator/signals.rs`
- Modify: `src/main.rs` (full CLI), `src/orchestrator/mod.rs`
- Create: `tests/support/daemon.rs`, `tests/orchestrator.rs`

**Interfaces:**
- Consumes: Tasks 5–9, `util::cancel::Cancel`, `ws::{accept_authorized, connect_authorized, call, receive, send}`, `clock::check`.
- Produces:
  ```rust
  // endpoint.rs
  pub const IDLE_CONNECTIONS: usize = 64;
  pub fn serve(daemon: Arc<Daemon>) -> Result<String>;   // binds 127.0.0.1:0; writes DATA/orchestrator.token (0600) then DATA/orchestrator.json
                                                          // {"control","installation","pid","version","listen","advertise","started_ms","data_directory"}; JSON RPC {"action","payload"} → {"ok":true,"result"} | {"ok":false,"error":{"code","message"}}
  pub struct ControlClient; impl ControlClient {
      pub fn connect(config: &Config) -> Result<Self>;    // ORCHESTRATOR_NOT_RUNNING (no orchestrator.json) | WS_CONNECT_FAILED | ACCESS_DENIED
      pub fn request(&mut self, action: &str, payload: Value) -> Result<Value>;      // mutation: never resent; CONNECTION_LOST
      pub fn request_read(&mut self, action: &str, payload: Value) -> Result<Value>; // retries once after reconnect
  }
  // hub.rs
  pub const SHUTDOWN_BOUND: Duration = Duration::from_secs(30);
  pub struct Daemon { pub config: Config, pub config_path: PathBuf, pub catalog: Arc<Catalog>, pub stopping: Cancel, /* folders: Mutex<BTreeMap<String, Controller>>, network: OnceLock<Arc<Network>> (Task 11), executable, started_ms, lock file */ }
  pub fn run(config_path: &Path) -> Result<()>;           // Config::load_or_init; clock::check; DATA/orchestrator.lock (ALREADY_RUNNING → exit 75); Catalog::open; endpoint::serve; signals::install; autostart desired_running folders (≤ maximum_running); loop { tick all; stopping.sleep(50 ms) }; shutdown(); remove orchestrator.json
  impl Daemon {
      pub fn execute(&self, action: &str, payload: &Value) -> Result<Value>;
      pub fn shutdown(&self) -> Value;  // {"stopped":true,"folders":[{"state","outcome","error"}]}: stop(false) all in parallel; at SHUTDOWN_BOUND−15 s any LeftRunning → stop(true); still alive → kill_verified → outcome "killed"; log FORCED_STOP per forced folder; exit 0
      pub fn snapshot(&self) -> Value;  // {"daemon":{installation,pid,version,started_ms,config,data_directory,listen,advertise,control,stopping},"folders":[view + record + conflicts + error],"peers":[]}
  }
  ```
  Actions (payload keys): `status {}` → `snapshot()`, `list {}` → `[FolderRecord]`, `inspect {state}` → `{"record", "phase", "heartbeat", "lanes", "sync", "worker": <worker status reply | null>}`, `create {label, repo?, mount?}` (→ `Catalog::create_folder`, `Engine::open(state, Some(repo))` to initialize then `shutdown()`, start, returns record), `start {state}`, `stop {state, wait: bool}`, `force-stop {state}`, `retry {state}`, `pause {state}`, `resume {state}`, `configure-mount {state, mount?}` (stopped only: `FOLDER_RUNNING`), `shutdown {}`, `folder {state, op, payload}` (passthrough to the worker control lane for `status | conflicts | resolve | preview-version | events | tree`; `FOLDER_NOT_RUNNING` otherwise). `maximum_running` exceeded → `WORKER_LIMIT`. Pairing actions arrive in Task 11.
  ```rust
  // manage.rs
  pub fn run(config_path: &Path, command: ManagementCommand, json: bool) -> Result<()>;  // `watch`: status every 1 s until Ctrl-C; text via format_status
  pub fn format_status(snapshot: &Value) -> String; // daemon line; one line per folder: "[id8] label  phase  mount|headless  conflicts N  error"; peers section
  // signals.rs
  pub fn install(cancel: Cancel) -> Result<()>;      // Unix: SIGINT/SIGTERM self-pipe + watcher thread; Windows: SetConsoleCtrlHandler (CTRL_C/BREAK → cancel; CLOSE/LOGOFF/SHUTDOWN → cancel and block the handler until `cancel.wait_done()`)
  ```
  CLI (`src/main.rs`), every line below is what `docs/CLI.md` will document in Task 13:
  ```
  tkfs [-f|--defaults-file CONFIG=orchestrator.toml] [--state DIR] [--runtime FILE] [--request-id ID] [--generation N] <COMMAND>
    init [--state DIR [--repo ID]]            # no --state: Config::initialize(-f); with --state: folder store init (prints {device,repo,state})
    orchestrator | start                      # hub::run(-f)
    gui                                       # unchanged (Windows)
    manage [--json] <status|watch|list|inspect STATE|create LABEL [--mount P] [--headless]|configure-mount STATE [--mount P]|start STATE|stop STATE [--no-wait]|force-stop STATE|retry STATE|pause STATE|resume STATE|shutdown>
    create LABEL [--mount P] [--headless]     # = manage create
    daemon --state DIR [--mount P] [--supervised]
    status|tree|events|conflicts [--history]|resolve ENTITY VERSION|checkpoint|branch NAME [--snapshot S]|checkout BRANCH|mkdir PATH|rename PATH TARGET [--replace]|delete PATH|import PATH SOURCE|cat PATH|flush|gc|stop|rpc OP [PAYLOAD]
  ```
  Folder commands resolve the state directory from `--state DIR` or `--runtime FILE` (its parent); neither → `FOLDER_REQUIRED: use --state DIR or --runtime STATE/runtime.json`. `--mount` on `create` is stored and passed to the worker at spawn (single mount, today's `attach` at startup); `--headless` is the default when `--mount` is absent.

- [ ] **Step 1: Write the test harness** `tests/support/daemon.rs`

```rust
pub struct TestDaemon { pub home: TempDir, pub config: PathBuf, child: Option<Child>, pub log: PathBuf }
impl TestDaemon {
    pub fn start() -> Result<Self>;                        // writes orchestrator.toml with listen="127.0.0.1:0"; spawns `tkfs -f config start` (stdout/stderr → log); waits ≤ 30 s for DATA/orchestrator.json
    pub fn start_with(network_toml: &str) -> Result<Self>; // extra [network] lines
    pub fn manage(&self, args: &[&str]) -> Result<Value>;  // `tkfs -f config manage --json ARGS…` bounded 60 s; Err(stderr) on non-zero
    pub fn wait_phase(&self, selector: &str, phase: &str, timeout: Duration) -> Result<Value>;  // polls `manage inspect`
    pub fn listen_endpoint(&self) -> Result<String>;      // "ws://" + DATA/orchestrator.json["listen"] (Task 11)
    pub fn wait_lanes(&self, endpoint: &str, count: usize, state: &str, timeout: Duration) -> Result<Value>; // polls `manage peers` (Task 11)
    pub fn state_dir(&self, selector: &str) -> Result<PathBuf>;  // from `manage inspect` ["record"]["state"]
    pub fn signal_term(&self);                             // Unix: libc::kill(SIGTERM); Windows: `manage shutdown`
    pub fn wait_exit(&mut self, timeout: Duration) -> Result<i32>;  // kill + Err("STRANDED_PROCESS") on timeout
    pub fn shutdown(mut self) -> Result<()>;               // manage shutdown + wait_exit(60 s) == 0
}
impl Drop for TestDaemon { /* kill if still running; eprintln STRANDED */ }
```

- [ ] **Step 2: Write the failing tests** `tests/orchestrator.rs`

```rust
#[test] fn start_auto_inits_config_creates_headless_folders_and_shuts_down_cleanly() {
    let d = TestDaemon::start()?;                       // orchestrator.toml did not exist: created with format_version=2
    let created = d.manage(&["create", "Project", "--headless"])?;  assert!(created["mount"].is_null());
    d.wait_phase("Project", "Running", 30 s)?;
    let list = d.manage(&["list"])?; assert_eq!(list.as_array().unwrap().len(), 1);
    let status = d.manage(&["status"])?; assert_eq!(status["folders"][0]["phase"]["phase"], "Running");
    let inspected = d.manage(&["inspect", "Project"])?; assert!(inspected["worker"]["entries"].is_number()); // inspect = view + "worker": worker `status` reply when Running
    d.shutdown()?;                                      // exit 0; DATA/orchestrator.json removed; the worker exited (STATE/runtime.json gone ≤ 10 s)
}
#[test] fn stop_start_retry_pause_resume_and_selectors() {
    // stop Project → phase Stopped, desired_running=false survives a daemon restart (second TestDaemon on same home: folder stays Stopped);
    // start → Running; pause → paused=true in inspect; resume → false; stop with a 3-char prefix → stderr contains FOLDER_NOT_FOUND
}
#[test] fn second_orchestrator_on_same_data_directory_exits_75_and_changes_nothing() {
    // read orchestrator.json/.token bytes; run `tkfs -f config start` → exit code 75, stderr contains ALREADY_RUNNING; files unchanged; first daemon still answers `status`
}
#[test] fn sigterm_runs_the_shutdown_sequence_and_forces_a_hung_worker_within_the_bound() {
    // create Project; `tkfs --state DATA/<id> rpc inject-fault '{"name":"shutdown.hang"}'`; d.signal_term(); wait_exit(45 s) == 0;
    // log contains "FORCED_STOP"; STATE/worker.json absent or stale; starting a new TestDaemon on the same home brings Project to Running again
}
#[test] fn crash_loop_trips_breaker_and_retry_clears_it() {
    // `stop Project`; copy STATE/metadata.sqlite aside and overwrite it with 1 KiB of zeros; `start Project`;
    // within 10 s: inspect phase == "Backoff" (worker exits 73 STORE); within 60 s: phase == "Failed" and code == "CRASH_LOOP"
    //   (failures at ~0, 1, 3, 7, 15 s: backoff 1+2+4+8 s plus spawn time);
    // restore the file; `retry Project` → Running within 30 s.
}
```

- [ ] **Step 3: Run** `cargo test --test orchestrator` → compile errors / `manage` unknown.
- [ ] **Step 4: Implement** `hub.rs`, `endpoint.rs`, `manage.rs`, `signals.rs`, the CLI. Keep `gui::run(config, state)` call compiling (unchanged behaviour until the GUI plan). `manage` output in text mode: `format_status`; errors `CODE: message` on stderr, exit 1.
- [ ] **Step 5: Run** `cargo test --test orchestrator --test worker_lifecycle --test desktop` → pass; `cargo clippy --all-targets -- -D warnings`.
- [ ] **Step 6: Commit** `git commit -m "feat: orchestrator daemon, control endpoint, manage CLI and signal shutdown"`

---

### Task 11: Peer network — lanes, inbound, pairing

**Files:**
- Create: `src/orchestrator/network.rs`
- Modify: `src/orchestrator/hub.rs` (construct `Network`, add actions), `src/main.rs` (`manage reset-pairing-code | pair ENDPOINT [--connections N] [--code CODE] | unpair INSTALLATION | peers | remote-list ENDPOINT`)
- Test: `tests/peer_sync.rs`

**Interfaces:**
- Consumes: `session::{dial_expecting, dial, accept, client_hello, CodeAuth, auth::{PeerAuth, new_secret, enrollment_request, enrollment_secret, peer_key}}`, `Catalog` (Task 6), `clock`.
- Produces:
  ```rust
  pub const DIAL_TIMEOUT: Duration = Duration::from_secs(3); pub const LANE_BACKOFF_MAX: Duration = Duration::from_secs(60);
  pub const READY_RESET: Duration = Duration::from_secs(5); pub const INCOMPATIBLE_HOLD: Duration = Duration::from_secs(600); pub const UNAUTHORIZED_HOLD: Duration = Duration::from_secs(60);
  pub const PRE_AUTH_CAP: usize = 32; pub const SESSION_CAP: usize = 128;
  #[derive(Clone, Debug, Serialize)] #[serde(tag = "state")]
  pub enum LaneState { Idle, Dialing { since_ms: u64 }, Ready { since_ms: u64, health: ConnHealth }, Backoff { until_ms: u64, attempt: u32, error: String }, Incompatible { until_ms: u64 }, Unauthorized { until_ms: u64 } }
  pub trait FolderDirectory: Send + Sync {
      fn link_by_repo(&self, repo: &str) -> Option<Arc<WorkerLink>>;      // Running, not paused
      fn published(&self) -> Vec<Value>;                                   // [{"repo","label"}] of all folders (paused included)
  }
  pub struct Network { /* catalog, config, installation, folders: Arc<dyn FolderDirectory>, peers: Mutex<BTreeMap<endpoint, PeerLanes>>, inbound sessions, stopping: Cancel */ }
  pub fn start(config: &Config, catalog: Arc<Catalog>, folders: Arc<dyn FolderDirectory>, stopping: Cancel) -> Result<Arc<Network>>;
  //   listener on config.listen() (skip when None); per paired peer a lane thread per lane index: 0 = Control, 1..pool = Bulk (LaneSpec{index, count}); state machine per spec; clock::check() before each dial (CLOCK_INVALID → Backoff)
  impl Network {
      pub fn pair(&self, endpoint: &str, code: &str, connections: Option<usize>) -> Result<String>;  // probe dial with CodeAuth; peer.enroll {endpoint: advertise-or-listen-derived} body enrollment_request(secret); save_peer; returns installation
      pub fn unpair(&self, installation: &str) -> Result<()>;      // best-effort peer.revoke on control lane; catalog.unpair; close lanes
      pub fn reset_pairing_code(&self) -> Result<String>;          // catalog.reset_code; goaway("PAIRING_CODE_CHANGED") on every inbound session
      pub fn remote_list(&self, endpoint: &str) -> Result<Vec<Value>>;  // catalog.list on the control lane (paired) or a probe lane
      pub fn peers(&self) -> Value;                                // [{"endpoint","installation","lanes":[LaneState…],"clock":{offset_ms,rtt_ms,measured_at}|null}]
      pub fn control_lane(&self, endpoint: &str) -> Option<Conn>; pub fn bulk_lane(&self, endpoint: &str, digest: &Digest) -> Option<Conn>;
      pub fn shutdown(&self, budget: Duration);
  }
  pub struct PeerService { /* network, peer: PeerInfo (installation, role) */ } impl Handler for PeerService;
  //   catalog.list → published(); peer.enroll → catalog.save_paired; peer.revoke → catalog.unpair; ping → {alive:true}; clock.probe {t1} → {t1,t2,t3} (Task 12 stores);
  //   meta.exchange {repo} → link_by_repo(repo).call("meta.exchange"…) (REPO_NOT_PUBLISHED when none); objects.fetch {repo} → link.fetch(body)
  //   role check: objects.* only when peer.role == Bulk, meta.exchange only on Control → OP_NOT_ALLOWED
  ```
  Hub actions added: `reset-pairing-code {}` → `{code}`; `pair {endpoint, code, connections?}` → `{installation}`; `unpair {installation}`; `peers {}`; `remote-list {endpoint}`. `snapshot().peers = network.peers()`.

- [ ] **Step 1: Write the failing tests** `tests/peer_sync.rs`

```rust
#[test] fn pair_opens_control_and_bulk_lanes_and_reset_code_revokes() {
    let a = TestDaemon::start()?; let b = TestDaemon::start()?;
    let code = b.manage(&["reset-pairing-code"])?["code"].as_str().unwrap().to_string(); assert_eq!(code.len(), 64);
    let endpoint_b = b.listen_endpoint()?;              // "ws://127.0.0.1:PORT" from DATA/orchestrator.json["listen"]
    let paired = a.manage(&["pair", &endpoint_b, "--code", &code])?;  assert!(paired["installation"].is_string());
    a.wait_lanes(&endpoint_b, 2 /* pool */, "Ready", 30 s)?;         // lanes[0] role control, lanes[1] bulk
    let peers_b = b.manage(&["peers"])?; assert_eq!(peers_b.as_array().unwrap().len(), 1 /* b sees a as paired inbound */);
    b.manage(&["reset-pairing-code"])?;                 // a's lanes fall to Unauthorized within 10 s and never carry traffic again
    a.wait_lanes(&endpoint_b, 2, "Unauthorized", 15 s)?;
}
#[test] fn wrong_code_is_refused_and_nothing_is_saved() { /* pair with 64 zeros → stderr contains ACCESS_DENIED or PAIRING_FAILED; `peers` empty */ }
#[test] fn remote_list_shows_published_folders() { /* b creates "Shared" headless; a pair; a remote-list → [{"repo","label":"Shared"}] */ }
```
Unit test in `network.rs`: `peer_service_refuses_objects_on_control_and_meta_on_bulk` using a stub `FolderDirectory` and `Conn::from_established` pairs as `tests/session_core.rs` does.

- [ ] **Step 2: Run** → compile error. **Step 3: Implement** `network.rs` and the hub/CLI additions. **Step 4: Run** `cargo test --test peer_sync --lib orchestrator::network` → pass.
- [ ] **Step 5: Commit** `git commit -m "feat: orchestrator peer lanes, inbound sessions and pairing"`

---

### Task 12: Sync scheduler, object pump, `receive`, clock probes

**Files:**
- Modify: `src/orchestrator/network.rs` (scheduler, pump, clock), `src/orchestrator/hub.rs` (`receive` action; `sync` in folder view), `src/main.rs` (`manage receive ENDPOINT REPO LABEL [--mount P] [--headless]`)
- Test: `tests/peer_sync.rs` (add)

**Interfaces:**
- Consumes: `sync::{FeedCursor, FeedRequest, FeedReply, FeedPage, Object}`, `WorkerLink::{call_json, fetch, write, watch}`, `Watch.demand`, `clock::{Sample, smooth, WARN_OFFSET_MS}`, `catalog::{set_clock, clock}`.
- Produces:
  ```rust
  pub const MAX_EXCHANGES: usize = 4; pub const OBJECT_BATCH: usize = 32; pub const CLOCK_PROBE_EVERY: u64 = 60; // × sync_interval_ms
  pub struct SyncJob { pub peer: String, pub repo: String, pub cursor: FeedCursor, pub last_exchange_ms: u64, pub error: Option<String> }
  // scheduler thread: every sync_interval_ms, for each (Ready control lane, published repo not paused), ≤ MAX_EXCHANGES concurrent, 1 per (peer,repo):
  //   page = link.call_json("feed.outgoing", {"after": cursor.sent}) → FeedPage
  //   reply = lane.call("meta.exchange", {"repo"}, FeedRequest{after: cursor.received, events: page.events}, Priority::Control, 10 s) → FeedReply
  //   link.call("feed.apply", {}, reply.events) ; cursor.sent = page.cursor; cursor.received = reply.cursor; repeat at once while page.more || reply.more
  // object pump thread per Running folder: wanted = watch demand (from Controller) ∪ link.call_json("objects.missing", {"limit": OBJECT_BATCH});
  //   group by bulk_lane(peer, digest) across peers with Ready bulk lanes; lane.call("objects.fetch", {"repo"}, wanted, Priority::Data, 10 s) → link.write(objects)
  //   nothing wanted → sleep sync_interval_ms; a watch with demand wakes the pump at once (Condvar from Controller)
  // clock probe on a control lane: at Ready and every CLOCK_PROBE_EVERY scheduler rounds: t1 = now_ms; reply {t1,t2,t3}; t4 = now_ms;
  //   offset = smooth(previous, Sample.offset().offset_ms); catalog.set_clock(installation, Offset{offset, rtt}, t4); |offset| > WARN_OFFSET_MS → log warn CLOCK_OFFSET_LARGE
  // Daemon action `receive {endpoint, repo, label, mount?}`: remote_list(endpoint) must contain repo (REPO_NOT_PUBLISHED); create_folder(label, repo, mount); Engine::open(state, Some(repo)) then shutdown; start
  // folder view gains "sync": {endpoint: {sent, received, last_exchange_ms, error}}
  ```

- [ ] **Step 1: Write the failing tests** `tests/peer_sync.rs`

```rust
#[test] fn receive_replicates_metadata_and_objects_across_two_orchestrators() {
    // b: create "Shared" headless; b: `tkfs --state DATA/<id> import notes.txt <2.5 MiB file>` (two chunks + manifest);
    // a: pair b; a: receive endpoint_b <repo> "Shared" --headless; a.wait_phase("Shared","Running");
    // within 60 s: a `cat notes.txt` equals the source bytes; a inspect Shared ["sync"][endpoint_b]["received"] ≥ 2
    // and ConnHealth of a's control lane to b has bytes_down < 1 MiB (objects went over bulk)
}
#[test] fn paused_folder_stops_exchanging_and_resume_catches_up() { /* pause Shared on a; import on b; 5 s later a entries unchanged; resume → file appears ≤ 30 s */ }
#[test] fn clock_offset_is_measured_and_stored() { /* after lanes Ready, a `peers` shows clock.offset_ms within ±2000 and rtt_ms < 1000, measured_at ≥ FLOOR_MS */ }
#[test] fn peer_with_invalid_clock_is_refused() { /* b started with env TKFS_FAKE_NOW_MS=1_000_000_000_000 → b exits 78 CLOCK_INVALID before listening (covers daemon start); for the handshake side: a with fake clock refuses to dial → lanes Backoff{error contains CLOCK_INVALID} */ }
```

- [ ] **Step 2: Run** → fails (`receive` unknown). **Step 3: Implement**. **Step 4: Run** `cargo test --test peer_sync` → pass (allow 2–3 min; every wait bounded).
- [ ] **Step 5: Commit** `git commit -m "feat: metadata scheduler, object pump, receive and peer clock offsets"`

---

### Task 13: Documentation, `docs_cli`, compose and systemd

**Files:**
- Create: `tests/docs_cli.rs` (from `git show master:tests/docs_cli.rs`, `MIN_DOCUMENTED` lowered to 25, `VALUE_OPTIONS` = `-f --defaults-file --state --runtime --request-id --generation`)
- Modify: `docs/CLI.md`, `docs/CONFIGURATION.md`, `docs/LINUX.md`, `README.md` (error-class table: add `CLOCK_INVALID`, `CATALOG_INCOMPATIBLE`, `FORCED_STOP`, `AMBIGUOUS_FOLDER`, `REPO_NOT_PUBLISHED`, `WORKER_NOT_RUNNING`, `FOLDER_REQUIRED`, `OP_NOT_ALLOWED`), `examples/docker-compose.yml`, `deploy/tkfs-orchestrator.service.example`, `docs/revamp/WAVE_6_REPLACEMENT.md` (one paragraph: orchestration restored, pointer to the spec)

**Interfaces:** none new. The compose file (spec §7.11):

```yaml
services:
  tkfs-daemon:
    image: ubuntu:24.04
    container_name: tkfs_daemon
    restart: unless-stopped
    network_mode: host
    working_dir: /app
    stop_grace_period: 60s          # the orchestrator handles SIGTERM itself (bounded 30 s shutdown)
    # --- MOUNTING (off by default: folders run headless) ---
    # devices: ["/dev/fuse:/dev/fuse"]; cap_add: [SYS_ADMIN]; security_opt: ["apparmor:unconfined"]; add fuse3 to PACKAGES
    volumes: [".:/app"]
    command: >
      sh -c "PACKAGES='ca-certificates libssl3t64';
             dpkg -s $PACKAGES >/dev/null 2>&1 || { apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends $PACKAGES; } &&
             exec ./tkfs -f orchestrator.toml start"
# Folders: docker compose exec tkfs-daemon ./tkfs manage create NAME --headless
# Status:  docker compose exec tkfs-daemon ./tkfs manage status
```

- [ ] **Step 1: Restore and adapt `tests/docs_cli.rs`**; run `cargo test --test docs_cli` → FAIL (docs describe commands that no longer exist / new ones undocumented).
- [ ] **Step 2: Rewrite `docs/CLI.md`** for the Task 10–12 command set (every command and `manage` subcommand appears in a fenced `sh` block), `docs/CONFIGURATION.md` for Task 5's schema and the files in `DATA/` and `STATE/`, `docs/LINUX.md` (quick start, signals, containers, systemd), `README.md` error table, compose, service example.
- [ ] **Step 3: Run** `cargo test --test docs_cli` → pass. Spot-check: `docker compose -f examples/docker-compose.yml config` parses.
- [ ] **Step 4: Full gate** `cargo fmt --check && cargo clippy --all-targets -- -D warnings && cargo test` on Windows; on Linux the same with `CARGO_TARGET_DIR=$HOME/tkfs-target/linux`.
- [ ] **Step 5: Commit** `git commit -m "docs: restored CLI reference, configuration, Linux deployment and docs_cli gate"`

---

## Self-review notes

- Spec coverage for this plan's scope: §7.1 processes (T3, T7, T9, T10), §7.2 lanes (T2, T3, T8, T11, T12), §7.3 control plane minus mount/branch/remote-conflict/diagnostics/revalidate commands (T10, T11, T12; the rest belong to the mounts, branches and GUI plans), §7.4 config (T5) and no-legacy (T6 `CATALOG_INCOMPATIBLE`), §7.7 clocks (T1, T3, T10, T12), §7.9 local conflicts via `folder` passthrough (T10), §7.10 pairing (T6, T11), §7.11 Linux/Docker (T13), §7.12 codes (T13).
- Deferred to later plans, on purpose: `attach`, `detach`, `mount-now`, `configure-mount` for running folders, `.tkfs-runtime.json` and `-C` (mounts plan); `branch …`, `checkout` removal (branches plan); `--peer` on `conflicts/resolve/preview` (remote conflicts plan); `diagnostics`, `revalidate`, `watch` rendering polish and the GUI as control client (GUI/docs plan).
- Names used across tasks: `worker::Ready`, `worker::WorkerRecord`, `demand::{Seq, Demand}`, `sync::{FeedPage, FeedRequest, FeedReply, FeedCursor, Object, Service}`, `process::{LaunchSpec, ProcessEvent, WorkerProcess, spawn, adopt, kill_verified, process_identity, ObservedProcess, terminate}`, `worker_client::{WorkerLink, Watch}`, `folder::{Controller, Phase, StopOutcome, Launcher, RealLauncher, backoff_delay}`, `hub::{Daemon, run}`, `endpoint::{serve, ControlClient}`, `manage::{run, format_status}`, `signals::install`, `network::{Network, start, LaneState, FolderDirectory, PeerService, SyncJob}`, `catalog::{Catalog, FolderRecord, Peer}`, `config::Config`, `clock::{check, check_ms, Sample, Offset, smooth, FLOOR_MS, WARN_OFFSET_MS}`.
