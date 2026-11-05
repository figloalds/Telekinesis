# Two-Way Content Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** File contents move both ways over one pairing, on the connection the dialer opened, with active and passive sync profiles.

**Architecture:** The dialer still originates every request. It gains a push pass (`objects.wanted`, `objects.store`), a one-way `Notify` frame from the listener wakes it, and a declared profile decides how eagerly it acts. Capabilities travel in the handshake's existing `features` lists, so `PROTOCOL_VERSION` stays 4.

**Tech Stack:** Rust 2024, std threads and channels, tungstenite, the existing `Conn` session layer, rusqlite.

**Spec:** `docs/superpowers/specs/2026-11-04-two-way-content-design.md`

## Global Constraints

- Follow `AGENTS.md`: headless only. Never run a filesystem mount or `tests/winfsp_binding.rs`. W6-P0 stays open.
- Run only the suites named in each task, and at the end those in Task 8. All test waits are bounded; a stranded process is a failure.
- Only dialers originate requests (revamp decision 4). `Conn` stays client-originated.
- `PROTOCOL_VERSION` and `SUPPORTED_VERSIONS` do not change. Pairing codes stay 64 hex.
- Conflict handling does not change: both versions are kept, and no side has authority.
- An uncertain request is never replayed (invariant 16).
- Keep `require_paired`, `REPO_NOT_PUBLISHED` and `op_allowed` behaviour.
- `git status` shows an untracked `android/` holding the spike's ignored build output. Never add it: stage files by path, never with `git add -A` or `git add .`.
- Commit messages end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Do not push.
- Run `cargo fmt` before each commit.

## Review Focus

1. **Zero-byte file created on the dialer:** it reaches the listener and reads back as empty, with no spin or error. (Task 4)
2. **Listener folder paused while the dialer has work:** `REPO_NOT_PUBLISHED` causes a backoff, not a hot loop or log flood, and work resumes after resume. (Task 4)
3. **Listener cannot write (disk full):** the store fails, nothing is reported durable, and the dialer retries later without losing anything. (Task 3)
4. **Pairing revoked mid-transfer:** the push stops cleanly and the listed-digest set is cleared, so a re-pair starts fresh. (Task 3)
5. **Dialer edits while its lanes are down:** nothing is lost, and the change arrives once lanes return, without waiting for the backup poll. (Task 6)

---

## File Structure

| File | Responsibility |
|---|---|
| `src/model/protocol.rs` | Feature names, `FrameKind::Notify`, `frame::Notify`, `max_body("objects.store")` |
| `src/session/handshake.rs`, `conn.rs`, `io.rs`, `frame.rs` | Carry features into `PeerInfo`; send and receive `Notify` |
| `src/orchestrator/config.rs` | `sync_profile`, `backup_poll_ms` |
| `src/core.rs` | `Store::conflict_references` |
| `src/orchestrator/sync.rs` | Worker scan covers conflict heads; worker op `objects.wanted` |
| `src/orchestrator/worker_client.rs` | `WorkerLink::wanted`; `Watch.feed` |
| `src/orchestrator/network.rs` | Listener ops, admission, sending notify, lane nudging, status |
| `src/orchestrator/push.rs` (new) | The dialer's push state and steps |
| `src/orchestrator/sync_driver.rs` | Pump order, due logic, notify wake-ups, policy, bursts |
| `src/orchestrator/folder.rs`, `hub.rs`, `manage.rs` | Folder signals, `sync-now` action and command |
| `tests/peer_sync.rs`, `tests/support/daemon.rs` | Acceptance tests and harness additions |

---

### Task 1: Capabilities and settings

**Files:**
- Modify: `src/model/protocol.rs`, `src/session/handshake.rs:145-165,266-272,930-937,989-995,1053-1060`, `src/session/conn.rs:194-202`, `src/orchestrator/config.rs:30-50,170`, `src/orchestrator/network.rs` (`run_lane`, `start`), `docs/CONFIGURATION.md`
- Test: `src/orchestrator/config.rs` tests, `tests/session_handshake.rs`, `tests/support/daemon.rs`

**Interfaces:**
- Produces:
  - `protocol::features::{CONTENT_PUSH = "content-push", NOTIFY = "notify", PROFILE_PASSIVE = "profile-passive"}`
  - `PeerInfo.features: Vec<String>` with `#[serde(default)]`: on a dialer's `Conn`, the server's features; on a listener's `Conn`, the dialer's.
  - `PeerInfo::has_feature(&self, name: &str) -> bool`
  - `config::Network.sync_profile: SyncProfile`, `config::Network.backup_poll_ms: u64`
  - `pub enum SyncProfile { Active, Passive }` with `serde(rename_all = "snake_case")`, default `Active`
  - `TestDaemon::start_env(network_toml: &str, env: &[(&str, &str)]) -> Result<Self>`

- [ ] **Step 1: Write the failing tests**

```rust
// config.rs tests
#[test] fn sync_profile_defaults_to_active_and_accepts_passive() // both parse; "other" fails
#[test] fn backup_poll_must_be_within_bounds() // 999 and 3_600_001 fail with INVALID_BACKUP_POLL; 1000 and 3_600_000 pass; default 30_000

// tests/session_handshake.rs
#[test] fn features_are_exchanged_in_both_directions() {
    // dialer hello.features = [NOTIFY, PROFILE_PASSIVE]; server advertises [CONTENT_PUSH, NOTIFY]
    assert!(client_conn.peer().has_feature(features::CONTENT_PUSH));
    assert!(server_conn.peer().has_feature(features::PROFILE_PASSIVE));
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test --lib orchestrator::config` and `cargo test --test session_handshake features_are_exchanged`
Expected: FAIL, the names are not defined.

- [ ] **Step 3: Implement**

`accept_v3` advertises `[CONTENT_PUSH, NOTIFY]` in `ServerHello.features`. `run_lane` sets `hello.features` from a new `Target.features`: always `NOTIFY`, plus `PROFILE_PASSIVE` when passive. A passive dialer sends `PROFILE_PASSIVE` and omits `NOTIFY`, so the listener never sends it a notify.

Add fault point `session.features.none`: when `fault::hit` fires, both sides send an empty list. Task 8 uses it to simulate an older peer.

`start_env` passes the environment variables to the daemon process. Validation errors are `INVALID_SYNC_PROFILE` (from parsing) and `INVALID_BACKUP_POLL: network.backup_poll_ms must be 1000..=3600000`. Document both settings in `docs/CONFIGURATION.md`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: the commands from Step 2, plus `cargo test --test session_core` and `cargo test --test session_handshake`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/model/protocol.rs src/session src/orchestrator/config.rs src/orchestrator/network.rs tests/session_handshake.rs tests/support/daemon.rs docs/CONFIGURATION.md
git commit -m "feat: negotiate peer features and add sync profile settings"
```

---

### Task 2: Conflict versions in the scan

**Files:**
- Modify: `src/core.rs` (near `conflicts`, line 1288), `src/orchestrator/sync.rs:236-244`
- Test: `src/orchestrator/sync.rs` tests

**Interfaces:**
- Produces: `Store::conflict_references(&self) -> Result<Vec<(String, FileContentRef)>>`: one item per current head that is not the visible version, for entities with more than one head, skipping heads with no content. The key is `"{entity}/{version}"`.

- [ ] **Step 1: Write the failing tests**

```rust
#[test] fn missing_lists_the_bytes_of_every_conflict_head() {
    // Two heads on one entity, neither version's content present.
    // Page through objects.missing to the end: both root digests are listed.
}
#[test] fn a_conflict_head_scan_resumes_from_its_cursor() {
    // limit 1: paging with the returned cursors lists each digest exactly once
    // and ends with cursor None.
}
#[test] fn a_conflict_manifest_lists_its_chunks_once_present() {}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test --lib orchestrator::sync`
Expected: FAIL, the losing head's digest is not listed.

- [ ] **Step 3: Implement**

In `missing`, merge `conflict_references` with the visible references into one vector sorted by key, owning the key strings. `ScanAt` needs no new variant, because `"{entity}/{version}"` sorts next to its entity and `decode` keeps everything after the index as the key. Take the conflict references under the same store lock as the entries.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test --lib orchestrator::sync`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/core.rs src/orchestrator/sync.rs
git commit -m "feat: the object scan covers every conflict version"
```

---

### Task 3: Listener push operations

**Files:**
- Modify: `src/model/protocol.rs:95-112`, `src/model/errors.rs`, `src/orchestrator/sync.rs:370-410`, `src/orchestrator/worker_client.rs`, `src/orchestrator/network.rs:1014-1140`, `README.md` (error table)
- Test: `src/orchestrator/network.rs` tests (near line 1247), `src/model/protocol.rs` tests

**Interfaces:**
- Consumes: `PeerInfo.has_feature` (Task 1), the extended scan (Task 2).
- Produces:
  - `WantedPage { demand: Vec<(Digest, Kind)>, missing: Vec<(Digest, Kind)>, cursor: Option<String> }` in `sync.rs`
  - `WorkerLink::wanted(&self, limit: usize, exclude: &[Digest], after: Option<&str>) -> Result<WantedPage, CallError>`
  - Peer op `objects.wanted`, header `{repo, limit, exclude, after}`, replying with a `WantedPage` body. `limit: 0` returns only `demand`.
  - Peer op `objects.store`, header `{repo}`, body `Vec<Object>` as JSON, replying with `StoreReply { stored: Vec<Digest>, refused: Vec<Refused> }`, where `Refused { digest: Digest, code: String }`.
  - `Network::forget_listed(&self, installation: &str)`

- [ ] **Step 1: Write the failing tests**

```rust
#[test] fn wanted_lists_demand_first_and_a_page_of_missing() {}
#[test] fn store_accepts_a_listed_object_and_names_it_stored() {
    // After the reply, the listener's packs contain the digest.
}
#[test] fn store_refuses_an_unlisted_digest_with_object_not_wanted() {
    // refused == [{digest, "OBJECT_NOT_WANTED"}]; stored empty; session still usable.
}
#[test] fn store_refuses_a_mismatched_digest_with_invalid_peer_object() {}
#[test] fn a_mixed_batch_stores_the_good_objects_and_refuses_the_rest() {}
#[test] fn a_digest_listed_on_one_bulk_lane_is_accepted_on_another() {}
#[test] fn the_listed_set_is_bounded_and_an_evicted_digest_can_be_listed_again() {
    // MAX_LISTED + 1 digests: the oldest is refused until listed again.
}
#[test] fn unpaired_and_unpublished_store_are_refused() {
    // PEER_REVOKED and REPO_NOT_PUBLISHED, as objects.fetch.
}
#[test] fn store_reports_nothing_durable_when_the_worker_write_fails() {
    // Review Focus 3. Fault point objects.store.write fails: the op returns an error,
    // stored lists nothing, and a later retry of the same digest succeeds.
}
#[test] fn revoking_a_peer_clears_its_listed_digests() {
    // Review Focus 4. After peer.revoke or a pairing-code reset, a previously listed
    // digest is refused; after re-listing it is accepted.
}
```

```rust
// protocol.rs tests
assert_eq!(max_body("objects.store"), 32 * 1024 * 1024);
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test --lib orchestrator::network`
Expected: FAIL.

- [ ] **Step 3: Implement**

- Worker op `objects.wanted` in `Service::handle_reply`: `demand` is `self.demand.pending()` truncated to `BATCH`, `missing` comes from `self.missing`. Add it to the repo check at line 370.
- `Network` gains `listed: Mutex<BTreeMap<(String, String), ListedDigests>>`, keyed by (installation, repo). `ListedDigests` holds a `VecDeque` and a `BTreeSet`, FIFO-bounded by `const MAX_LISTED: usize = 4096`, in `network.rs`.
- `objects.wanted` records `demand` and `missing` in the set before replying. Demand therefore always enters the set this way, which satisfies the spec's "or currently in demand": a dialer only learns of demand from this reply.
- `objects.store`: for each object, a digest mismatch becomes `INVALID_PEER_OBJECT`, an unlisted digest becomes `OBJECT_NOT_WANTED`, and the rest go to `link.write`, which fsyncs before replying. Remove stored digests from the set. A batch over `OBJECT_BATCH` or `MAX_BATCH_BYTES` is refused whole with the worker's `OBJECT_BATCH_TOO_LARGE`, because that is a dialer bug, not a per-object refusal.
- Call `forget_listed` from `peer.revoke`, unpair and pairing-code reset.
- Register `OBJECT_NOT_WANTED` and `INVALID_PEER_OBJECT` in `model::errors` with the same class as `REPO_NOT_PUBLISHED`, and add them to the README table.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test --lib orchestrator::network` and `cargo test --lib model::`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/model src/orchestrator/sync.rs src/orchestrator/worker_client.rs src/orchestrator/network.rs README.md
git commit -m "feat: listeners serve objects.wanted and objects.store with admission"
```

---

### Task 4: The dialer's push pass

**Files:**
- Create: `src/orchestrator/push.rs`
- Modify: `src/orchestrator/mod.rs`, `src/orchestrator/sync_driver.rs:100-135,592-655`
- Test: `tests/peer_sync.rs`, `src/orchestrator/sync_driver.rs` tests

**Interfaces:**
- Consumes: `objects.wanted`, `objects.store`, `StoreReply`, `WantedPage` (Task 3); `PeerInfo.has_feature` (Task 1).
- Produces:
  - `Lanes::any_bulk_lane(&self, endpoint: &str) -> Option<Conn>`
  - `push::PushState::default()`: per (folder id, endpoint), the scan cursor, batch size, digests this side lacks with their backoff, and counters.
  - `push::run(lanes: &dyn Lanes, state: &mut PushState, endpoints: &[String], folder: &SyncFolder, mode: PushMode, interrupt: Option<(&Seq, u64)>, cancel: &Cancel) -> PushOutcome`
  - `pub enum PushMode { Demand, Background }`
  - `pub struct PushOutcome { pub pushed: usize, pub remaining: usize, pub interrupted: bool, pub error: Option<String> }`

- [ ] **Step 1: Write the failing tests**

```rust
// tests/peer_sync.rs. A dials B, one pairing.
#[test] fn a_file_created_on_the_dialer_becomes_readable_on_the_listener() {
    // Acceptance 1. import on A; wait_file(&state_b, path, content, REPLICATED).
    // The reverse direction still works in the same test.
}
#[test] fn edits_converge_and_conflict_versions_survive_their_writer_leaving() {
    // Acceptance 2. Edits on both sides converge. Pause both, edit one file on each,
    // resume: conflicts lists two heads. Wait until background transfer settles.
    // Shut down A. On B, preview-version succeeds for both versions; resolve to A's
    // version, and cat returns A's bytes.
}
#[test] fn a_listener_read_fails_cleanly_while_the_dialer_is_away_then_succeeds() {
    // Acceptance 6. Metadata arrived, bytes did not (A stopped before the push).
    // cat on B fails with OBJECT_MISSING within a bound; after A returns, cat succeeds.
}
#[test] fn an_interrupted_push_completes_after_restart() {
    // Acceptance 7. 64 MiB file. Kill A mid-push, restart, wait for identical bytes on
    // B. Repeat by killing B. Use start_listening with a fixed port for B.
}
#[test] fn a_zero_byte_file_reaches_the_listener() {} // Review Focus 1
#[test] fn a_paused_listener_causes_backoff_and_resumes() {
    // Review Focus 2. Pause on B, import on A, wait 5 s: A's log holds at most one
    // SYNC_PUSH_FAILED for that error. Resume on B: the file arrives.
}

// sync_driver.rs tests, with the Lanes stub
#[test] fn the_pump_runs_demand_pull_then_demand_push_then_background() {}
#[test] fn a_push_round_stops_before_its_next_call_when_demand_arrives() {}
#[test] fn a_listener_without_content_push_is_never_pushed_to() {}
#[test] fn a_digest_this_side_lacks_is_backed_off_and_not_an_error() {}
#[test] fn a_timed_out_store_halves_the_batch_and_answers_grow_it() {}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test --test peer_sync a_file_created_on_the_dialer`
Expected: FAIL, the wait for the file on B times out.

- [ ] **Step 3: Implement `push::run`**

For each endpoint whose bulk lane has `CONTENT_PUSH`:
1. Call `objects.wanted`, with `limit: 0` in `Demand` mode and `limit: OBJECT_BATCH` with the stored cursor in `Background` mode. In `Demand` mode, push `demand`. In `Background` mode, push `demand` then `missing`.
2. Read with `folder.link.fetch_reply`. Whatever is absent goes into the lacks map, using `UNAVAILABLE_FIRST` doubling to `UNAVAILABLE_MAX`, and into `exclude`.
3. Group by owning lane with `Lanes::bulk_lane`, respect `truncated_at`, and call `objects.store` with `Priority::Data` and `fetch_deadline`.

Timeouts halve the batch and answers grow it back, as in `PumpState`. A failure rests the endpoint and logs `SYNC_PUSH_FAILED` once per distinct error. Digests in `refused` are dropped from the round, since they will be listed again if still wanted.

- [ ] **Step 4: Change `pump_round` to the four-step order**

Order: pass 1 pull demand, `push::run(Demand)`, background pull, `push::run(Background)`. Keep `PushState` in the pump thread beside `PumpState`. Check the interrupt the same way pass 2 does. At this stage the push steps run every round; Task 6 adds due logic.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test --test peer_sync` and `cargo test --lib orchestrator::sync_driver`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/orchestrator/push.rs src/orchestrator/mod.rs src/orchestrator/sync_driver.rs tests/peer_sync.rs
git commit -m "feat: dialers push the objects a listener wants"
```

---

### Task 5: The Notify frame

**Files:**
- Modify: `src/model/protocol.rs:170-215,387-392`, `src/session/frame.rs:426`, `src/session/conn.rs`, `src/session/io.rs:738-752`
- Test: `tests/session_core.rs`, `src/session/frame.rs` tests

**Interfaces:**
- Produces:
  - `FrameKind::Notify = 12`, with `body_forbidden` true
  - `frame::Notify { repo: String, reason: NotifyReason }`
  - `pub enum NotifyReason { Metadata, Demand }` with `serde(rename_all = "snake_case")`
  - `Conn::notify(&self, notify: &frame::Notify)`: server side only. It never blocks, and drops the frame if the control queue is over its bound.
  - `Conn::on_notify(&self, handler: Arc<dyn Fn(&frame::Notify) + Send + Sync>)`: client side. The handler runs on the I/O thread and must not block.

- [ ] **Step 1: Write the failing tests**

```rust
#[test] fn a_server_notify_reaches_the_client_handler() {}
#[test] fn notify_is_refused_as_a_violation_on_the_server_side() {}
#[test] fn a_notify_with_no_handler_set_is_ignored() {}
#[test] fn a_notify_on_a_closed_conn_is_a_no_op() {}
#[test] fn notify_is_dropped_not_blocking_when_the_control_queue_is_full() {
    // Bounded time, and the connection stays usable.
}
// frame.rs: the unknown-kind test uses 13, and 12 decodes as Notify.
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test --test session_core notify`
Expected: FAIL.

- [ ] **Step 3: Implement**

Add `FrameKind::Notify` to `ALL`, whose size becomes 12. In `io.rs` add `FrameKind::Notify if side == Side::Client => self.on_notify(frame)`. The existing fallback arm makes it a violation on the server side. Store the handler in the connection's shared state in a `OnceLock`. A frame that arrives before the handler is set is ignored, which is safe because it is only a hint.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test --test session_core` and `cargo test --lib session::`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/model/protocol.rs src/session tests/session_core.rs
git commit -m "feat: add the Notify session frame"
```

---

### Task 6: Active sync with notify and a backup poll

**Files:**
- Modify: `src/orchestrator/control.rs:149-163`, `src/orchestrator/worker_client.rs:30-34`, `src/orchestrator/folder.rs:182-225,312-317`, `src/orchestrator/hub.rs:534-556,1121-1145`, `src/orchestrator/network.rs`, `src/orchestrator/sync_driver.rs:407-470,592-655`
- Test: `tests/peer_sync.rs`, `src/orchestrator/sync_driver.rs` tests

**Interfaces:**
- Consumes: `Conn::notify`, `Conn::on_notify` (Task 5); `push::run` (Task 4); `backup_poll_ms` (Task 1).
- Produces:
  - `Watch.feed: u64`: the worker's latest feed position, the value `feed.outgoing` pages after.
  - `pub trait FolderSignals: Send + Sync { fn changed(&self, repo: &str); fn demanding(&self, repo: &str); }`
  - `Controller::with_signals(self, signals: Arc<dyn FolderSignals>) -> Self`
  - `Network::notify(&self, repo: &str, reason: NotifyReason)`
  - `pub trait NotifySink: Send + Sync { fn notified(&self, endpoint: &str, notify: &frame::Notify); }`
  - `Network::set_notify_sink(&self, sink: Arc<dyn NotifySink>)`
  - `ReadyPeer.notify: bool`
  - `SyncPolicy { profile: SyncProfile, interval: Duration, backup_poll: Duration }`
  - `sync_driver::start` takes `policy: SyncPolicy` in place of `interval`. `Driver::new` stays for existing stub tests, with an active policy whose `backup_poll` equals `interval`.
  - `Driver::local_change(&self, repo: &str)`

- [ ] **Step 1: Write the failing tests**

```rust
// tests/peer_sync.rs. Both daemons use sync_interval_ms = 60000, backup_poll_ms = 60000.
#[test] fn a_change_on_either_side_arrives_by_notify_well_inside_the_poll() {
    // Acceptance 3. An import on A, then one on B, each readable on the other within 10 s.
}
#[test] fn the_backup_poll_delivers_a_change_when_notify_is_suppressed() {
    // Acceptance 4, part 1. Fault point peer.notify.drop on B, backup_poll_ms = 2000,
    // sync_interval_ms = 60000. A change on B reaches A within a bound.
}
#[test] fn a_repeated_demand_notify_saves_a_blocked_reader() {
    // Acceptance 4, part 2. Fault point peer.notify.drop with TKFS_FAULT_COUNT=1.
    // cat on B of a file only A holds succeeds within Demand::FETCH_WAIT.
}
#[test] fn a_waiting_reader_goes_ahead_of_background_transfer() {
    // Acceptance 5. A 64 MiB background transfer is running in each direction in turn.
    // A reader blocked on a small file gets it within 5 s.
}
#[test] fn an_edit_made_while_lanes_are_down_arrives_when_they_return() {
    // Review Focus 5. backup_poll_ms = 60000. Stop B. Import on A. Restart B on the
    // same port. The file is readable on B within 20 s.
}

// sync_driver.rs tests
#[test] fn a_notify_session_waits_for_the_backup_poll_when_nothing_changed() {}
#[test] fn a_session_without_notify_keeps_the_sync_interval() {}
#[test] fn a_notify_marks_only_its_peer_and_repo_due() {}
#[test] fn a_new_control_session_is_always_due() {}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test --test peer_sync a_change_on_either_side`
Expected: FAIL, the change does not arrive within 10 s.

- [ ] **Step 3: Implement the signals**

- `control::watch` adds `"feed"` to its reply.
- `WatchThread` calls `signals.changed(repo)` when `feed` advances, and `signals.demanding(repo)` when demand becomes non-empty and every `DEMAND_NOTIFY_EVERY: Duration = 2 s` while it stays non-empty.
- `Directory` implements `FolderSignals` by calling `Network::notify` and, for `changed`, `Driver::local_change`.

- [ ] **Step 4: Implement sending and receiving**

- `Network::notify` sends on every inbound control session that has `NOTIFY` and is paired. It coalesces per (session, repo, reason) to at most one frame per `NOTIFY_MIN_GAP: Duration = 100 ms`. Fault point `peer.notify.drop` skips the send.
- `run_lane`, on a Ready control lane, calls `conn.on_notify` with a closure that reads the sink from a `OnceLock` on `Network`, so the driver can register after lanes start.
- `Driver` implements `NotifySink`: `Metadata` marks the (endpoint, repo) due and bumps a new `poke: Arc<Seq>`. `Demand` marks it push-demand-due and also bumps `wake`.

- [ ] **Step 5: Implement the due logic**

`schedule` waits on `poke` with a timeout of `policy.interval`. `round` and `probe_clocks` advance only on timeouts, so clock probes keep their rate. `dispatch` starts an exchange for a pair only when one of these holds:
- it is marked due;
- its control session is new;
- its poll has elapsed: `backup_poll` if `peer.notify`, otherwise `interval`.

`local_change` marks every peer's pair for that repo due. In `pump_round`, `push::run(Demand)` runs only for push-demand-due pairs, and `push::run(Background)` runs only when a local change, an exchange that moved events, or a poll marked the pair due, or the last page was non-empty. So an idle pair makes no `objects.wanted` calls.

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cargo test --test peer_sync` and `cargo test --lib orchestrator::`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/orchestrator tests/peer_sync.rs
git commit -m "feat: active peers sync on notify with a backup poll"
```

---

### Task 7: Passive profile and `sync-now`

**Files:**
- Modify: `src/orchestrator/sync_driver.rs`, `src/orchestrator/network.rs:130-140,917-1012`, `src/orchestrator/hub.rs:118-132,721-800`, `src/orchestrator/manage.rs:78-112,190-225`, `docs/CLI.md`
- Test: `tests/peer_sync.rs`, `src/orchestrator/network.rs` tests

**Interfaces:**
- Consumes: `SyncPolicy` (Task 6), `PushOutcome` (Task 4).
- Produces:
  - `Network::nudge_lanes(&self)`: clears transient backoff so lanes redial at once. `Unauthorized` and `Incompatible` holds are not affected.
  - `Driver::sync_now(&self, folders: &[String], budget: Duration) -> Value`, returning `{"folders": [{"id", "exchanged", "pushed", "remaining", "error"}]}`
  - Daemon action `sync-now`, payload `{state?}`, added to `MUTATIONS`
  - `tkfs manage sync-now [FOLDER]`
  - `const SYNC_NOW_BOUND: Duration`: `endpoint::REPLY_TIMEOUT` minus 5 s
  - `const PASSIVE_LANE_BACKOFF_MAX: Duration = 300 s`

- [ ] **Step 1: Write the failing tests**

```rust
#[test] fn a_passive_dialer_makes_no_sync_requests_while_idle() {
    // Acceptance 9. A passive. Let lanes reach Ready and one sync-now finish. Over 15 s:
    // B's peers row for A has an unchanged requests_served, and notifies_sent is 0.
    // Heartbeats do not count.
}
#[test] fn sync_now_moves_metadata_and_pushes_but_does_not_download_unread_files() {
    // Acceptance 10. Passive A receives from B, whose folder holds big.bin (8 MiB).
    // Import new.txt on A; sync-now on A; reply has error null, remaining 0.
    // new.txt reads on B. A's pack bytes stay well under big.bin's size. cat big.bin on
    // A then returns its bytes.
}
#[test] fn a_passive_dialer_learns_of_a_remote_change_only_when_asked() {
    // Import on B; wait 5 s: tree on A lacks it. sync-now on A: tree has it.
}
#[test] fn sync_now_redials_a_lane_waiting_in_backoff() {
    // Stop B until A's lanes are in Backoff with until_ms over 20 s away. Restart B.
    // sync-now on A: lanes Ready and the reply has no error within SYNC_NOW_BOUND.
}
#[test] fn sync_now_reports_an_unreachable_peer_within_its_bound() {
    // B stopped: the reply arrives within SYNC_NOW_BOUND with error set and remaining
    // not hidden.
}
// network.rs: lane_backoff is capped at PASSIVE_LANE_BACKOFF_MAX for a passive target.
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test --test peer_sync a_passive_dialer`
Expected: FAIL.

- [ ] **Step 3: Implement the passive gates**

When `policy.profile` is `Passive`:
- `dispatch` runs only for pairs in an open burst, and `probe_clocks` only during a burst;
- `pump_round` skips background pull;
- it runs push only during a burst.

Pass 1 still runs, so a local reader can fetch while the process runs. Lanes stay open while the process runs, as the spec says. Add `requests_served` and `notifies_sent` per installation to `Network` and the `peers` row, since the test reads them.

- [ ] **Step 4: Implement `sync_now`**

Call `nudge_lanes`, wait for a Ready control lane up to 10 s, then open a burst: `burst_until = now + budget`, with every (peer, selected folder) due and woken. Wait until no job is running, nothing is due, and `push::run` reported `remaining: 0` for each selected folder, or until `budget` ends. Report what was done. `remaining` is an estimate from the last wanted page, so the docs say so. An unreachable peer or a budget that ran out sets `error`, using a code from the failure, or `SYNC_NOW_TIMEOUT`. Register that code in `model::errors`. `run_lane`'s sleep becomes a wait on a per-lane `Seq` so `nudge_lanes` can end it and reset `attempt`.

- [ ] **Step 5: Add the action and command, with docs**

Add the `manage` subcommand `SyncNow { folder: Option<String> }`, the action in `Daemon::execute` resolving the selector as other actions do (none means every folder), and a `docs/CLI.md` entry with a code block, so `tests/docs_cli.rs` passes.

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cargo test --test peer_sync`, `cargo test --test docs_cli`, `cargo test --lib orchestrator::`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/model src/orchestrator tests/peer_sync.rs docs/CLI.md
git commit -m "feat: passive profile and the sync-now burst"
```

---

### Task 8: Status, older peers, docs and final verification

**Files:**
- Modify: `src/orchestrator/network.rs:604-665`, `src/orchestrator/sync_driver.rs:380-400`, `docs/ARCHITECTURE.md`, `docs/CONFIGURATION.md`, `docs/CLI.md`, `docs/LINUX.md`, `README.md`, `docs/revamp/REVAMP_PLAN.md:1269`
- Test: `tests/peer_sync.rs`

**Interfaces:**
- Consumes: `session.features.none` (Task 1), everything above.
- Produces: `peers` rows gain `"features"`, `"profile"` and `"two_way_content"`. The per-repo sync view gains `"pushed"` and `"last_push_error"`.

- [ ] **Step 1: Write the failing tests**

```rust
#[test] fn an_older_listener_still_syncs_pull_only_and_peers_says_so() {
    // Acceptance 11. B with TKFS_FAULT_POINT=session.features.none. Pair and receive:
    // A reads B's files. A's peers row has two_way_content false. A's new file does
    // not reach B, and A's log has no SYNC_PUSH_FAILED.
}
#[test] fn an_older_dialer_keeps_the_short_poll() {
    // A with the fault: a change on B reaches A inside a few sync intervals, without
    // any notify (B's notifies_sent stays 0).
}
#[test] fn peers_and_sync_views_report_features_profile_and_pushes() {
    // Passive A: B's row for A shows profile "passive". After a push, A's folder sync
    // view has pushed > 0 and last_push_error null.
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test --test peer_sync an_older_listener`
Expected: FAIL, the fields do not exist.

- [ ] **Step 3: Implement the status fields**

Read features from the control lane's `Conn::peer()` on outbound rows and from inbound sessions on inbound rows.

- [ ] **Step 4: Update the docs**

- `ARCHITECTURE.md`: replace the claim that content moves one way, and describe push, notify and profiles.
- `CONFIGURATION.md`: describe how `sync_interval_ms` and `backup_poll_ms` relate.
- `CLI.md`: note that pair-then-receive is now two-way, and describe the new `peers` fields.
- `REVAMP_PLAN.md` decision 4: add a note that dialers still originate every request and now push content, pointing at the spec.

- [ ] **Step 5: Run the full verification**

```bash
cargo fmt --check
cargo check --all-targets
cargo test --lib -- orchestrator:: session:: ws:: model::
cargo test --test peer_sync --test orchestrator --test session_core --test session_handshake --test docs_cli --test worker_lifecycle
```

Expected: every command succeeds. Do not run `tests/winfsp_binding.rs` or anything that mounts.

- [ ] **Step 6: Commit**

```bash
git add src/orchestrator tests/peer_sync.rs docs README.md
git commit -m "feat: report two-way status and document it"
```
