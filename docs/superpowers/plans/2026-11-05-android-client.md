# Android Client Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A Telekinesis Android app that exposes the PC's folders through a `DocumentsProvider` for reading and writing, works offline-first, syncs in bursts, and frees space only with the PC's confirmation.

**Architecture:** One app process runs the normal orchestrator and folder workers on threads (`orchestrator::embedded`, passive profile). Kotlin holds the Android parts (provider, WorkManager, Compose screens). A small Rust crate, `android/native`, holds the JNI surface and the keep, free and waiting-to-send bookkeeping. Core additions stay narrow and work on the desktop too, so most of them are tested there.

**Tech Stack:** Rust 2024, `jni` 0.21, rusqlite, rustls on Android; Kotlin, Compose with Material 3, WorkManager, Google code scanner; AGP 8.12.3, Gradle 8.13, NDK 27.0.12077973; Python for build and end-to-end scripts.

**Spec:** `docs/superpowers/specs/2026-11-05-android-client-design.md`, with `docs/superpowers/specs/2026-11-05-android-client-mockup.html` for the screens and their copy. Read both.

## Global Constraints

- Branch `android-client`, already cut from `two-way-content`. Do not push. Do not rewrite history.
- End every commit message with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Follow `AGENTS.md`: no host filesystem mounts, W6-P0 stays open, all test waits bounded, and a stranded process is a failure. Never pass `--mount` in tests. End-to-end tests use a headless orchestrator on the PC.
- Desktop behaviour does not change except for the additions named here. Active dialers behave exactly as today. Android-only dependencies stay behind `cfg(target_os = "android")`.
- `targetSdk` stays below 37 (35). `minSdk` stays 26. The arm64 library must be 16 KiB page aligned.
- Android builds use rustls with bundled roots. Debug signing only. Install with `adb install`.
- Pairing codes stay 64 hex. Add no second kind of code.
- New error codes go in `model::errors` and the README table. New CLI commands and flags go in `docs/CLI.md` (`tests/docs_cli.rs` checks it).
- Tests in this repo: `cargo test --lib`, `cargo test --test <name>`. App JVM tests: `android\gradlew.bat -p android :app:testDebugUnitTest`. Crate tests: `cargo test --manifest-path android/native/Cargo.toml`. App build: `python scripts/build_android.py --abi x86_64`. End to end: `python scripts/android_e2e.py --scenario <name>`.
- The owner's phone comes later. Acceptance 19 and 20 are not part of this plan's completion.

## Review Focus

1. A file or folder name with spaces, `%`, `#`, `:` or non-ASCII letters must list, open, rename and delete normally. The document id splits at the first colon only. Pinned in Task 8 and Task 13.
2. Opening a file the phone has not downloaded for writing, with the PC away: `wt` must work with no network, and a partial `rw` write must fail cleanly and leave the file unchanged. Pinned in Task 13.
3. Reinstalling over an existing install (`adb install -r`) must keep folders, pairings, marks and waiting files, and start with the config already on disk. Pinned in Task 20.
4. Storage full during a write or a receive: the writer gets an error, nothing corrupts, and it recovers once space returns. Pinned in Task 19.
5. The PC revoked this phone (`reset-pairing-code` or unpair from the PC): the app says to pair again instead of showing a generic failure forever, and re-pairing keeps the folder's data. Pinned in Task 9.

---

## Part 1: Foundation in the core

### Task 1: Android project and embedded core on this branch

**Files:**
- Restore from `spike/android`: `android/.gitignore`, `android/build.gradle`, `android/settings.gradle`, `android/gradle.properties`, `android/gradle/`, `android/gradlew`, `android/gradlew.bat`, `android/native/`, `android/app/build.gradle`, `android/app/src/main/AndroidManifest.xml`, `android/app/src/main/res/drawable/ic_folder.xml`, `android/app/src/main/java/dev/tkfs/spike/`
- Modify: `Cargo.toml`, `Cargo.lock`, `src/main.rs`, `src/invariants.rs`, `src/orchestrator/endpoint.rs`, `src/orchestrator/hub.rs`, `src/orchestrator/process.rs`, `src/orchestrator/signals.rs`, `src/orchestrator/worker.rs`, `src/session/handshake.rs`, `src/session/io.rs`, `src/ws.rs`, `scripts/build_android.py`, local `.git/info/exclude`

**Interfaces:**
- Produces: `hub::run_embedded(config_path: &Path, launcher: Arc<dyn Launcher>, started: impl FnOnce(Arc<Daemon>)) -> Result<()>`, `process::EmbeddedWorker`, `WorkerProcess::embedded`, `worker::Options::on_ready: Option<mpsc::Sender<Ready>>`.

- [ ] **Step 1: Remove the `android/` line from `.git/info/exclude`** and make `android/.gitignore` cover `build/`, `.gradle/`, `.idea/`, `local.properties` and `app/src/main/jniLibs/`. Check with `git check-ignore android/app/build android/native/Cargo.toml`: only the first is ignored. Existing local build output stays ignored.

- [ ] **Step 2: Restore the spike's Android files** with `git checkout spike/android -- <paths above>`. Do not restore `android/build-native.ps1` or `android/testclient`. Remove `include ':testclient'` from `settings.gradle` and set `rootProject.name = "Telekinesis"`.

- [ ] **Step 3: Port the spike's core changes** from `git diff 3560f992 spike/android -- Cargo.toml src/` onto this branch, by hand where `hub.rs` moved. Leave out `sync_driver::BACKGROUND_PREFETCH`, because `sync_profile = "passive"` replaces it. Replace every "Android spike" comment with a description of what the code is.

- [ ] **Step 4: Update `scripts/build_android.py`.** Drop the "check out spike/android" hint. Add `-C link-arg=-Wl,-z,max-page-size=16384` through `CARGO_TARGET_<TRIPLE>_RUSTFLAGS`. Add `check_page_alignment(library: Path)`, which parses the ELF64 program headers and raises `RuntimeError` if any `PT_LOAD` has `p_align` below 16384, called on each built library.

- [ ] **Step 5: Refresh `android/native/Cargo.lock`** so `--locked` passes.

- [ ] **Step 6: Verify.**
Run: `cargo test --lib`. Expected: all pass.
Run: `python scripts/build_android.py --abi x86_64`. Expected: `Created ...tkfs-debug.apk`.
The spike Kotlin still builds at this point; Task 8 replaces it.

- [ ] **Step 7: Commit** as `feat: bring the Android project and embedded core onto the branch`.

### Task 2: `orchestrator::embedded` host

**Files:**
- Create: `src/orchestrator/embedded.rs`, `tests/embedded.rs`
- Modify: `src/orchestrator/mod.rs`, `src/orchestrator/hub.rs`, `android/native/src/lib.rs`

**Interfaces:**
- Consumes: Task 1's `run_embedded`, `Launcher`, `EmbeddedWorker`.
- Produces:
```rust
pub struct Host;
impl Host {
    pub const START_WAIT: Duration = Duration::from_secs(15);
    pub fn start(config: PathBuf) -> Arc<Host>;                // returns at once
    pub fn daemon(&self) -> Result<Arc<Daemon>>;               // ORCHESTRATOR_FAILED, ORCHESTRATOR_NOT_READY
    pub fn engine(&self, folder: &str) -> Result<Arc<Engine>>; // FOLDER_NOT_FOUND, WORKER_NOT_READY
    pub fn stop(&self) -> Result<()>;                          // bounded; STRANDED_WORKER on timeout
}
```

- [ ] **Step 1: Write the failing tests** in `tests/embedded.rs`, using `support::daemon::TestDaemon` for the PC and a `Host` on a temp config with `listen = ""` and `sync_profile = "passive"`.
```rust
#[test]
fn embedded_host_pairs_receives_and_reads_through_the_engine() -> Result<()> {
    // PC folder holds a.txt. execute("pair"), execute("receive"), execute("sync-now"),
    // then engine.open_file("/a.txt", false, false, false) and read: identical bytes.
    // host.stop() returns within its bound.
}
#[test]
fn an_embedded_orchestrator_serves_no_control_endpoint() -> Result<()> {
    // No discovery file in the data directory, and execute("status")["daemon"]["control"] is null.
}
```

- [ ] **Step 2: Run them.** Run: `cargo test --test embedded`. Expected: FAIL, `Host` is not defined.

- [ ] **Step 3: Implement.** Move the spike's `ThreadLauncher` and `ThreadWorker` out of `android/native/src/lib.rs` into `embedded.rs`, with the engine map and worker map as fields of `Host`, not statics. Change `run_embedded` so it does not call `endpoint::serve` or write `Discovery`. `android/native/src/lib.rs` keeps one `OnceLock<Arc<Host>>`.

- [ ] **Step 4: Run them.** Run: `cargo test --test embedded`. Expected: PASS. Run: `cargo test --lib`. Expected: PASS.

- [ ] **Step 5: Commit** as `feat: an embedded host runs the orchestrator and workers on threads`.

### Task 3: A stalled session thread does not call its peer dead

**Files:**
- Create: `tests/session_freeze_fault.rs`
- Modify: `src/session/io.rs`

**Interfaces:**
- Produces: fault point `session.io.turn`, and a `last_turn: Instant` field on the I/O state.

- [ ] **Step 1: Write the failing test.** This is its own test binary because faults are per process. A loopback pair uses `ConnOptions { heartbeat: 100 ms, dead_after: 600 ms, .. }`. Configure `session.io.turn` with action `hang`, 1000 ms, count 2, so each side's I/O thread stalls longer than `dead_after`.
```rust
#[test]
fn a_stalled_io_thread_does_not_declare_its_peer_dead() -> Result<()> {
    // After the stall: conn.closed() is None on both ends, and a ping call succeeds.
}
```

- [ ] **Step 2: Run it.** Run: `cargo test --test session_freeze_fault`. Expected: FAIL with `TRANSPORT_DEAD`.

- [ ] **Step 3: Implement.** At the start of each I/O turn, in the function holding the `dead_after` checks near `io.rs:1618`: if `now - last_turn >= options.dead_after / 2`, this thread did not run, so set `last_rx` and `last_write_progress` to `now`. Then set `last_turn = now`. Wake-ups already happen by `heartbeat` (5 s against 20 s), so a live thread never reaches that gap. Hit the fault point at the start of the turn.

- [ ] **Step 4: Run.** Run: `cargo test --test session_freeze_fault --test session_core`. Expected: PASS, including the existing test that a silent peer is declared dead.

- [ ] **Step 5: Commit** as `fix: a stalled session thread does not take its peer for dead`.

### Task 4: Feed cursors that survive, for a passive dialer

**Files:**
- Modify: `src/orchestrator/sync.rs`, `src/orchestrator/sync_driver.rs`, `src/orchestrator/catalog.rs`, `src/orchestrator/hub.rs`, `src/orchestrator/manage.rs`, `docs/CLI.md`, `docs/ARCHITECTURE.md`
- Test: `src/orchestrator/sync.rs`, `src/orchestrator/sync_driver.rs`, `src/orchestrator/catalog.rs`, `tests/peer_sync.rs`

**Interfaces:**
- Produces:
```rust
// sync.rs: both gain, with #[serde(default)]
pub struct FeedPage  { /* cursor, events, more, */ pub journal: String, pub top: u64 }
pub struct FeedReply { /* cursor, events, more, */ pub journal: String, pub top: u64 }
// catalog.rs, table peer_cursors(installation, repo, sent, received, local_journal, peer_journal)
pub struct SavedCursor { pub sent: u64, pub received: u64, pub local_journal: String, pub peer_journal: String }
impl Catalog {
    pub fn feed_cursor(&self, installation: &str, repo: &str) -> Result<Option<SavedCursor>>;
    pub fn save_feed_cursor(&self, installation: &str, repo: &str, cursor: &SavedCursor) -> Result<()>;
    pub fn feed_cursors_of_repo(&self, repo: &str) -> Result<Vec<(String, SavedCursor)>>;
    pub fn clear_feed_cursors(&self) -> Result<()>;
}
// sync_driver.rs, injected so tests can use a fake
pub trait CursorStore: Send + Sync {
    fn load(&self, installation: &str, repo: &str) -> Option<SavedCursor>;
    fn save(&self, installation: &str, repo: &str, cursor: &SavedCursor);
    fn clear(&self);
}
```
- Manage action `reset-cursors` (add to `MUTATIONS`), CLI `tkfs manage reset-cursors`: clears saved cursors and every slot's in-memory cursor, so the next burst starts over.

The rules, from the spec:
- `journal` is the store's `device`, `top` is `max(seq)` of its events.
- A passive dialer sends `{"repo": ..., "durable": true}` on `meta.exchange` and `{"durable": true}` on `feed.apply`. The worker runs `sync_metadata` before replying when `durable` is set and the request carried events.
- `received` and `sent` are saved only after those replies, after each page, so a long first sync keeps its progress.
- On a new session or worker, a passive driver loads the saved cursor instead of `FeedCursor::default()`. An empty `journal` in a reply (an older listener) means nothing is saved and nothing resumes.
- `exchange` restarts from zero when `page.journal != saved.local_journal`, `reply.journal != saved.peer_journal`, `page.top < cursor.sent`, or `reply.top < cursor.received`.
- An active driver never loads or saves a cursor. `Catalog::unpair` deletes that installation's rows.

- [ ] **Step 1: Write the failing unit tests.**
```rust
// sync.rs
fn a_durable_exchange_commits_before_replying()      // new pub(crate) Store::metadata_is_durable() is true after the reply
fn a_plain_exchange_does_not_force_a_commit()
fn feed_replies_name_their_journal_and_top()
// catalog.rs
fn feed_cursors_round_trip_and_unpair_removes_them()
// sync_driver.rs, using the existing policy_driver and dispatching fakes
fn a_passive_driver_resumes_a_saved_cursor_on_a_new_session()
fn a_resumed_exchange_starts_over_when_the_peer_journal_changed()
fn a_resumed_exchange_starts_over_when_the_peer_top_is_below_the_cursor()
fn a_resumed_exchange_starts_over_when_the_local_journal_changed()
fn an_active_driver_never_saves_a_cursor()
fn a_reply_with_no_journal_saves_nothing()
```

- [ ] **Step 2: Write the failing integration tests** in `tests/peer_sync.rs`. Add a helper `seed_events(state: &Path, count: usize)` that stops the folder, opens an `Engine` in-process, creates `count` empty files, closes it, and restarts the folder.
```rust
#[test]
fn a_passive_burst_after_a_restart_does_not_replay_the_log() -> Result<()> {
    // 10,000 events on the PC, passive A dialing through support::proxy. First sync-now completes.
    // Restart A (shutdown_keep_home, start_in), wait for lanes. Record proxy bytes_up + bytes_down.
    // sync-now. Assert the delta is under 64 * 1024 bytes.
}
#[test]
fn killing_the_listener_after_a_reply_loses_no_event_counted_as_sent() -> Result<()> {
    // Passive A imports a file and sync-now returns clean. Kill B immediately (kill_keep_home),
    // restart B: the file's entry is present on B. A's next sync-now still works.
}
#[test]
fn reset_cursors_makes_the_next_burst_start_over() -> Result<()> {
    // After reset-cursors a burst is a full exchange (proxy delta over the 64 KiB bound with 10,000 events).
}
```

- [ ] **Step 3: Run them.** Expected: FAIL to compile or assert.

- [ ] **Step 4: Implement** per the rules above. `Catalog` implements `CursorStore`. Pass it to `Driver` through `SyncPolicy` construction in `hub.rs`, using `ReadyPeer::installation` as the key. Give `exchange` a per-page callback that saves.

- [ ] **Step 5: Run.** Run: `cargo test --lib` and `cargo test --test peer_sync --test orchestrator`. Expected: PASS.

- [ ] **Step 6: Commit** as `feat: a passive dialer's feed cursors survive sessions and restarts`.

### Task 5: Metadata-only bursts

**Files:**
- Modify: `src/orchestrator/sync_driver.rs`, `src/orchestrator/hub.rs`, `src/orchestrator/manage.rs`, `docs/CLI.md`
- Test: `src/orchestrator/sync_driver.rs`, `tests/peer_sync.rs`

**Interfaces:**
- Produces: `pub enum BurstMode { Full, MetadataOnly }`, `Driver::sync_now(&self, folders: &[String], budget: Duration, mode: BurstMode) -> Value`, the `sync-now` payload field `metadata_only: bool`, and `tkfs manage sync-now [STATE] --metadata-only`.

- [ ] **Step 1: Write the failing tests.**
```rust
// sync_driver.rs
fn a_metadata_only_burst_takes_no_push_step() // nothing asked with objects.wanted or objects.store
// tests/peer_sync.rs
fn a_metadata_only_burst_moves_events_and_no_objects() -> Result<()> {
    // Passive A has a new file, B has a new file. Metadata-only sync-now: pushed == 0,
    // the name of A's file appears on B (wait_entry), and pack_bytes(B state) is unchanged.
    // Then a full sync-now, and A's file reads on B with identical bytes.
}
```

- [ ] **Step 2: Run them.** Expected: FAIL.

- [ ] **Step 3: Implement.** A burst stores its mode. `join_burst` marks only `exchange` due for a metadata-only burst and sets `no_push` on its pairs. Push steps are skipped for a pair when every open burst holding it is metadata-only, including the `background` mark `record_finish` adds when events move.

- [ ] **Step 4: Run.** Run: `cargo test --lib` and `cargo test --test peer_sync --test docs_cli`. Expected: PASS.

- [ ] **Step 5: Commit** as `feat: sync-now can move metadata alone`.

### Task 6: `manage set-endpoint`

**Files:**
- Modify: `src/orchestrator/network.rs`, `src/orchestrator/catalog.rs`, `src/orchestrator/hub.rs`, `src/orchestrator/manage.rs`, `src/model/errors.rs`, `README.md`, `docs/CLI.md`
- Test: `src/orchestrator/catalog.rs`, `tests/peer_sync.rs`

**Interfaces:**
- Produces: `Network::set_endpoint(&self, installation: &str, endpoint: &str) -> Result<()>`, `Catalog::set_peer_endpoint(&self, installation: &str, endpoint: &str) -> Result<()>` (replaces every row of that installation with one row, keeping credential and connections), action `set-endpoint {installation, endpoint}` added to `MUTATIONS`, CLI `tkfs manage set-endpoint INSTALLATION ENDPOINT`, error codes `PEER_NOT_FOUND` (existing) and `PEER_NOT_AT_ENDPOINT` (new).

- [ ] **Step 1: Write the failing tests.**
```rust
#[test]
fn set_endpoint_moves_a_peer_with_no_code() -> Result<()> {
    // A pairs with B through a proxy. set-endpoint to B's direct address. peers shows only the new
    // endpoint, lanes reach Ready, and sync-now moves a new file. The feed cursor still applies.
}
#[test]
fn set_endpoint_refuses_a_different_installation() -> Result<()> {
    // Address of an unrelated daemon C: PEER_NOT_AT_ENDPOINT, peers unchanged, lanes to B stay Ready.
}
```

- [ ] **Step 2: Run them.** Expected: FAIL.

- [ ] **Step 3: Implement.** Validate with `crate::ws::endpoint_uri`. Probe with the existing `accepts(endpoint, installation, key)` before changing anything. Close the old lanes the way `unpair` does, then `reopen(installation)`.

- [ ] **Step 4: Run.** Run: `cargo test --test peer_sync --test docs_cli` and `cargo test --lib catalog`. Expected: PASS.

- [ ] **Step 5: Commit** as `feat: set-endpoint moves a peer to a new address with no code`.

### Task 7: Pairing link and QR on the PC

**Files:**
- Create: `src/orchestrator/pairing_link.rs`
- Modify: `Cargo.toml`, `Cargo.lock`, `src/orchestrator/mod.rs`, `src/orchestrator/network.rs`, `src/orchestrator/hub.rs`, `src/orchestrator/manage.rs`, `src/gui.rs`, `src/gui_adapter.rs`, `ui/*.slint` (the pairing dialog near `pairing-code`), `docs/CLI.md`, `README.md`
- Test: `src/orchestrator/pairing_link.rs`, `src/orchestrator/network.rs`

**Interfaces:**
- Produces: `pairing_link::link(endpoint: Option<&str>, code: &str) -> String` giving `tkfs://pair?endpoint=<percent-encoded>&code=<hex>`, `pairing_link::qr_text(link: &str) -> Result<String>`, `Network::pairing_endpoint(&self) -> Option<String>`. The `reset-pairing-code` reply becomes `{code, link, endpoint}`. CLI `tkfs manage reset-pairing-code --qr`.
- `qrcode` crate with default features off, under `cfg(not(target_os = "android"))`.

- [ ] **Step 1: Write the failing tests.**
```rust
fn link_percent_encodes_the_endpoint_and_carries_the_code()
fn link_without_an_endpoint_has_only_the_code()
fn qr_text_renders_a_non_empty_block()
fn pairing_endpoint_prefers_advertise()
fn pairing_endpoint_is_none_for_a_loopback_or_empty_listener()
```

- [ ] **Step 2: Run them.** Expected: FAIL.

- [ ] **Step 3: Implement.** `pairing_endpoint`: non-empty `advertise` wins. An empty or loopback listener gives `None`. An unspecified address (`0.0.0.0`) gives `ws://<lan-ip>:<port>`, with the LAN address from a connected UDP socket's `local_addr`, which sends nothing. With `None`, `--qr` and the GUI still show the QR and say the phone needs the address typed and `network.listen` must be reachable. In the GUI, render the QR with `SharedPixelBuffer<Rgb8Pixel>` beside the existing code field, cleared whenever the code is cleared. The link holds the code, so never log it.

- [ ] **Step 4: Run.** Run: `cargo test --lib pairing` and `cargo test --test docs_cli --test desktop`. Expected: PASS. Run: `cargo build --release --bin tkfs`. Expected: builds with the GUI.

- [ ] **Step 5: Commit** as `feat: a new pairing code can be shown as a QR for a phone`.

---

## Part 2: The real app, read side

### Task 8: App foundation, read-only provider and the end-to-end harness

**Files:**
- Delete: `android/app/src/main/java/dev/tkfs/spike/`
- Create: `android/app/src/main/java/dev/tkfs/Native.kt`, `.../dev/tkfs/provider/DocumentIds.kt`, `.../dev/tkfs/provider/TkfsProvider.kt`, `.../dev/tkfs/provider/FileProxy.kt`, `.../dev/tkfs/provider/ShellControl.kt`, `android/app/src/test/java/dev/tkfs/provider/DocumentIdsTest.kt`, `scripts/android_e2e.py`
- Modify: `android/build.gradle`, `android/app/build.gradle`, `android/app/src/main/AndroidManifest.xml`, `android/native/src/lib.rs`, `android/native/Cargo.toml`, `scripts/build_android.py` (Gradle `:app:assembleDebug` unchanged)

**Interfaces:**
- Consumes: Task 2's `Host`.
- Produces, JNI on `dev.tkfs.Native`, failures as `IOException` carrying the code:
```kotlin
external fun start(config: String)
external fun manage(action: String, payload: String): String      // Daemon::execute
external fun list(folder: String, path: String): String           // [{name, directory, size, modified_ms}]
external fun stat(folder: String, path: String): String
external fun open(folder: String, path: String, writable: Boolean): Long
external fun read(folder: String, token: Long, offset: Long, out: ByteArray, size: Int): Int
external fun close(folder: String, token: Long)
fun ensureStarted(context: Context)
```
- Package and authority: `dev.tkfs`, `dev.tkfs.documents`. Label "Telekinesis". `compileSdk 35`, `targetSdk 35`, `minSdk 26`. Kotlin 2.x, Compose BOM, Material 3, `work-runtime-ktx`, `play-services-code-scanner`, JUnit 4; exact versions are the builder's.
- `DocumentIds.split(id: String): Pair<String, String>` (first colon only), `DocumentIds.make(folder, path)`, `DocumentIds.isChild(parent, id)`.
- Config written once by `ensureStarted`: the spec's TOML, plus `data_directory`, `default_mount_directory` and `maximum_running = 8` from the spike, `connections = 4`, `worker_lanes = 2`.
- `ShellControl`: `TkfsProvider.call` answers method `tkfs.manage` (arg = action, extra `payload`) and later `tkfs.*` methods, returning `{"result": json}`, and only when `BuildConfig.DEBUG` and `Binder.getCallingUid()` is 2000 or 0. Everything else goes to `super.call`. This is how the harness drives the app.
- `scripts/android_e2e.py`: `--scenario NAME|all`, `--avd Medium_Phone_API_36.0`, `--keep-emulator`, `--skip-build`. It boots the AVD with no window if no device is online (bound 300 s), builds and installs the x86_64 APK, starts a headless PC orchestrator (`target/debug/tkfs -f CONFIG start`, config on `127.0.0.1:<free port>`, reached from the emulator at `10.0.2.2`), and resets the app with `pm clear` unless told otherwise. It writes `test-runs/android-e2e/<UTC>/report.json` and `logcat.txt`. Every wait is bounded. A PC process that will not stop is reported as a failure. Helpers: `shell_manage(action, payload)`, `content_read(doc)`, `content_write(doc, local_file)`, `content_query(uri)`, `ui_dump()`, `tap_text(text)`, `wait_frozen(package, bound)` (`dumpsys activity processes`), `pc_cat(state, path)`.

- [ ] **Step 1: Write the failing JVM test.**
```kotlin
@Test fun splitsAtTheFirstColonOnly() { assertEquals("F" to "/a:b c%#é.txt", DocumentIds.split("F:/a:b c%#é.txt")) }
@Test fun aSiblingWithTheSamePrefixIsNotAChild() { assertFalse(DocumentIds.isChild("F:/a", "F:/ab/c")) }
@Test fun theRootHoldsItsChildren() { assertTrue(DocumentIds.isChild("F:/", "F:/x")) }
```
Run: `android\gradlew.bat -p android :app:testDebugUnitTest`. Expected: FAIL.

- [ ] **Step 2: Implement `DocumentIds`, `Native`, the JNI functions and the read-only provider,** porting the spike's provider. `onRead` keeps its reopen-by-path fallback. A plain launcher `Activity` showing "Telekinesis" is enough until Task 9. Run the JVM test. Expected: PASS.

- [ ] **Step 3: Add scenario `read` to the harness.** The PC folder holds `notes.txt`, `docs/sub.txt`, `a b%#:é.txt` and a 2 MiB `photo.png` with known SHA-256. Pair and receive through `shell_manage`, `sync-now`, query children and read each through `adb shell content`, compare hashes against the PC. Then with no further reads, assert the phone's folder pack size is unchanged across listing every directory (acceptance 15, thumbnails come in Task 16). Also run `cold_start`: `am force-stop`, then `content read` of a downloaded file works.

- [ ] **Step 4: Run.** Run: `python scripts/android_e2e.py --scenario read`. Expected: PASS, with a report listing each check.

- [ ] **Step 5: Commit** as `feat: real app foundation with a read-only provider and an end-to-end harness`.

### Task 9: Screens: Folders, Add a PC, Settings shell

**Files:**
- Create: `.../dev/tkfs/ui/MainActivity.kt`, `.../ui/Theme.kt`, `.../ui/AppState.kt`, `.../ui/FolderLine.kt`, `.../ui/FoldersScreen.kt`, `.../ui/AddPcScreen.kt`, `.../ui/SettingsScreen.kt`, `.../PairLink.kt`, `android/app/src/test/java/dev/tkfs/ui/FolderLineTest.kt`, `android/app/src/test/java/dev/tkfs/PairLinkTest.kt`

**Interfaces:**
- Consumes: `Native.manage` actions `status`, `pair`, `remote-list`, `receive`, `sync-now`, `unpair`, `set-endpoint`, `peers`.
- Produces:
```kotlin
data class PairInfo(val endpoint: String?, val code: String)
object PairLink { fun parse(text: String): PairInfo? }   // tkfs://pair?endpoint=&code=; code must be 64 hex
enum class Tone { Ok, Waiting, Bad, Quiet }
data class Line(val text: String, val tone: Tone)
fun folderLine(s: FolderStatus, now: Long): Line
```
- Copy comes from the mockup. Theme: pine primary `#0E6B5A` with dynamic colour off, light and dark, system font.

`folderLine` precedence, first match wins: folder error, unresolved conflicts ("2 conflicts to settle"), waiting ("3 files waiting to send"), PC unreachable with no waiting ("Synced at 14:02"), last sync ("Synced 2 min ago"), never synced ("Not synced yet"). A top strip shows "My PC isn't reachable. Last synced at 14:02." `PEER_REVOKED` shows "My PC no longer recognizes this phone. Pair again." with the action going to Add a PC.

- [ ] **Step 1: Write the failing JVM tests.**
```kotlin
@Test fun parsesEndpointAndCode()
@Test fun aLinkWithNoEndpointStillGivesTheCode()
@Test fun refusesACodeThatIsNot64Hex()
@Test fun conflictsOutrankWaitingAndWaitingOutranksSynced()
@Test fun aFolderErrorIsShownInWords()
@Test fun aRevokedPeerSaysToPairAgain()
```
Run: `:app:testDebugUnitTest`. Expected: FAIL.

- [ ] **Step 2: Implement.** `AppState` is a `StateFlow` refreshed from `status` and `peers` every 2 s while a screen shows and after each action, off the main thread. Native errors reach the screen as their code plus a plain sentence. Add a PC: "Scan QR code" uses `GmsBarcodeScanning`, which needs no camera permission; the two fields accept pasting and a pasted `tkfs://` link fills both. After pairing, list `remote-list` with a checkbox per folder, then `receive` the chosen ones and run a burst. Settings has the PC row with Change address and Unpair, and placeholders the later tasks fill.

- [ ] **Step 3: Run.** Run: `:app:testDebugUnitTest`. Expected: PASS.

- [ ] **Step 4: Add scenario `ui_folders`.** After pairing through the fields with `tap_text` and `adb shell input text`, `ui_dump()` contains the folder's label and a Synced line. Then `reset-pairing-code` on the PC, trigger a burst: the dump contains "Pair again" (Review Focus 5). Pair again with the new code: the folder and its downloaded bytes are still there, and sync works. Save a screenshot of each screen to the run directory with `screencap`, and read them to check the layout against the mockup.

- [ ] **Step 5: Run.** Run: `python scripts/android_e2e.py --scenario ui_folders`. Expected: PASS.

- [ ] **Step 6: Commit** as `feat: Folders, Add a PC and Settings screens`.

### Task 10: When syncing happens

**Files:**
- Create: `.../dev/tkfs/sync/SyncPolicy.kt`, `.../sync/SyncCoordinator.kt`, `.../sync/BurstWorker.kt`, `.../Settings.kt`, `android/app/src/test/java/dev/tkfs/sync/SyncPolicyTest.kt`
- Modify: `TkfsProvider.kt`, `MainActivity.kt`, `FoldersScreen.kt`, `SettingsScreen.kt`

**Interfaces:**
- Consumes: Task 5's `metadata_only`.
- Produces:
```kotlin
enum class Trigger { Foreground, Manual, Listing, Periodic, LocalChange }
enum class Mode { Full, MetadataOnly, Skip }
class SyncPolicy(/* pure, takes nowMs */) {
    fun decide(trigger: Trigger, nowMs: Long, lastSyncMs: Long, metered: Boolean, syncOnMobile: Boolean): Mode
}
object SyncCoordinator {
    fun onForeground(ctx: Context); fun syncNow(ctx: Context); fun onListing(ctx: Context)
    fun onLocalChange(ctx: Context)  // debounced 5 s; enqueues Task 14's transfer job, a burst until then
    fun burst(ctx: Context, mode: Mode): BurstResult
}
```
- Values from the spec: foreground at most once per 30 s; listing when the last sync is older than 2 min; periodic default 30 min, requires a network; local changes debounced 5 s. Metered with the setting off: `Foreground`, `Listing` and `Periodic` are `MetadataOnly`; `Manual` is `Full`.
- `BurstResult` carries the `sync-now` reply. After any burst with `exchanged > 0`, call `notifyChange` on the roots URI and the tree. Provider cursors set a notification URI. A listing is answered at once and the burst runs behind it.

- [ ] **Step 1: Write the failing JVM tests.**
```kotlin
@Test fun foregroundWaitsThirtySeconds()
@Test fun aListingSyncsOnlyWhenOlderThanTwoMinutes()
@Test fun meteredWithTheSettingOffMovesMetadataOnly()
@Test fun manualIsAlwaysFull()
@Test fun meteredWithTheSettingOnIsFull()
```
Run: `:app:testDebugUnitTest`. Expected: FAIL.

- [ ] **Step 2: Implement.** Periodic work uses `enqueueUniquePeriodicWork` with `NetworkType.CONNECTED`, re-enqueued when the interval setting changes. `BurstWorker` runs one burst bounded by the native bound and returns success whatever the result, because the next trigger retries. Keep `lastSyncMs` per folder in `Settings`.

- [ ] **Step 3: Run.** Run: `:app:testDebugUnitTest`. Expected: PASS.

- [ ] **Step 4: Add scenario `sync`.** Change a file on the PC: `content query` of the tree is stale at first, and after `am start` of the app the new name appears within a bound. A listing older than 2 min triggers a burst, set up through a `tkfs.set_last_sync` shell method. `dumpsys jobscheduler` shows the periodic job. Check with lane byte counters that an idle minute causes no exchange.

- [ ] **Step 5: Run.** Run: `python scripts/android_e2e.py --scenario sync`. Expected: PASS.

- [ ] **Step 6: Commit** as `feat: bursts on foreground, listing, schedule and request`.

### Task 11: First-read speed

**Files:**
- Modify: `src/orchestrator/demand.rs`, `src/content/local.rs`, `src/runtime/engine.rs`, `src/orchestrator/sync_driver.rs`, `android/native/src/lib.rs`, `tests/embedded.rs`

**Interfaces:**
- Produces:
```rust
// content/local.rs
pub trait ObjectFetcher { fn fetch(..); fn hint(&self, _o: &[(Digest, Kind)]) {} fn unhint(&self, _o: &[(Digest, Kind)]) {} }
impl LocalContent { pub fn hint_range(&self, r: &FileContentRef, offset: u64, len: u64) -> Result<Vec<(Digest, Kind)>>; } // only missing chunks; never blocks if the manifest is absent
// orchestrator/demand.rs
impl Demand { pub const HINT_TTL: Duration = Duration::from_secs(15); pub const HINT_PENDING: usize = 256; }
// runtime/engine.rs
pub const READ_AHEAD: u64 = 16 * 1024 * 1024;
impl Engine { pub fn read_sequential(&self, token: u64, offset: u64, out: &mut [u8]) -> Result<usize>; }
```
`Native.read` calls `read_sequential`. Desktop callers keep `read`.

Rules: `pending()` lists waiting reads first, then unexpired hints up to `HINT_PENDING`. Hints expire after `HINT_TTL` unless refreshed, so an idle viewer holding a file open does not keep the pump busy. `read_sequential` hints the window after the read once the reader has passed half of the last window, and the handle's close unhints what it hinted.

- [ ] **Step 1: Measure first.** Add a bounded test run timing a sequential read of a 32 MiB file through the embedded `Host` with plain `Engine::read`, with timing logs around demand wake-up, fetch and write. Put the numbers and the confirmed cause in the commit message of this task. If the cause is not missing read-ahead, stop and report before changing anything.

- [ ] **Step 2: Write the failing tests.**
```rust
// demand.rs
fn hints_come_after_waiting_reads_in_pending()
fn a_hint_expires_unless_refreshed()
fn unhint_removes_a_hint_only_when_no_handle_holds_it()
fn arrival_clears_hints()
// sync_driver.rs
fn a_single_peer_at_rest_keeps_its_full_deadline_for_a_reader()
// tests/embedded.rs
fn a_sequential_first_read_is_at_least_five_times_faster_than_plain_reads()
fn a_partial_read_does_not_fetch_the_whole_file()
// 64 MiB file; read the first 1 MiB; after 3 s the phone holds at most (1 MiB + READ_AHEAD + one batch) of its chunks.
fn an_idle_open_handle_stops_asking_after_the_hint_ttl()
```

- [ ] **Step 3: Run them.** Expected: FAIL.

- [ ] **Step 4: Implement.** In `Driver::supply`, change `hurried` to `interrupt.is_none() && index + 1 < endpoints.len()`, so the last candidate gets its full deadline. This closes the "slow peers locked out" case for a phone with one PC; a reader still fails at `FETCH_WAIT`.

- [ ] **Step 5: Run.** Run: `cargo test --lib` and `cargo test --test embedded --test peer_sync`. Expected: PASS.

- [ ] **Step 6: Add scenario `speed`.** Read a new 64 MiB random file on the emulator through `content read` from a release-built PC orchestrator. Record MiB/s in the report, and fail below 10 MiB/s. It runs over the emulator's network, so note in the report that it is not Wi-Fi.

- [ ] **Step 7: Run.** Run: `python scripts/android_e2e.py --scenario speed`. Expected: PASS at 10 MiB/s or better. If not, report the number and the remaining bottleneck rather than lowering the bound.

- [ ] **Step 8: Commit** as `feat: read-ahead for first reads on a phone`, with the measurements in the message.

---

## Part 3: Write side

### Task 12: `objects.have`

**Files:**
- Modify: `src/model/protocol.rs`, `src/orchestrator/sync.rs`, `src/orchestrator/worker_client.rs`, `src/orchestrator/network.rs`, `src/orchestrator/hub.rs`, `docs/ARCHITECTURE.md`
- Test: `src/orchestrator/sync.rs`, `src/orchestrator/network.rs`, `tests/peer_sync.rs`

**Interfaces:**
- Produces:
```rust
// protocol.rs
pub const CONTENT_HAVE: &str = "content-have";          // advertised with CONTENT_PUSH
// sync.rs
pub const HAVE_BATCH: usize = 4096;
// worker_client.rs
impl WorkerLink { pub fn have(&self, digests: &[Digest]) -> Result<Vec<Digest>, CallError>; }
// hub.rs
pub enum Held { Known(BTreeSet<Digest>), NoFeature, Unreachable(String) }
impl Daemon { pub fn held_by(&self, folder: &str, installation: &str, digests: &[Digest]) -> Result<Held>; } // pages by HAVE_BATCH
```
- Wire: `objects.have` on a bulk lane, header `{repo}`, JSON body `[digest]`, reply `{held: [digest]}`. More than `HAVE_BATCH` is `ARGUMENT_TOO_LARGE`. `PeerService` applies `require_paired` and the same folder lookup as `objects.fetch`. The worker flushes packs before answering.

- [ ] **Step 1: Write the failing tests.**
```rust
// sync.rs
fn objects_have_names_only_what_the_packs_hold()
fn objects_have_flushes_before_replying()
fn objects_have_refuses_an_oversized_list()
// network.rs, next to unpaired_and_unpublished_store_are_refused
fn unpaired_and_unpublished_have_are_refused()
// tests/peer_sync.rs
fn held_by_confirms_a_pushed_file_and_not_an_unknown_digest() -> Result<()>
fn held_by_reports_a_peer_without_the_feature() -> Result<()>   // feature override, as the older-peer tests do
fn held_by_reports_an_unreachable_peer() -> Result<()>
```

- [ ] **Step 2: Run them.** Expected: FAIL.

- [ ] **Step 3: Implement** as specified.

- [ ] **Step 4: Run.** Run: `cargo test --lib` and `cargo test --test peer_sync`. Expected: PASS.

- [ ] **Step 5: Commit** as `feat: objects.have lets a dialer confirm what a listener holds`.

### Task 13: Provider writes

**Files:**
- Modify: `android/native/src/lib.rs`, `Native.kt`, `TkfsProvider.kt`, `FileProxy.kt`, `scripts/android_e2e.py`
- Test: `android/app/src/test/java/dev/tkfs/provider/OpenModeTest.kt`

**Interfaces:**
- Produces:
```kotlin
external fun write(folder: String, token: Long, offset: Long, data: ByteArray, size: Int, append: Boolean): Int
external fun length(folder: String, token: Long): Long
external fun truncate(folder: String, token: Long, size: Long)
external fun sync(folder: String, token: Long)        // Engine::flush(Some(token))
external fun create(folder: String, path: String, directory: Boolean)
external fun rename(folder: String, from: String, to: String)
external fun delete(folder: String, path: String)
data class OpenMode(val read: Boolean, val write: Boolean, val truncate: Boolean, val append: Boolean)
fun parseOpenMode(mode: String): OpenMode
fun numberedName(existing: Set<String>, name: String): String   // also used by Task 14
```
`parseOpenMode`: `r` read; `w` and `wt` write and truncate; `rw` read and write; `rwt` read, write and truncate; `wa` write and append. Anything else is refused.

Behaviour: `onGetSize` returns `Native.length` live. `onFsync` calls `sync`. A writable handle's `onRelease` calls `sync`, then `close`, then `SyncCoordinator.onLocalChange` and `notifyChange` on the parent. Writes never use a raw descriptor. Document flags: `FLAG_SUPPORTS_WRITE`, `DELETE`, `RENAME`, `MOVE`, and `DIR_SUPPORTS_CREATE` on directories; root `FLAG_SUPPORTS_CREATE`. `moveDocument` within a folder uses `rename`; across folders it refuses, so Android copies and deletes. `createDocument` on a name clash numbers the name, `name (1).ext`.

- [ ] **Step 1: Write the failing JVM test.**
```kotlin
@Test fun wTruncates()
@Test fun rwKeepsContents()
@Test fun waAppends()
@Test fun anUnknownModeIsRefused()
@Test fun aFreeNameIsKept()
@Test fun aClashGetsTheFirstFreeNumberBeforeTheExtension()
@Test fun aNameWithNoExtensionGetsANumber()
```
Run: `:app:testDebugUnitTest`. Expected: FAIL.

- [ ] **Step 2: Implement.** Run the JVM test. Expected: PASS.

- [ ] **Step 3: Add scenario `write`,** acceptance 11, each checked on the PC with `pc_cat` after a burst: create through `content write`, edit, rename, move into a subfolder, delete; names from Review Focus 1. Force-stop right after a write's release, and the content still arrives after restart. Review Focus 2 with the PC stopped: `wt` on a file never downloaded succeeds and reaches the PC later, and `rw` writing part of a file never downloaded fails with an I/O error, leaving the file's listed size and later PC contents unchanged.

- [ ] **Step 4: Run.** Run: `python scripts/android_e2e.py --scenario write`. Expected: PASS.

- [ ] **Step 5: Commit** as `feat: the provider writes, creates, renames and deletes`.

### Task 14: Transfer job and sharing in

**Files:**
- Create: `.../dev/tkfs/sync/TransferWorker.kt`, `.../dev/tkfs/share/ShareActivity.kt`, `.../dev/tkfs/share/ShareImportService.kt`, `android/app/src/main/res/drawable/ic_notification.xml`
- Modify: `AndroidManifest.xml`, `SyncCoordinator.kt`, `SettingsScreen.kt`, `scripts/android_e2e.py`

**Interfaces:**
- Consumes: `Native.create/open/write/close`, `SyncCoordinator`.
- Produces: `TransferWorker`, unique work name `transfer`, expedited with `OutOfQuotaPolicy.RUN_AS_NON_EXPEDITED_WORK_REQUEST`, foreground type `dataSync`, requiring `NetworkType.UNMETERED` when "Sync on mobile data" is off and `CONNECTED` otherwise. `SyncCoordinator.onLocalChange` enqueues it.

Rules: the job repeats full bursts until `remaining` is zero, the work is stopped, or a burst made no progress (same `remaining`, `pushed == 0`). On stop it returns `Result.success`; on the next start it continues. Manifest adds `FOREGROUND_SERVICE`, `FOREGROUND_SERVICE_DATA_SYNC`, `POST_NOTIFICATIONS`, `REQUEST_IGNORE_BATTERY_OPTIMIZATIONS`. Settings has the battery row, asking once and showing the state. `ShareImportService` is started by `ShareActivity` with the stream URIs in `ClipData` and the read grant flag, so the grant lasts as long as the service. It copies with a progress notification, seals each file, then enqueues the transfer job. `ShareActivity` copy matches the mockup: "Save to Telekinesis", folder radio list, "Inside this folder", "Save 2 files".

- [ ] **Step 1: Implement.** Run `:app:testDebugUnitTest`. Expected: PASS, with `numberedName` from Task 13.

- [ ] **Step 2: Add scenario `share`,** acceptance 12 and 13. Push a file to `/sdcard/Download`, scan it, and start `ShareActivity` with `am start -a android.intent.action.SEND --eu android.intent.extra.STREAM <media uri> --grant-read-uri-permission`, then tap through with `tap_text`. With the PC stopped: the app keeps the file. Start the PC: it arrives, with no action on the phone beyond a trigger from the table. Then a 256 MiB file: `am force-stop` mid-push, and the push completes after restart with identical bytes. Also force-stop during a 256 MiB receive, and that completes too. With the PC stopped, a shared file clash gets a numbered name.

- [ ] **Step 3: Run.** Run: `python scripts/android_e2e.py --scenario share`. Expected: PASS.

- [ ] **Step 4: Commit** as `feat: a transfer job and sharing into a folder`.

### Task 15: Waiting to send

**Files:**
- Create: `android/native/src/phone.rs`
- Modify: `android/native/src/lib.rs`, `android/native/Cargo.toml`, `src/runtime/engine.rs`, `Native.kt`, `AppState.kt`, `SyncCoordinator.kt`, `scripts/android_e2e.py`
- Test: `android/native/src/phone.rs`, `src/runtime/engine.rs`

**Interfaces:**
- Consumes: Task 4's `Catalog::feed_cursors_of_repo`, Task 12's `Daemon::held_by`.
- Produces:
```rust
// engine.rs
pub struct HeadClosure { pub written_here: bool, pub deleted: bool, pub digests: Vec<Digest> } // manifest and chunks
impl Engine {
    pub fn own_events_after(&self, after: u64, limit: usize) -> Result<Vec<(u64, Event)>>; // author == device
    pub fn head_closure(&self, entity: &str) -> Result<HeadClosure>;                        // never fetches
}
// phone.rs, database <data_directory>/phone.sqlite
pub trait Confirmer { fn sent(&self, repo: &str) -> Vec<(String, u64)>; fn held(&self, installation: &str, digests: &[Digest]) -> Held; }
pub struct Phone;
impl Phone {
    pub fn open(path: &Path) -> Result<Phone>;
    pub fn scan(&self, folder: &str, engine: &Engine) -> Result<()>;
    pub fn confirm(&self, folder: &str, repo: &str, engine: &Engine, peers: &dyn Confirmer) -> Result<Confirmed>; // {waiting, unsupported}
    pub fn waiting(&self, folder: &str) -> Result<Vec<Waiting>>;                                                  // {entity, path}
}
```
- JNI: `Native.waiting(folder): String`, `Native.confirm(folder): String`, called after each burst and when the app shows.

Rules from the spec: a file leaves the table when one peer has both `sent >= its event seq` and, for content, every digest of its head closure held. A head not written here needs no bytes. A delete needs no bytes. An edit returns it to the table through its new event. `burst.remaining` is not used.

- [ ] **Step 1: Write the failing tests** in `phone.rs`, with a real `Engine` on a temp dir and a fake `Confirmer`.
```rust
fn a_new_file_waits_until_sent_and_held()
fn sent_alone_does_not_clear_a_file()
fn held_alone_does_not_clear_a_file()
fn a_file_edited_again_waits_again()
fn a_delete_waits_for_the_exchange_only()
fn a_rename_waits_for_the_exchange_only()
fn a_head_written_by_a_peer_needs_no_bytes()
fn one_confirming_peer_is_enough()
fn a_peer_without_the_feature_leaves_files_waiting_and_says_so()
fn waiting_survives_reopening_the_database()
```
Run: `cargo test --manifest-path android/native/Cargo.toml`. Expected: FAIL.

- [ ] **Step 2: Implement.** Run the crate tests. Expected: PASS. Run `cargo test --lib`. Expected: PASS.

- [ ] **Step 3: Extend scenario `share`** with the waiting count: with the PC stopped, `ui_dump` shows "1 file waiting to send", and it still does after `am force-stop`; after the PC returns and a burst, it no longer does and the PC has the bytes. An unsent file in a folder shows "Update the PC" against a PC with `content-have` turned off by the fault override.

- [ ] **Step 4: Add scenario `metered`,** acceptance 17, using `adb shell cmd netpolicy set metered-network` or the emulator's equivalent, with the setting off: opening the app syncs a name from the PC, a shared file stays waiting and moves no bytes (PC pack size unchanged), and opening an undownloaded file still downloads.

- [ ] **Step 5: Run.** Run: `python scripts/android_e2e.py --scenario share` and `--scenario metered`. Expected: PASS.

- [ ] **Step 6: Commit** as `feat: waiting to send is durable and confirmed by the PC`.

### Task 16: Browse

**Files:**
- Create: `.../ui/BrowseScreen.kt`, `.../ui/FileRow.kt`, `android/app/src/test/java/dev/tkfs/ui/FileRowTest.kt`
- Modify: `src/content/local.rs`, `src/runtime/engine.rs`, `android/native/src/lib.rs`, `TkfsProvider.kt`, `MainActivity.kt`, `scripts/android_e2e.py`
- Test: `src/runtime/engine.rs`

**Interfaces:**
- Produces:
```rust
impl LocalContent { pub fn chunks_if_local(&self, r: &FileContentRef) -> Result<Option<Vec<Digest>>>; } // None if the manifest is absent; never fetches
pub struct Presence { pub local_bytes: u64, pub total_bytes: u64 }
impl Engine { pub fn presence(&self, path: &str) -> Result<Presence>; }
impl Daemon { pub fn reach_a_peer(&self, within: Duration) -> Result<()>; } // nudges lanes out of backoff; PC_UNREACHABLE if no control lane is Ready in time
```
- `openDocument` for reading a file not fully local calls `Native.reachPeer(folder)`, which waits at most 3 s, so with no reachable peer the open fails within the spec's 5 s. The app then posts a notification naming the file: "My PC isn't reachable, so video-obra.mp4 can't open yet."
- `Native.list` and `stat` rows gain `local_bytes`. `fun rowLine(name, size, localBytes, waiting, kept): Line` gives "On this phone, 3.4 MB", "Waiting to send, 2.3 MB", "Not downloaded, 812 MB", "Kept on this phone, ...".
- Provider: `COLUMN_SUMMARY` is "On this phone" or "Not downloaded". `FLAG_SUPPORTS_THUMBNAIL` and `openDocumentThumbnail` only for image files fully local, decoding with `BitmapFactory` to the requested size.
- Browse: tap opens through `ACTION_VIEW` with `FLAG_GRANT_READ_URI_PERMISSION`. A long press opens the bottom sheet with the mockup's actions and wording; Keep on this phone and Remove download are shown disabled until Task 19. "Delete everywhere" confirms first.

- [ ] **Step 1: Write the failing tests.**
```rust
fn presence_counts_local_chunks_and_never_fetches()     // offline, no manifest: local_bytes == 0, returns at once
fn presence_of_a_partly_read_file_is_partial()
// tests/embedded.rs
fn reach_a_peer_fails_within_its_bound_with_no_peer_up()
fn reach_a_peer_redials_a_lane_in_backoff_at_once()
```
```kotlin
@Test fun waitingOutranksOnThisPhone()
@Test fun aPartlyDownloadedFileIsNotOnThisPhone()
```
Run: `cargo test --lib presence` and `:app:testDebugUnitTest`. Expected: FAIL.

- [ ] **Step 2: Implement.** Cache a fully local result by content digest in memory; recompute others. Run the tests. Expected: PASS.

- [ ] **Step 3: Extend scenario `read`** for acceptance 15: list a folder of 50 images through the provider and query thumbnails: PC bytes sent stay under a small bound (metadata only), and only local images offer thumbnails. Add scenario `ui_browse`: rows show the right lines, tapping opens a viewer intent (`dumpsys activity`), and the sheet's wording matches the mockup. With the PC stopped, `content read` of a file never opened fails within 5 s and `dumpsys notification` shows the message. Save screenshots and read them.

- [ ] **Step 4: Run.** Run: `python scripts/android_e2e.py --scenario read` and `--scenario ui_browse`. Expected: PASS.

- [ ] **Step 5: Commit** as `feat: browse files with what is on the phone`.

### Task 17: Conflicts

**Files:**
- Create: `.../ui/ConflictsScreen.kt`, `android/app/src/test/java/dev/tkfs/ui/ConflictsTest.kt`
- Modify: `MainActivity.kt`, `FoldersScreen.kt`, `AppState.kt`, `scripts/android_e2e.py`

**Interfaces:**
- Consumes: `Native.manage("folder", {state, op, payload})` with ops `conflicts`, `preview-version` `{version}`, `resolve` `{entity, version}`.
- Produces: `fun versionLine(e: VersionEvent, device: String, now: Long): String` ("This phone, Today at 11:42, 2.3 MB", or the peer's name).

- [ ] **Step 1: Write the failing JVM test** for `versionLine`: this device, a peer, today and an earlier date.

- [ ] **Step 2: Implement** from the mockup: "Both versions stay until you choose.", Preview in a dialog, "Keep this one". `preview-version` reads up to 64 KiB, which downloads that much when absent. Run the test. Expected: PASS.

- [ ] **Step 3: Add scenario `conflicts`,** acceptance 18. Edit one file on the PC and the phone between bursts. The phone shows "1 conflict to settle". Choosing one through the UI settles it on both sides after a burst, with the chosen bytes.

- [ ] **Step 4: Run.** Run: `python scripts/android_e2e.py --scenario conflicts`. Expected: PASS.

- [ ] **Step 5: Commit** as `feat: settle conflicts from the phone`.

---

## Part 4: Space

### Task 18: Evict in the core

**Files:**
- Modify: `src/runtime/engine.rs`, `src/core.rs`, `tests/embedded.rs`
- Test: `src/runtime/engine.rs`, `tests/embedded.rs`

**Interfaces:**
- Produces:
```rust
pub struct Evicted { pub removed: usize, pub refused: usize }
impl Engine {
    /// Removes `digests` from the packs, except what this store must keep.
    pub fn evict(&self, digests: &BTreeSet<Digest>) -> Result<Evicted>;
    pub fn hydrate(&self, entity: &str, cancel: &dyn Fn() -> bool) -> Result<Presence>; // sequential demand, bounded by FETCH_WAIT per step
    pub fn head_chunks(&self, entity: &str) -> Result<Vec<Digest>>;                     // chunks only, never fetches
}
```
Rules: under the same locks as `collect`, `live = live_objects(32) - (digests - protected)`, where `protected` is the closure of every unresolved conflict version plus every working draft's basis. Open handles are already covered by `state.pins`. Call `packs.collect(&live, 1.0)`, so an explicit removal takes effect. Manifests and trees are never in `digests`; refuse and count them.

- [ ] **Step 1: Write the failing tests.**
```rust
// engine.rs
fn evict_removes_a_file_chunks_and_keeps_its_manifest()
fn evict_keeps_the_chunks_of_an_unresolved_conflict_version()
fn evict_keeps_the_basis_of_a_draft()
fn evict_keeps_what_an_open_handle_pins()
fn evict_refuses_manifests_and_trees()
fn evict_does_not_touch_other_files_sharing_a_segment()
// tests/embedded.rs
fn a_freed_file_reads_back_identically_from_the_peer() -> Result<()>
fn hydrate_downloads_a_whole_file_without_a_reader() -> Result<()>
```

- [ ] **Step 2: Run them.** Expected: FAIL.

- [ ] **Step 3: Implement.**

- [ ] **Step 4: Run.** Run: `cargo test --lib evict` and `cargo test --test embedded`. Expected: PASS.

- [ ] **Step 5: Commit** as `feat: evict lets a store free objects it can get back`.

### Task 19: Keep and free

**Files:**
- Modify: `android/native/src/phone.rs`, `android/native/src/lib.rs`, `Native.kt`, `TkfsProvider.kt`, `BrowseScreen.kt`, `SettingsScreen.kt`, `TransferWorker.kt`, `SyncCoordinator.kt`, `FoldersScreen.kt`, `scripts/android_e2e.py`

**Interfaces:**
- Consumes: Task 12's `held_by`, Task 18's `evict`, `hydrate`, `head_chunks`, Task 16's `presence`.
- Produces:
```rust
impl Phone {
    pub fn mark(&self, folder: &str, entity: &str, keep: bool) -> Result<()>;
    pub fn is_kept(&self, folder: &str, entity: &str, ancestors: &[String]) -> Result<bool>;
    pub fn opened(&self, folder: &str, entity: &str, now_ms: u64) -> Result<()>;
    pub fn remove_download(&self, folder: &str, repo: &str, entity: &str, engine: &Engine, peers: &dyn Confirmer) -> Result<Freed>;
    pub fn free_oldest(&self, folder: &str, repo: &str, engine: &Engine, peers: &dyn Confirmer, need_bytes: u64) -> Result<Freed>;
    pub fn hydrate_kept(&self, folder: &str, engine: &Engine, cancel: &dyn Fn() -> bool) -> Result<u64>;
}
pub struct Freed { pub bytes: u64, pub blocked: Option<String> } // PC_UNREACHABLE, PC_TOO_OLD, NOT_ON_PC
```
- JNI: `mark`, `removeDownload`, `freeSpace(needBytes)`, `hydrateKept`, plus `Native.opened`, called from `openDocument`. Kotlin: `val floor = maxOf(2 GiB, volume / 10)` from `StatFs(filesDir)`.

Rules from the spec: free only digests named by `held_by` just before evicting. Never free a marked file (its chunks are subtracted even when a freed file shares them), a file with an open handle or draft, or an unresolved conflict version (the last two enforced by `evict`). Oldest `opened` first, with never-opened treated as oldest. The floor check runs at the end of each burst and on "Free up space now". Offline and below the floor: nothing freed, and Folders shows "Low on space. Can't free anything until My PC is reachable." Kept files download during transfer jobs. Browse's Keep and Remove actions become live; "Remove download" with `NOT_ON_PC` says "Not on My PC yet, so it can't be removed." Settings shows "Keep this much free" and "Free up space now".

- [ ] **Step 1: Write the failing tests** in `phone.rs`, fake `Confirmer`, real `Engine`.
```rust
fn oldest_opened_goes_first_and_stops_when_enough_is_freed()
fn a_marked_file_is_never_freed()
fn a_marked_folder_covers_files_that_arrive_later()
fn a_mark_follows_a_rename()
fn a_chunk_shared_with_a_marked_file_stays()
fn a_file_the_peer_does_not_hold_is_not_freed()
fn an_unreachable_peer_frees_nothing_and_says_why()
fn a_peer_without_the_feature_frees_nothing_and_says_why()
```
Run: `cargo test --manifest-path android/native/Cargo.toml`. Expected: FAIL.

- [ ] **Step 2: Implement.** Run the crate tests. Expected: PASS.

- [ ] **Step 3: Add scenario `space`,** acceptance 16. Download several 64 MiB files and mark one. Fill `/data` with `dd` in the app-visible shared storage until free space is below the floor (`df`). Run a burst: unmarked files go oldest first, the marked one stays, a file not yet on the PC stays, and freed files read back identically. PC stopped: nothing is freed and the Folders screen says why. Review Focus 4: fill to under 1 MiB free, then a `content write` returns an error and leaves no partial file visible, a receive reports an error, and after removing the filler both succeed and no folder needed repair. Always remove the filler files in cleanup, even on failure.

- [ ] **Step 4: Run.** Run: `python scripts/android_e2e.py --scenario space`. Expected: PASS.

- [ ] **Step 5: Commit** as `feat: keep files on the phone and free the rest when space runs low`.

---

## Part 5: Finish

### Task 20: Freeze, settings, docs and the whole run

**Files:**
- Create: `docs/ANDROID.md`
- Modify: `README.md`, `docs/ARCHITECTURE.md`, `docs/CONFIGURATION.md`, `src/model/errors.rs`, `SettingsScreen.kt`, `scripts/android_e2e.py`, `scripts/build_android.py`

- [ ] **Step 1: Add scenario `freeze`,** acceptance 14. Background the app (`input keyevent KEYCODE_HOME`), `wait_frozen` with a 60 s bound, wait past `DEAD_AFTER` (25 s), then `content read` a file never opened: it opens with correct bytes. Assert the worker's instance id from `status` is unchanged, which is the "no worker restarted" check. Repeat once with a handle open across the freeze, by holding a `content read` through a slow pipe if the tool allows it, otherwise document that this case was not exercised.

- [ ] **Step 2: Add scenario `reinstall`,** Review Focus 3. With a folder, a pairing, a mark and a waiting file: `adb install -r` the same APK. All four are still there, and the waiting file reaches the PC.

- [ ] **Step 3: Finish Settings** against the mockup: mobile data, background check interval, floor, battery, PC row, Diagnostics (Copy; Resync from the start calling `reset-cursors`). Add a JVM test only for logic not already covered.

- [ ] **Step 4: Write `docs/ANDROID.md`:** building with `scripts/build_android.py`, installing with `adb install` (and that a different debug key forces an uninstall), the PC settings (`network.listen` reachable, a build from this branch), what will not work (no real paths, no gallery indexing, no folder creation on the phone, one address per peer), and how to run `scripts/android_e2e.py`. Add a line to the README and to `docs/ARCHITECTURE.md`, and the new error codes to the README table.

- [ ] **Step 5: Run everything, with no mounts.**
Run: `cargo test --lib`, then `cargo test --test embedded --test peer_sync --test orchestrator --test session_core --test session_freeze_fault --test session_handshake --test session_features_fault --test worker_lifecycle --test docs_cli --test desktop`. Expected: PASS. Do not run `tests/winfsp_binding.rs`, or anything else that mounts.
Run: `cargo test --manifest-path android/native/Cargo.toml` and `android\gradlew.bat -p android :app:testDebugUnitTest`. Expected: PASS.
Run: `python scripts/build_android.py`. Expected: both ABIs, page alignment checked.
Run: `python scripts/android_e2e.py --scenario all`. Expected: PASS, with `report.json` listing every scenario, and no process left behind.

- [ ] **Step 6: Commit** as `feat: freeze and reinstall checks, Settings, and Android docs`.

### Task 21: Binary object bodies

Added during execution. Task 11 found that objects travel as JSON number arrays, 3.57 wire bytes per random byte, so read-ahead alone reaches about 8.4 MiB/s on an unlimited link and about 2.7 MiB/s on the emulator's network, against the spec's 10 MiB/s target. It also costs mobile data on every transfer.

**Files:**
- Modify: `src/orchestrator/sync.rs`, `src/orchestrator/worker_client.rs`, `src/orchestrator/network.rs`, `src/orchestrator/push.rs`, `src/orchestrator/sync_driver.rs`, `src/model/protocol.rs`, `docs/ARCHITECTURE.md`, `scripts/android_e2e.py`
- Test: `src/orchestrator/sync.rs`, `src/orchestrator/network.rs`, `tests/peer_sync.rs`

**Interfaces:**
- Produces: `sync::encode_objects(objects: &[Object]) -> Vec<u8>` and `sync::decode_objects(body: &[u8]) -> Result<Vec<Object>>`. Binary layout: magic bytes, then per object digest, kind, u32 length and raw bytes. `decode_objects` also accepts today's JSON, told apart by the first byte.
- Feature `content-binary` in `model::protocol::features`, advertised by both sides of a peer session.

Rules:
- A reply is binary only when the request asked with `binary: true` in its header, so an older server still answers JSON and a client decodes either.
- A request body (`objects.store`, `objects.write`) is binary only toward a peer advertising `content-binary`, or a worker known to take it. An older peer or adopted older worker keeps JSON, and tests cover both.
- Limits are unchanged in meaning: `MAX_BATCH_BYTES` stays, and `MAX_REPLY_JSON` and `OBJECT_JSON_OVERHEAD` keep applying to JSON replies only. A digest is still verified on receipt.

- [ ] **Step 1: Write the failing tests.**
```rust
// sync.rs
fn objects_round_trip_through_the_binary_body()
fn decode_objects_still_reads_json()
fn a_truncated_binary_body_is_an_error_and_never_a_partial_object()
fn a_binary_reply_is_sent_only_when_asked()
// tests/peer_sync.rs
fn binary_objects_move_within_a_small_margin_of_their_size() -> Result<()> // proxy bytes for a random file under 1.1 times its size
fn an_older_peer_still_syncs_with_json_objects() -> Result<()>             // session.features.none
```

- [ ] **Step 2: Run them.** Expected: FAIL.

- [ ] **Step 3: Implement** per the rules. Run: `cargo test --lib`, `cargo test --test peer_sync --test embedded`, `cargo test --release --test embedded`. Expected: PASS.

- [ ] **Step 4: Put `speed` back into `--scenario all`.** Run: `python scripts/android_e2e.py --scenario speed`. Expected: at least 10 MiB/s. If not, report the number and the remaining bottleneck, without lowering the bound, and leave `speed` out of `all`.

- [ ] **Step 5: Commit** as `feat: objects travel as binary between peers that can`.
