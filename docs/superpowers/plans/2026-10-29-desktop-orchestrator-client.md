# Desktop GUI as an orchestrator client — Implementation Plan (step 1)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** one `orchestrator.toml` shared by the GUI and the CLI, and a Windows
GUI that runs everything through the orchestrator. The GUI starts or attaches
to the orchestrator and creates, receives, pairs and manages folders over the
control endpoint, never by spawning workers itself.

**Architecture:**
- The config path is resolved once, the same way for the GUI and the CLI.
- An old desktop-format file is refused, never rewritten.
- A new library module `orchestrator::launcher` starts or attaches to the
  orchestrator within a bound.
- `src/desktop.rs` becomes a thin control client (plus pure payload helpers).
- `src/gui.rs`, `src/gui_adapter.rs` and `ui/app.slint` render the
  orchestrator's `status` snapshot and send its actions.

**Tech Stack:**
- Rust 2024
- clap
- serde_json and toml
- Slint `=1.17.0` (Windows only)
- tungstenite control endpoint (`orchestrator::endpoint::ControlClient`)

**Spec:** `docs/superpowers/specs/2026-10-29-desktop-orchestrator-client-design.md`.
The approved mockup is `docs/superpowers/specs/2026-10-29-desktop-orchestrator-client-mockup.html`.
Out of scope: Goal 7 (Send to host) and the two-way content prerequisite (§7).

## Global Constraints

- **Config.** There is one configuration: the format 2 `orchestrator.toml`
  (`src/orchestrator/config.rs`). Nothing writes or rewrites it except
  `Config::initialize` when the file is missing.
- **Config path order.**
  1. `-f/--defaults-file/--config`
  2. `TKFS_CONFIG` (an empty value is ignored)
  3. Platform default: on Windows `orchestrator.toml` next to the executable,
     elsewhere `./orchestrator.toml`.

  There is no silent fallback to any other location.
- **Old desktop file.** It is refused with
  `DESKTOP_CONFIG_UNSUPPORTED: <path> …`, left byte-for-byte untouched.
- **No workers or mounts from the GUI.** The GUI never starts a worker and
  never mounts. Every folder action goes through the orchestrator (`create`,
  `receive`, `start`, `stop`, `pause`, `resume`, `configure-mount`, `folder`,
  `pair`, `unpair`, `peers`, `remote-list`, `reset-pairing-code`,
  `shutdown`).
- **Mount errors are not masked.** A mount request reaches the orchestrator
  as a non-null `mount` and fails with `MOUNT_UNSUPPORTED` until W6-P0
  closes. It is never turned into headless.
- **Pairing code.** The code format stays 64 hex characters
  (`src/pairing_code.rs`), shown in full, once.
- **Bounded waits.** Every wait is bounded: orchestrator start 30 s, kill
  wait 5 s. A process still alive after a kill is reported as
  `STRANDED_PROCESS: pid N`. No test passes `--mount` or creates a mount
  (AGENTS.md).
- **No new crates.** The clipboard uses `clip.exe`. GUI code stays under
  `#[cfg(windows)]`.

## Review Focus

1. **Orchestrator already running.** An orchestrator already runs for this
   config, started from a console or a service. The GUI must attach, not
   start a second one or report `ALREADY_RUNNING`. Pinned by
   `attaches_to_an_orchestrator_started_elsewhere` (Task 4).
2. **Config directory not writable.** The executable's directory, or a
   `-f` path's parent, cannot be written. First launch must fail naming the
   path, not write somewhere else. Pinned by
   `an_unwritable_config_location_is_named_in_the_error` (Task 4).
3. **Stopped behind the GUI's back.** `tkfs manage shutdown` is run while
   the window is open. The GUI must show "Orchestrator stopped" with a Start
   action and not respawn by itself. Pinned by
   `status_after_shutdown_reports_not_running` (Task 5) and
   `a_lost_orchestrator_is_not_restarted_automatically` (Task 6).
4. **Pasted code with whitespace.** The pasted pairing code has leading or
   trailing whitespace or newlines (copied from a terminal). It must pair.
   Pinned by `pairing_code_strips_whitespace` (Task 5).
5. **Unusable label in a default mount path.** A folder label containing
   `..`, a path separator or a Windows-invalid character is used to build
   the default mount path. It must be refused before any request, never
   escape the mount root. Pinned by
   `default_mount_refuses_labels_that_are_not_one_path_segment` (Task 5).

---

### Task 0: Branch

- [ ] **Step 1:** `git switch -c gui-orchestrator-client`.
- [ ] **Step 2:** Commit the spec and mockup already in the working tree:

```bash
git add docs/superpowers/specs/2026-10-29-desktop-orchestrator-client-design.md docs/superpowers/specs/2026-10-29-desktop-orchestrator-client-mockup.html docs/superpowers/plans/2026-10-29-desktop-orchestrator-client.md
git commit -m "docs: desktop GUI as orchestrator client design, mockup and plan"
```

---

### Task 1: One config path resolution for the GUI and the CLI (D1)

**Files:**
- Modify: `src/orchestrator/config.rs` (new fn + tests)
- Modify: `src/main.rs:149-173` (use it for `Gui` and every other command)
- Modify: `docs/CLI.md:40` (the `-f` row), `docs/CONFIGURATION.md` (mention `TKFS_CONFIG`)
- Test: `src/orchestrator/config.rs` `mod tests`

**Interfaces:**
- Produces:
  - `pub const CONFIG_ENV: &str = "TKFS_CONFIG";`
  - `pub fn resolve_path(explicit: Option<&Path>, env: Option<&std::ffi::OsStr>, exe: &Path, cwd: &Path) -> PathBuf`
    - Returns an absolute path. A relative `explicit` or `env` is joined to
      `cwd`.
    - Windows default: `exe.with_file_name("orchestrator.toml")`. Otherwise:
      `cwd.join("orchestrator.toml")`.

- [ ] **Step 1: Write the failing tests** in `config.rs` `mod tests`:

```rust
#[test]
fn resolve_path_prefers_explicit_then_env_then_platform_default() {
    let cwd = Path::new(r"C:\work");
    let exe = Path::new(r"C:\Tools\tkfs\tkfs.exe");
    assert_eq!(resolve_path(Some(Path::new("a.toml")), Some("b.toml".as_ref()), exe, cwd), cwd.join("a.toml"));
    assert_eq!(resolve_path(None, Some("b.toml".as_ref()), exe, cwd), cwd.join("b.toml"));
    assert_eq!(resolve_path(None, Some("".as_ref()), exe, cwd), resolve_path(None, None, exe, cwd));
    #[cfg(windows)]
    assert_eq!(resolve_path(None, None, exe, cwd), PathBuf::from(r"C:\Tools\tkfs\orchestrator.toml"));
    #[cfg(not(windows))]
    assert_eq!(resolve_path(None, None, exe, cwd), cwd.join("orchestrator.toml"));
}
```

- [ ] **Step 2:** `cargo test --lib orchestrator::config::tests::resolve_path` → FAIL (`resolve_path` not found).
- [ ] **Step 3:** Implement `resolve_path` and `CONFIG_ENV`.
- [ ] **Step 4:** In `main.rs`, compute the path once:

  ```rust
  resolve_path(cli.config.as_deref(),
               std::env::var_os(CONFIG_ENV).as_deref(),
               &std::env::current_exe()?,
               &std::env::current_dir()?)
  ```

  Then:
  - Pass it to `gui::run` and to the existing `config_path` uses.
  - Delete the `state.join("desktop.toml")` branch. The GUI ignores `--state`
    from now on.
- [ ] **Step 5:** Update `docs/CLI.md:40`. The default reads: "`TKFS_CONFIG`
  when set, else `orchestrator.toml` next to `tkfs.exe` on Windows and in
  the current directory elsewhere." Add `TKFS_CONFIG` to
  `docs/CONFIGURATION.md`.
- [ ] **Step 6:** `cargo test --lib orchestrator::config` and `cargo test --test docs_cli` → PASS.
- [ ] **Step 7:** Commit: `feat: one config path resolution for GUI and CLI (TKFS_CONFIG, exe-adjacent default on Windows)`.

---

### Task 2: Refuse old desktop configurations (D2)

**Files:**
- Modify: `src/orchestrator/config.rs:65-80` (`Config::load`)
- Test: `src/orchestrator/config.rs` `mod tests`

**Interfaces:**
- Produces: `Config::load` / `Config::load_or_init` fail with
  `DESKTOP_CONFIG_UNSUPPORTED: {path} was written by an older desktop build (format_version 3 with folders); move it aside and tkfs creates a new orchestrator.toml`.

- [ ] **Step 1: Write the failing test:**

```rust
#[test]
fn an_old_desktop_config_is_refused_by_name_and_left_untouched() {
    let dir = tempfile::tempdir().unwrap();
    let body = "format_version = 3\ndata_directory = 'data'\ndefault_mount_directory = 'mounts'\nfolders = []\n";
    let path = write(dir.path(), body);
    for result in [Config::load(&path), Config::load_or_init(&path)] {
        let message = err(result);
        assert!(message.starts_with("DESKTOP_CONFIG_UNSUPPORTED:"), "{message}");
        assert!(message.contains(&path.display().to_string()), "{message}");
    }
    assert_eq!(std::fs::read_to_string(&path).unwrap(), body);
}
```

  Also add the same case with `format_version = 2` plus `folders = []`
  (expect `DESKTOP_CONFIG_UNSUPPORTED`).

- [ ] **Step 2:** Run it → FAIL (today: `INVALID_CONFIG …`).
- [ ] **Step 3:** In `Config::load`, before the typed parse, parse into
  `toml::Table`. If it has a `folders` key or `format_version == 3`, bail
  with the message above. Any other parse failure keeps today's
  `INVALID_CONFIG` message.
- [ ] **Step 4:** Run `cargo test --lib orchestrator::config` → PASS.
- [ ] **Step 5:** Commit: `feat: refuse old desktop configurations with DESKTOP_CONFIG_UNSUPPORTED`.

---

### Task 3: Forward the folder actions the GUI keeps

The design's call on folder ops: **checkpoint, fork, checkout and gc are
forwarded.** `flush` is dropped from the GUI: it serves mounts, and there
are none.

**Files:**
- Modify: `src/orchestrator/hub.rs:134` (`FOLDER_OPS`)
- Modify: any doc listing forwarded ops (`grep -rn "preview-version" docs/`)
- Test: `tests/orchestrator.rs`, in the test that already calls `folder`
  with `status`/`tree` (around lines 118-141)

**Interfaces:**
- Produces: `FOLDER_OPS` = status, conflicts, resolve, preview-version,
  events, tree, checkpoint, fork, checkout, gc.

- [ ] **Step 1: Extend the test:**
  - `request("folder", {"state":"Project","op":"checkpoint","payload":{}})`
    returns an object with a string `"id"`.
  - `fork` with `{"name":"side"}` succeeds.
  - `checkout` with `{"branch":"side"}` succeeds.
  - `gc` returns a numeric `"removed"`.
  - `shutdown` stays `OP_NOT_ALLOWED` (already asserted).
- [ ] **Step 2:** `cargo test --test orchestrator` → FAIL with `OP_NOT_ALLOWED: checkpoint`.
- [ ] **Step 3:** Add the four ops to `FOLDER_OPS`. Update the doc list.
- [ ] **Step 4:** `cargo test --test orchestrator` → PASS.
- [ ] **Step 5:** Commit: `feat: forward checkpoint, fork, checkout and gc to folder workers`.

---

### Task 4: Start or attach to the orchestrator, bounded (`orchestrator::launcher`)

**Files:**
- Create: `src/orchestrator/launcher.rs`
- Modify: `src/orchestrator/mod.rs` (`pub mod launcher;`)
- Replace: `tests/desktop.rs` (the old `Desktop` tests go; this file now
  tests the launcher and the Task 5 client)

**Interfaces:**
- Consumes:
  - `Config::load_or_init`
  - `ControlClient::connect`, `ControlClient::request_read`
  - `hub::EXIT_ALREADY_RUNNING` (75)
- Produces:
  - `pub const START_BOUND: Duration = Duration::from_secs(30);`
  - `pub const KILL_BOUND: Duration = Duration::from_secs(5);`
  - `pub const CONSOLE_LOG: &str = "orchestrator-console.log";` (under
    `DATA/logs/`)
  - `pub enum Started { Attached, Spawned { pid: u32 } }` (derive `Debug`,
    `PartialEq`)
  - `pub fn ensure_running(config_path: &Path, exe: &Path, bound: Duration) -> Result<(Config, ControlClient, Started)>`

Behaviour:
1. `Config::load_or_init(config_path)`. Its errors propagate with the path in
   the message: `CONFIG_WRITE_FAILED: {path}: {error}` when the file cannot
   be created.
2. If `connect` plus `request_read("status")` succeeds, return `Attached`.
3. Otherwise spawn `exe -f <config_path> orchestrator`:
   - stdin null;
   - stdout and stderr appended to `DATA/logs/orchestrator-console.log`
     (create the directory);
   - on Windows, `creation_flags(0x0800_0000)` (`CREATE_NO_WINDOW`).
4. Poll every 50 ms until `connect` plus `status` succeed, then return
   `Spawned { pid }`. Drop the `Child` without waiting: the orchestrator
   outlives the GUI.
5. If the child exits with 75, keep polling `connect` until `bound`, then
   return `Attached`.
6. If the child exits with any other code, fail with
   `ORCHESTRATOR_START_FAILED: exited with {code}; see {console log path}`.
7. If `bound` elapses, kill the child and wait up to `KILL_BOUND`. Fail with
   `ORCHESTRATOR_START_TIMEOUT: no control endpoint within {bound:?}; see {console log path}`,
   with `; STRANDED_PROCESS: pid N` appended if it is still alive.

- [ ] **Step 1: Write the failing tests in the new `tests/desktop.rs`:**
  - **Setup.** Use `mod support;`. Write each home's config as text with
    `[network] listen = "127.0.0.1:0"`, so the default port is never needed.
  - **Cleanup.** A guard sends `shutdown` through a fresh `ControlClient`
    on drop, then waits at most `hub::SHUTDOWN_BOUND` for
    `DATA/orchestrator.json` to be gone.
  - `spawns_then_attaches_then_starts_again_after_shutdown`:
    1. The first `ensure_running(cfg, CARGO_BIN_EXE_tkfs, START_BOUND)` is
       `Spawned{pid}`.
    2. The second is `Attached`, and `status["daemon"]["pid"] == pid`.
    3. `request("shutdown")`, then wait until the discovery file is gone.
    4. The third is `Spawned{pid2}` with `pid2 != pid`.
  - `attaches_to_an_orchestrator_started_elsewhere`: start it with
    `support::daemon::TestDaemon::start_in(home)`. `ensure_running` on its
    config is `Attached`, and `status.daemon.pid == d.pid()`.
  - `a_failing_orchestrator_is_reported_with_its_log`: `data_directory`
    points at an existing regular file. The error starts with
    `ORCHESTRATOR_START_FAILED` and contains `orchestrator-console.log`.
    Assert within `START_BOUND`.
  - `an_unwritable_config_location_is_named_in_the_error`: the config path
    is `<file>/orchestrator.toml`, whose parent is a regular file. The error
    contains that path, and no file is created anywhere else in the
    temporary home.
- [ ] **Step 2:** `cargo test --test desktop` → FAIL (module missing).
- [ ] **Step 3:** Implement `launcher.rs` as specified.
- [ ] **Step 4:** `cargo test --test desktop` → PASS. Then
  `cargo test --test orchestrator` → still PASS.
- [ ] **Step 5:** Commit: `feat: orchestrator launcher that attaches or starts within a bound`.

---

### Task 5: `src/desktop.rs` becomes the orchestrator client (D4)

**Files:**
- Modify: `src/desktop.rs`. Add `Client` and the helpers next to the old
  `Config`, `Folder` and `Desktop` types; the GUI still uses those until
  Task 6 deletes them, so every commit builds.
- Modify: `tests/desktop.rs` (add the client tests)

**Interfaces:**
- Consumes: `launcher::{ensure_running, Started, START_BOUND}`,
  `ControlClient`.
- Produces:
  - `pub struct Client { pub config_path: PathBuf, pub config: Config, client: ControlClient }`
  - `pub fn Client::open(config_path: &Path, exe: &Path) -> Result<(Client, Started)>`
  - `pub fn Client::connect_again(&self) -> Result<Client>`: a second
    client on the same config, with no spawn. Used by the GUI's second
    thread.
  - `pub fn status(&mut self) -> Result<Value>`: `request_read("status")`.
  - `pub fn read(&mut self, action: &str, payload: Value) -> Result<Value>`:
    `request_read`.
  - `pub fn act(&mut self, action: &str, payload: Value) -> Result<Value>`:
    `request`; mutations are never resent.
  - `pub fn folder_op(&mut self, state: &str, op: &str, payload: Value) -> Result<Value>`:
    `act("folder", {"state","op","payload"})`.
  - `pub fn default_mount(dir: &Path, label: &str) -> Result<PathBuf>`.
    - `dir.join(label)`.
    - `INVALID_FOLDER_NAME: …` when the trimmed label is empty, `.`, `..`,
      or contains any of `<>:"/\|?*` or a control character.
  - `pub fn mount_choice(dir: &Path, label: &str, path_text: &str, headless: bool) -> Result<Option<PathBuf>>`:
    - headless gives `None`;
    - blank `path_text` gives `Some(default_mount(dir, label)?)`;
    - anything else gives `Some(PathBuf::from(path_text.trim()))`.
  - `pub fn pairing_code(text: &str) -> String`: removes all whitespace.
  - `pub fn explain(error: &anyhow::Error) -> String`.
    - For a `MOUNT_UNSUPPORTED` error it returns exactly:
      `Mounting isn't available in this build yet. Tick "Headless" to sync this folder without a mount.`
    - Otherwise it returns `format!("{error:#}")`.

- [ ] **Step 1: Write the failing unit tests** in `src/desktop.rs`:

```rust
#[test]
fn mount_choice_defaults_to_the_mount_root_and_headless_means_none() {
    let root = Path::new(r"C:\TK\mounts");
    assert_eq!(mount_choice(root, "Acme", "  ", false).unwrap(), Some(root.join("Acme")));
    assert_eq!(mount_choice(root, "Acme", r"D:\X", false).unwrap(), Some(PathBuf::from(r"D:\X")));
    assert_eq!(mount_choice(root, "Acme", r"D:\X", true).unwrap(), None);
}
#[test]
fn default_mount_refuses_labels_that_are_not_one_path_segment() {
    for label in ["", " ", ".", "..", "a/b", r"a\b", "a:b", "x?", "con\u{7}"] {
        let e = default_mount(Path::new("m"), label).unwrap_err().to_string();
        assert!(e.starts_with("INVALID_FOLDER_NAME"), "{label:?}: {e}");
    }
}
#[test]
fn pairing_code_strips_whitespace() {
    let code = "ab".repeat(32);
    assert_eq!(pairing_code(&format!("  {code}\r\n")), code);
}
#[test]
fn mount_unsupported_is_explained_not_masked() {
    let e = anyhow::anyhow!("MOUNT_UNSUPPORTED: mounts of orchestrator-managed folders arrive later");
    assert!(explain(&e).starts_with("Mounting isn't available in this build yet."));
    assert_eq!(explain(&anyhow::anyhow!("WORKER_LIMIT: 8")), "WORKER_LIMIT: 8");
}
```

- [ ] **Step 2: Write the failing integration tests** in `tests/desktop.rs`:
  - `the_client_creates_a_headless_folder_and_runs_folder_ops`:
    1. `Client::open`.
    2. `act("create", {"label":"Project","mount":null})`.
    3. Poll `status()` (bounded 30 s) until the folder's `phase.phase` is
       `"Running"`.
    4. `folder_op(state,"checkpoint",{})` returns an `"id"`.
  - `a_mount_request_reaches_the_orchestrator_and_is_refused`:
    `act("create", {"label":"M","mount": <tempdir>/m})` fails with
    `MOUNT_UNSUPPORTED`, and `status().folders` is empty.
  - `status_after_shutdown_reports_not_running`: `act("shutdown")`; then
    `status()` on a fresh `connect_again()` is an error containing
    `ORCHESTRATOR_NOT_RUNNING`.
- [ ] **Step 3:** `cargo test --lib desktop` and `cargo test --test desktop` → FAIL.
- [ ] **Step 4:** Implement `Client` and the helpers in `src/desktop.rs` as
  specified. `cargo build` must pass.
- [ ] **Step 5:** `cargo test --lib desktop` and `cargo test --test desktop` → PASS.
- [ ] **Step 6:** Commit: `feat: desktop orchestrator client and dialog helpers`.

---

### Task 6: GUI on the orchestrator (wiring, rendering, dialogs)

**Files:**
- Modify: `src/desktop.rs`. Delete the old `Config`, `Folder` and `Desktop`
  (worker supervision, `blocked.json`) and their tests, so that only
  `Client` and the helpers remain.
- Modify: `src/gui.rs` (threads, message handling, action table)
- Modify: `src/gui_adapter.rs` (render from the `status` snapshot; delete
  `probe`, the listener/advertise settings, attach/detach, flush,
  shutdown-all and per-folder pair)
- Modify: `ui/app.slint`:
  - create and receive dialogs;
  - pairing code with Copy;
  - host dialog;
  - remove the `save-listener` callback and the per-folder
    listener/advertise fields.
- Modify: `docs/CLI.md:75-82` (the `gui` paragraph)
- Test: `src/gui.rs` `#[cfg(test)] mod tests` (Windows bin tests:
  `cargo test --bin tkfs`)

**Interfaces:**
- Consumes:
  - `desktop::Client::{open, connect_again, status, read, act, folder_op}`
  - `desktop::{mount_choice, pairing_code, explain}`
  - `launcher::Started`
  - `catalog::FolderRecord` fields in `status.folders[i].record`: `id`,
    `label`, `state`, `repo`, `mount`, `desired_running`, `paused`
  - `status.folders[i]`: `phase.phase`, `conflicts`, `error`, `sync`
  - `status.daemon`: `installation`, `version`, `listen`, `config`,
    `data_directory`, `stopping`
  - `status.peers[i]`: `endpoint`, `installation`, `direction`, `lanes`,
    `inbound`, `clock`
- Produces, in `ui/app.slint`:
  - `callback create-folder(string /*name*/, string /*mount*/, bool /*headless*/)`
  - `callback receive-folder(string /*endpoint*/, string /*repo*/, string /*label*/, string /*mount*/, bool /*headless*/)`
  - `callback copy-code()`
  - `callback start-orchestrator()`
  - `in property <string> default-mount-directory`
  - `in-out property <bool> headless`

**Threads.** Two background threads, each with its own `desktop::Client`:
- **Status thread.** It owns the instance from `open`, and polls `status()`
  every 1 s. For the selected folder it also reads:
  - `inspect` (`{"state"}`);
  - `folder` ops `conflicts` and `events`, only while its phase is Running.

  It posts the combined JSON to the UI with `upgrade_in_event_loop`.
- **Action thread.** It owns `connect_again()` and runs `Message`s one at a
  time.
- **Orchestrator lost.** Both treat `ORCHESTRATOR_NOT_RUNNING` and
  `CONNECTION_LOST` as "orchestrator stopped": banner "Orchestrator
  stopped", banner action `start-orchestrator`. Neither starts it by itself.
- **Starting it again.** `start-orchestrator` calls
  `Client::open(config_path, current_exe)` again, on the action thread.

**Action table.** `operation` maps to an orchestrator request; every failure
shows `explain(&error)`.

| GUI operation | Request |
|---|---|
| create | `act("create", {"label", "mount": mount_choice(config.default_mount_directory, label, mount, headless)?})` |
| remote-list | `read("remote-list", {"endpoint"})` |
| receive | `act("receive", {"endpoint", "repo", "label", "mount": mount_choice(…)?})` |
| start / stop / pause / resume | `act(op, {"state"})` |
| restart | `stop` then `start` |
| configure-mount | `act("configure-mount", {"state", "mount": blank → null, else the path})` |
| resolve / preview-version / fork / checkout / checkpoint / gc | `folder_op(state, op, …)` (payloads as today in `gui.rs`) |
| pair | `act("pair", {"endpoint", "code": pairing_code(code)})` |
| reset-code | `act("reset-pairing-code", {})`; puts `code` in `pairing-code` |
| unpair | `act("unpair", {"installation"})` (from the peer row) |
| shutdown | `act("shutdown", {})` |
| open | `explorer.exe` on `record.mount`; `FOLDER_NOT_MOUNTED` when null |
| folder log | `notepad.exe` on `STATE/worker.log` (`process::LOG_FILE`) |
| host Log | `notepad.exe` on `DATA/logs/orchestrator.log` |
| copy-code | `clip.exe` with the code on stdin, `CREATE_NO_WINDOW` |

**Copy and rendering** (the approved mockup):
- **Create dialog.**
  - Mount field placeholder: `default-mount-directory + "\\" + folder-name + " (default)"`.
  - **Headless** checkbox with the note "Sync without a mount."
  - Primary button: "Create & mount", or "Create" when headless.
  - Remove "Leave empty to start syncing without a filesystem mount."
- **Receive dialog.** The same Headless checkbox and mount field.
- **Host dialog.**
  - Shows `Config: {status.daemon.config}` and `Listening {listen or "not listening"}`.
  - "Generate a new pairing code" shows the full code in a read-only field,
    with a **Copy** button.
  - Keeps the existing "Shown once…" and closing-the-window texts.
  - "Shut down the daemon" becomes "Stop orchestrator".
- **Host card.** Orchestrator status chip, version, listen, installation
  (first 8 characters).
- **Peers sidebar.** From `status.peers`.

- [ ] **Step 1: Write the failing bin tests** (in `src/gui.rs` tests, over
  fixture JSON shaped like `hub::Daemon::snapshot`):
  - `folder_rows_come_from_the_status_snapshot`:
    - a Running headless folder renders label, a "Headless" mount chip and
      a running status;
    - a paused one shows paused;
    - a `Failed` one shows its `error` text.
  - `peer_rows_come_from_the_status_snapshot`: one outbound peer with
    `endpoint` and two lanes renders its endpoint and its lane count.
  - `a_lost_orchestrator_is_not_restarted_automatically`: model it as a pure
    function

    ```rust
    fn on_status_error(error: &anyhow::Error) -> (String /*banner*/, &'static str /*action*/)
    ```

    For `ORCHESTRATOR_NOT_RUNNING: …` it returns
    `("Orchestrator stopped", "start-orchestrator")`. The status loop calls
    nothing else on error: assert by code review in Step 5.
- [ ] **Step 2:** `cargo test --bin tkfs` → FAIL (functions or fields missing).
- [ ] **Step 3:** Implement the slint changes, then `gui_adapter.rs`
  rendering, then `gui.rs` threads and the action table. Then delete the
  old `Desktop`, `Config` and `Folder` from `src/desktop.rs`, along with
  `probe` and every reference to them.
- [ ] **Step 4:** Run:
  - `cargo test --bin tkfs` → PASS;
  - `cargo build --release` → PASS;
  - `cargo clippy --all-targets -- -D warnings` → PASS.
- [ ] **Step 5:** Read the status loop and confirm that its only reaction to
  an error is the banner. It must not call `Client::open` or spawn
  anything.
- [ ] **Step 6:** Update the `gui` paragraph in `docs/CLI.md`:
  - The GUI uses the same `orchestrator.toml`.
  - It starts the orchestrator when none answers and attaches otherwise.
  - Closing the window leaves it running.

  Delete the "give it a file of its own" advice and the
  `tkfs -f desktop.toml gui` example. Run `cargo test --test docs_cli` →
  PASS.
- [ ] **Step 7:** Commit: `feat: GUI runs on the orchestrator; drop the desktop config and worker supervision`.

---

### Task 7: Whole-suite and manual verification (no mounts)

- [ ] **Step 1:** `cargo test` (all targets) → PASS. Report any failure
  verbatim; do not skip tests.
- [ ] **Step 2: Manual, on this machine.**
  1. Copy `target\release\tkfs.exe` to a new empty folder and run it with no
     arguments. Expect:
     - `orchestrator.toml`, `data\` and `mounts\` are created next to it;
     - the window shows "Running";
     - `data\logs\orchestrator-console.log` exists.
  2. **New folder → Headless → Create.** The row reaches Running.
  3. **New folder, Headless unticked, empty path.** The error reads
     "Mounting isn't available in this build yet…", and no folder is added.
  4. **Settings → Generate.** A 64-character code is shown, and Copy puts it
     on the clipboard.
  5. Close the window. The orchestrator process is still running.
  6. Reopen. The window attaches with the same pid (host card or
     `tkfs manage status`).
  7. Run `tkfs manage status` from that folder. It works, and there is no
     `INVALID_CONFIG`.
  8. Run `tkfs manage shutdown`. The window shows "Orchestrator stopped"
     with Start. Click Start: it is Running again.
- [ ] **Step 3:** Record the results in the PR description: each check with
  its observed result.
