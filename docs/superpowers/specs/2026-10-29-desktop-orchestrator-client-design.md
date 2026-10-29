# Desktop GUI as an orchestrator client, one configuration, and "Send to host"

Date: 2026-10-29. Branch: `master` at `8b1628a0`. Status: design, awaiting
approval.

## 1. Intent

The core rework moved folder management, pairing and peer networking into the
orchestrator (`src/orchestrator/`). The Windows desktop GUI (`src/gui.rs`,
`src/gui_adapter.rs`, `src/desktop.rs`, `ui/app.slint`) was not ported. It
still starts folder workers itself, keeps its own folder list in its own
configuration format, and stubs pairing (`probe()` returns
`PAIRING_UNAVAILABLE`, `src/gui.rs:25-29`).

Two user-visible bugs follow:

- **Configuration collision.** The GUI's default configuration is
  `orchestrator.toml` next to the executable (`src/main.rs:160`), written in
  the desktop's format 3 with a `folders` list (`src/desktop.rs:35-62`). The
  CLI's default is `orchestrator.toml` in the current directory
  (`src/main.rs:167-172`). Run `tkfs manage` from the executable's directory
  and it reads the GUI's file and fails with
  `INVALID_CONFIG … unknown field folders`. The reverse is worse: the desktop
  loader accepts format 2 without `deny_unknown_fields`, backs the file up to
  `*.pre-wave6.toml` and rewrites it as format 3, dropping `[network]`,
  `maximum_running` and `[freshness]` (`src/desktop.rs:44-121`).
- **No pairing or peer sync from the GUI.** Workers never network, so folders
  created by the GUI never sync.

The owner wants "a flexible and powerful core that can be configured
relatively easily by a power user, but also an easy default for the desktop
application." The GUI at commit `1521d6b2` was "getting into good shape". It
was an orchestrator client: on first run it initialised the config, started
the orchestrator hidden, and did everything over the control endpoint. This
project restores that shape on the current orchestrator and adds one new
feature, **Send to host**: tell a paired host to receive one of my folders,
without touching that host.

## 2. Goals

1. **One configuration.** The orchestrator's format 2 `orchestrator.toml` is
   the only configuration. The desktop format 3 config, its folder list, and
   the GUI's worker supervision go away. The orchestrator catalog is the
   single list of folders; GUI and `tkfs manage` show and change the same
   list.
2. **The GUI and CLI find the configuration the same way:**
   1. `-f/--config`, then
   2. the `TKFS_CONFIG` environment variable, then
   3. the platform default.

   On Windows the platform default is `orchestrator.toml` next to the
   executable; elsewhere it is `./orchestrator.toml`, as today (§6, D1).
3. **First launch is one click.** `tkfs` with no arguments and no config:
   1. writes the default `orchestrator.toml` (as `Config::initialize` does now);
   2. starts `tkfs -f CONFIG orchestrator` in the background with no window,
      logging to `DATA/logs/orchestrator.log`;
   3. opens the window connected to it.

   Later launches attach to a running orchestrator, or start one if none
   answers. Closing the window leaves the orchestrator and its folders
   running. The host panel has Stop / Start for the orchestrator, as in
   `1521d6b2`.
4. **Restore the `1521d6b2` GUI on the current orchestrator API**
   (`ManagementCommand`, `src/orchestrator/manage.rs:16-116`, served by
   `hub.rs`):
   - folders list and stats;
   - Activity;
   - Peers sidebar and peer details with lanes;
   - host card;
   - New folder;
   - Pair with a host, Browse folders, Receive, Disconnect;
   - Settings with Generate pairing code;
   - folder Details (status, conflicts and resolve, missing files, errors,
     activity);
   - stop-before-remount;
   - Shut down.

   Mockup approved in conversation: the main window plus four dialogs (New
   folder, Send to host, Pair with a host, Settings). It is saved as
   `2026-10-29-desktop-orchestrator-client-mockup.html` next to this document.
   Its short pairing code is a placeholder (see Goal 6).
5. **Create dialog semantics.** An empty mount path means mount at
   `{default_mount_directory}\{label}`; the button reads "Create & mount". A
   **Headless** checkbox creates a headless folder instead. An empty path no
   longer implies headless (the current GUI's behaviour,
   `ui/app.slint:800`). Receive offers the same mounted / headless choice.
6. **Pairing in the GUI.**
   - Settings → "Generate a new pairing code" calls `reset-pairing-code`. The
     code is shown once in full (64 hex characters, `src/pairing_code.rs`)
     with a Copy button, and is cleared when the dialog closes.
   - "Pair with a host" takes an address and a pasted code (whitespace
     trimmed) and calls `pair`.
   - The code format and length do not change: a dialer learns the salt, and
     the verifier is one SHA-256, so only a full-entropy code resists offline
     guessing.
7. **Send to host (new).** On a folder row and in its Details, "Send to host"
   opens a dialog: pick a paired host, choose "Mounted, in its default mount
   folder" (default) or "Headless", and press Send. The remote orchestrator
   receives that repository from this host under this folder's label and
   starts syncing it. The GUI shows the remote's outcome: success, or the
   remote's error code and message verbatim. CLI parity:
   `tkfs manage send FOLDER INSTALLATION [--headless]` (§6, D3).

## 3. Non-goals

- Closing W6-P0 (the mount lifecycle blocker). This project wires mounts
  through the GUI, and the orchestrator keeps refusing them (`refuse_mount`,
  `hub.rs:1439`) until P0 is closed under its own acceptance.
- Adopting an arbitrary existing directory on disk (with files in it) as a
  synced folder. Discussed and set aside: "my local folder" here means a
  folder already in the catalog.
- Approval prompts on the receiving host. Pairing is the trust.
- The sender choosing a mount path on the remote.
- Migrating old desktop format 3 configs or their folder stores.
- Editing `orchestrator.toml` from the GUI.

## 4. Anti-goals (failure even if it "works")

- **The GUI starts workers itself, or mounts outside the orchestrator.** The
  current GUI mounts by running `tkfs --state S daemon --mount M` directly.
  That path is removed: it puts a live mount under a process the GUI may
  kill, which the W6-P0 rules in `AGENTS.md` forbid.
- **A mount request silently becomes headless.** Until P0 closes, "Create &
  mount", a mounted Receive and a mounted Send must fail with a clear message
  built from `MOUNT_UNSUPPORTED`. They must not quietly create a headless
  folder.
- **Any loader rewrites a configuration it did not create.** This includes
  the "backup and upgrade" path.
- **The GUI and CLI resolve different default config paths on the same
  machine.**
- **A shortened or "friendlier" pairing code.**
- **A Send that reports success while the remote cannot actually fetch the
  content** (see §5 and §7).

## 5. Constraints and facts the builder needs

- **Control transport.** The GUI talks to the orchestrator over the same
  control endpoint as `tkfs manage`. It finds it through
  `DATA/orchestrator.json` and authenticates with the
  `DATA/orchestrator.token` header (`src/orchestrator/endpoint.rs`). The
  `1521d6b2` GUI used a `Subscribe` long-poll that no longer exists. Status
  comes from `status` / `watch` (manage.rs).
- **Single instance.** One orchestrator per data directory
  (`DATA/orchestrator.lock`, `ALREADY_RUNNING` exit 75, `hub.rs:375-386`).
  The GUI's spawn must treat `ALREADY_RUNNING` as "attach", not as an error.
- **Bounded timing.** Spawn and attach waits are bounded. A spawned
  orchestrator that never publishes `orchestrator.json` within the bound
  shows as an error with the log path. Never wait forever (AGENTS.md).
- **Folder ops.** Worker ops the GUI uses go through the orchestrator's
  `folder{state, op, payload}` action, which forwards only `FOLDER_OPS`
  (`hub.rs:134`: status, conflicts, resolve, preview-version, events, tree).
  The current GUI also exposes gc, fork, checkout, checkpoint, flush,
  configure, open and log.
  - `open` and `log` are local shell actions.
  - For the rest, the builder either adds them to `FOLDER_OPS` (if the worker
    serves them and they are safe under the orchestrator) or drops them from
    the GUI.
  - Nothing goes to a worker directly.
- **Publishing is implicit.** Every catalog folder is visible to every paired
  peer (`hub.rs:1129`), and `receive` checks the repo against the remote list,
  then creates it (`hub.rs:808-829`). One folder per repo per host
  (`REPO_ALREADY_PRESENT`).
- **Peer ops today.** `catalog.list`, `peer.enroll`, `peer.revoke`, `ping`,
  `clock.probe`, `meta.exchange` and `objects.fetch` are served to peers
  (`network.rs:1013-1140`, policy in `protocol::op_allowed`). Send needs a new
  control-lane op (working name `folder.offer {repo, label, headless}`). It is
  served only to a paired peer (`require_paired`) and runs the local
  `receive` path with the sender's endpoint as recorded in the catalog.
- **Content moves one way per pairing today (prerequisite, §7).** Object
  content moves only by the dialer calling `objects.fetch` on the listener.
  Peers never write objects into each other. The listener never originates
  requests on an inbound session (`network.rs:686-713`, `1070-1144`).
  - The accepting side of a pairing stores only the key (`catalog.rs:429`),
    not an endpoint, so it can never dial back. Metadata flows both ways in
    `meta.exchange`; content does not.
  - This breaks ordinary two-way editing after a one-way pairing. It
    regresses `docs/revamp/REVAMP_PLAN.md` decision 4 (line 1268) and the
    upload path of §6.4 (`objects.wants` / `objects.push`).
  - Send is exactly this case: the remote receives my folder and cannot pull
    its content. Send therefore lands only after the two-way content project
    in §7.
  - `folder.offer` itself travels on my outbound control lane, which the
    sender always has after `pair`.
- **Mount root on the remote.** A mounted Send uses the remote's own
  `default_mount_directory`. The remote's orchestrator refuses it with
  `MOUNT_UNSUPPORTED` until P0 closes. The GUI shows that error as the
  remote's reply.
- **Docs.** `docs/CLI.md:75-80` currently tells users to give the GUI a
  separate file; that text and the `-f` default description
  (`docs/CLI.md:40`) change. The `docs_cli` gate (`tests/docs_cli.rs`) must
  keep passing. The Linux docs and the systemd example already pass `-f` and
  are unaffected.

## 6. Decisions

From the owner:

- One `orchestrator.toml` owned by the orchestrator. No desktop folder list.
- The GUI and CLI resolve the config the same way.
- A config the GUI finds that is an old desktop file (format 3 / `folders`)
  produces a clear error naming the file. It is not migrated or rewritten;
  those were test setups.
- The GUI never edits `orchestrator.toml`. GUI settings go through the
  orchestrator, only where it supports changing them.
- First run and later runs as in Goal 3.
- An empty mount path means the default mount folder plus label; Headless is
  an explicit checkbox.
- Pairing via generated code and pasted code. The code stays 64 hex
  characters.
- Send to host:
  - pairing is the trust;
  - no prompt on the remote;
  - the sender picks mounted (default) or headless;
  - mounted means the remote's default mount folder;
  - the remote's errors are shown as-is;
  - mounts fail clearly until P0, never fall back to headless.
- The approved mockup: `1521d6b2` layout plus the Headless checkbox and Send
  to host.

My calls (flag any you disagree with):

- **D1.** The platform default config path is next to the executable on
  Windows, and `./orchestrator.toml` elsewhere. `/usr/local/bin` is the wrong
  place for a config on Linux, and the GUI exists only on Windows, so GUI and
  CLI still agree on every machine where both run. `TKFS_CONFIG` is new.
  - This assumes the Windows build is a portable folder the user can write
    to, as today; the owner did not answer this.
  - If the exe's directory is not writable, first launch fails with a clear
    error naming the path and suggesting `-f` or `TKFS_CONFIG`. It does not
    fall back silently to another location.
  - Moving the default to `%LOCALAPPDATA%\Telekinesis` is a later decision if
    an installer appears.
- **D2.** The old desktop file is detected by the parse failure on `folders`
  (or `format_version = 3`) and reported as `DESKTOP_CONFIG_UNSUPPORTED:
  <path> was written by an older desktop build; move it aside`. Both the CLI
  and the GUI show it.
- **D3.** The CLI command is `tkfs manage send FOLDER INSTALLATION
  [--headless]`. FOLDER takes whatever other `manage` folder commands take.
  INSTALLATION is the id from `peers`.
- **D4.** `src/desktop.rs` loses its `Config` and worker supervision. What
  remains (if anything) is the orchestrator spawner. The `gui` →
  `Desktop::open` path is replaced by a control client.

## 7. Prerequisite and sequencing

**Owner's decision (2026-10-29).** Content must flow both ways over one
pairing. That is option B from review, chosen over:
- requiring two-way pairing;
- having the accepting side store and dial the advertised address.

The owner's reasoning: the peer transport is WebSocket precisely because it is
full duplex while acting as "half NAT piercing". The connection the dialer
opened should carry content in both directions.

This is its own project with its own design. It covers how the second
direction is driven: the dialer pushes as in REVAMP_PLAN §6.4, or the
listener originates requests over the inbound session. It also covers flow
control, lane fairness and the protocol version gate. This document does not
decide those.

Sequencing (my call):
1. Configuration unification and the GUI as an orchestrator client: Goals 1
   to 6. These do not depend on the prerequisite and can start now. The
   configuration collision is a live bug.
2. Two-way content over one pairing (separate design).
3. Send to host (Goal 7) and `tkfs manage send`, after 2. Until then the GUI
   does not show Send.

The GUI's pair-then-receive flow is not "done" until step 2 lands, because
edits on the receiving host would not reach the sender.

## 8. Left to the builder

- The exact peer op name, its payload and error codes, and its protocol
  version gating.
- Wiring the GUI's view model (`gui_adapter.rs`) onto `status` / `watch`
  snapshots, and the polling cadence.
- How much of `ui/app.slint` to take from `1521d6b2` versus adapting the
  current file.
- The Windows background-spawn details (`CREATE_NO_WINDOW`, log redirection),
  modelled on `1521d6b2:src/gui.rs:113-146`.
- Test strategy: unit tests for config resolution and old-file detection,
  hub tests for `folder.offer` (paired only, `REPO_NOT_PUBLISHED`,
  `REPO_ALREADY_PRESENT`, mount refused, content arriving over a one-way
  pairing once §7 step 2 has landed), and `docs_cli` for the new command. No mounted tests outside an isolated VM (AGENTS.md).
