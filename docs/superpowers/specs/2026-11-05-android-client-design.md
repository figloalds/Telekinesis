# Android client

Status: approved by the owner, 2026-11-05. The owner asked to build and test without a phone first
and connect their phone at a later step, so acceptance 19 and 20 are deferred to that step.

Builds on [two-way content](2026-11-04-two-way-content-design.md) and replaces the
throwaway spike on branch `spike/android`. Nothing from the spike is kept unless this
document says so.

## Why

The owner wants a folder that feels shared between PC and phone, with no NAS to buy or
set up. In the owner's words: "access documents directly, or use as a transfer buffer",
"using an app that is on one device but the file is in another", with "the strong
offline-first approach so that already synched files can remain accessible".

It is for the owner's own phone first ("just me for now"). The owner expects to keep
using it if it works and to fix whatever bothers them, so version one should be small,
honest and easy to change, not exhaustive.

## What the spike proved, and what it left

Proven on an Android 16 emulator, not on a real phone: the core compiles with rustls;
the orchestrator and workers run on threads of one app process; pairing, receive and
metadata sync work; the system picker, Files app and Google Photos read through a
`DocumentsProvider`; cold start and offline reads of cached files work.

Findings this design must handle:

- Android freezes a cached app about 10 s after it goes to the background. After a thaw,
  liveness reports `HEARTBEAT_LOST`, and the spike recovers by restarting the worker.
- First reads ran at about 0.9 MiB/s against about 100 MiB/s cached. The cause was not
  established. `Scratch::read` asks for only the chunks a read touches, and each 64 KiB
  chunk then goes through a demand wake-up, a network fetch and an fsynced worker write.
  So missing read-ahead is the likely cause, but this is unproven.
- Nothing frees downloaded bytes.
- Peer lanes could sit in backoff for a minute or more. `sync-now` now clears that.
- Writing from the phone was never built.
- Feed cursors live in memory and reset on every new control session or restarted worker
  (`sync_driver.rs`). A freeze ends the session, because a peer counts as dead after 20 s of
  silence. So nearly every burst would replay the folder's whole event log both ways, 256
  events a page. The spike was too short-lived to show this.

Since the spike, two-way content added `sync_profile = "passive"` and `sync-now`, which
replace the spike's `BACKGROUND_PREFETCH` switch.

## Goals

1. The PC's folders appear on the phone as a storage location that the Files app, file
   pickers and SAF-aware apps can use, reading and writing.
2. The phone lists everything in a folder even with no connection, and downloads a file's
   contents when it is first opened.
3. Files already on the phone open offline. New and edited files can be made offline and
   reach the PC at the next chance, with their bytes.
4. Files stay on the phone until space runs low. Any file or folder can be marked to
   always keep. Bytes are freed only when the PC is known to hold them.
5. It is battery-friendly wherever it can be. Heavy work happens while the app is in use
   or a transfer was asked for, with a light background check in between.
6. Conflicts stay as they are everywhere else: both versions kept, and the owner chooses.

## Non-goals

- FUSE, root, or real file paths. Apps that need a path will not work, and a gallery app
  will not index these images automatically.
- An always-on service, or the PC waking the phone.
- A relay or NAT traversal. It works on the same Wi-Fi, a VPN the owner already uses, or
  a `wss://` proxy. One endpoint per peer: no automatic switching between addresses.
- The Play Store, release signing, multiple users.
- Creating a new shared folder on the phone. Folders start on the PC and are received.
  Send to host remains its own project.
- Automatic camera upload, or two-way sync with a folder in the phone's own storage.
- Search and "recent files" in the provider, branches, and checkpoints.
- Host filesystem mounts of any kind.

## Anti-goals

These would make it a failure even if it technically worked:

- Bytes are freed that no other device holds, or a local edit is lost.
- A file is reported as sent to the PC before it is durable there.
- Browsing or listing downloads file contents. A folder of photos must not fill the phone
  or spend data because someone looked at it.
- The app quietly drains the battery, or spends mobile data on background transfers
  without having been allowed to.
- The phone appears to work while nothing is syncing. If it cannot reach the PC, or has
  unsent changes, the app says so.
- A freeze or kill corrupts a folder, or leaves the app stuck until it is reinstalled.

## Constraints

- Follow `AGENTS.md`: no host mounts, W6-P0 stays open, all test waits are bounded, and a
  stranded process is a failure. End-to-end tests use a headless orchestrator on the PC.
- Desktop behaviour does not change, except for the additions named here. Android-only
  dependencies stay behind `cfg(target_os = "android")`.
- Keep orchestration ([restoration design](2026-10-28-orchestration-restoration-design.md)):
  the phone runs the normal orchestrator and worker code on threads, not a redesign.
- Android builds use rustls with bundled roots, as in the spike, because OpenSSL does not
  cross-build from Windows. Private certificate authorities are not supported.
- Debug signing, installed with `adb install`. Google's developer verification has been
  enforced in Brazil on certified devices since 2026-09-30, and ADB remains a permitted
  way to install an unregistered app. Keep the same debug keystore: a different key forces
  an uninstall, which deletes unsent edits.
- `targetSdk` stays below 37. Apps targeting 37 or above must request
  `ACCESS_LOCAL_NETWORK` at run time, which also covers native sockets. Below that the
  `INTERNET` permission still grants it. `minSdk` stays 26 for `openProxyFileDescriptor`.
- The arm64 library must be 16 KiB page aligned, or it may not load on a 16 KiB device.
- The PC must run a build from this branch, because freeing, "waiting to send" and durable
  cursors need its new peer features, and it must listen on a reachable address. The default
  `network.listen` is loopback only.

## Design

### Shape

```
Files app / pickers / other apps            The app's own screens, share sheet
            │ content:// URIs                       │
     TkfsProvider (Kotlin) ───────── Native ────────┘
            │                          │ JNI
            └──────────────── tkfs-android (Rust) ──── Engine (per folder)
                                       │                    │
                               Daemon::execute        sync-now / demand
                                       │
                              orchestrator + workers on threads ──ws──► PC
```

One app process. Kotlin holds the Android parts. A small Rust crate at `android/native`
holds the JNI surface, the thread launcher and the keep/free bookkeeping. Core changes are
the ones listed under "Changes to the core".

### From the spike

| Keep and clean up | Drop or replace |
|---|---|
| `android/` project layout and Gradle wrapper | `BACKGROUND_PREFETCH`: use `sync_profile = "passive"` |
| Core: `run_embedded`, `Launcher` injection, `EmbeddedWorker`, `Options::on_ready`, TLS `cfg`s | The name "spike" in code, package and comments |
| `tkfs-android`'s thread launcher | The form-style `MainActivity` |
| Provider shape and on-demand start | `testclient`: `adb shell content` reaches the provider from outside the app, through a debug-only path permission the shell holds |
| `scripts/build_android.py` (already committed) | `android/build-native.ps1` |

Work happens on a new branch, `android-client`, cut from `two-way-content`. The Android
project gets tracked properly: `android/` is currently listed in `.git/info/exclude`, which
must be removed in favour of an `android/.gitignore` that excludes build output and
`jniLibs`.

The package becomes `dev.tkfs`, and the app is labelled "Telekinesis".

### Process and lifecycle

Android starts the process when something asks for it: the app, a provider request or a
background job. Each path calls the same `ensureStarted`, which starts the orchestrator
once and returns immediately. Native calls wait, bounded, for what they need.

Configuration written on first run:

```toml
format_version = 2
[network]
listen = ""              # the phone only dials
advertise = ""
sync_profile = "passive"
```

The embedded orchestrator serves no control endpoint. The app calls `Daemon::execute`
directly, so nothing needs a socket that other apps on the phone could try to reach.

**A thaw is not a failure.** After a freeze, every session's last-activity time looks old, so
a loopback session calls its other end dead, and the worker's liveness probe then fails too. The
target is that a thaw restarts nothing, so handles stay valid and the app feels instant. A worker that has
really died is still detected and restarted, and read handles still reopen by path, because
Android can kill the process at any time.

### Files and the provider

Document ids stay `FOLDER_ID:/path`, as in the spike. The provider adds these to it:

- **Writing.** `openDocument` accepts write modes and maps `onWrite`, `onFsync` and
  `onRelease` to `Engine::write`, `flush`, `cleanup` and `close`. `t` modes truncate. Writes
  never go through a raw scratch file descriptor, because that would bypass the dirty
  tracking.
- **Namespace.** Create, delete, rename and move within a folder, with the matching
  document and root flags. Moving across folders falls back to Android's copy and delete.
- **A closed write is durable.** A writable handle's release seals it (`Engine::flush` with
  its token) and fsyncs before release returns, and then asks for a burst. The sealer thread
  may be frozen moments later, so this cannot be left to it. If the process dies first,
  `seal::recover` picks the draft up at the next start. A draft whose seal needs what is not on the
  phone never keeps the folder from opening: it stays on disk, the sealer asks for what it needs and
  tries again, and every other file goes on as usual.
- **Change notifications.** Cursors set a notification URI, and the provider calls
  `notifyChange` when a burst moves events or local files change, so an open Files window
  updates.
- **Not downloaded yet.** `COLUMN_SUMMARY` shows "On this phone" or "Not downloaded".
  Thumbnails are offered only for files already on the phone, so browsing downloads nothing.
- **Writing a file the phone lacks.** `w`, `wt` and `rwt` need nothing of the old file, not even
  its manifest, so they work with no network, before and after a restart. They change nothing until
  the first write, fsync or close. `rw` and `wa` keep what they do not write: writing part of a chunk
  that is not here, or past a short last chunk that is not here, fails with an I/O error and changes
  nothing, and with the manifest not here either, the open itself fails as a read's does. Rename, move
  and delete need none of a file's contents.
- **A replacement cut short.** Two deaths differ here. If the client dies, Android calls `onRelease`
  as it does for a close, and the provider cannot tell them apart: the file is cut, and what the client
  wrote becomes the file. If the app is killed, no release runs. Once `w`, `wt` or `rwt` has cut a file
  that had content, the draft's record says `replacing` until its writer closes, and a draft found
  with that has no proof its writer finished. So the next start publishes it beside the file, as
  `name (recovered).ext` with the first free number on a clash, and the file stays as it was. A draft
  with nothing written is dropped. The new file syncs like any other and counts as waiting to send.
  Until the writer closes, only its own fsync seals it: a flush of everything or a worker's stop passes
  over it, leaving it for that start. A new or empty file has nothing to lose and recovers under its
  own name, as does anything written by a client that fsynced and closed.
- **Opening a file the phone lacks.** The open clears lane backoff, so a PC that just came
  back is tried at once. With no reachable peer it fails within 5 s rather than the spike's
  20 s, and the app shows why, since a viewer's own error message will not.

### First-read speed

First find the cause by timing one read end to end. The expected fix is read-ahead: when a
file that is not fully on the phone is opened for reading, its upcoming chunks are added to
demand so the pump fetches them in batches, with an fsync per batch and not per chunk. Reads
still go first, and read-ahead stops when the handle closes.

Target: at least 10 MiB/s for a sequential first read over home Wi-Fi. Reading part of a
large file must not download all of it, so read-ahead is a window that follows the reader.

This must also deal with the open two-way issue "slow peers locked out". With one PC, a read is
capped at `DEMAND_TRY` (3 s) only once a failure has put that PC at rest. On a slow link that can
then repeat: each capped fetch times out and counts as another failure. A phone with one PC must
not be locked out of its only source.

### When syncing happens

Everything uses `sync-now`. Nothing in the app polls while idle.

| Trigger | What runs |
|---|---|
| The app comes to the foreground (at most once per 30 s) | Burst |
| A pull-to-refresh or "Sync now" | Burst |
| A file written, created, renamed or deleted, or shared in | Burst, debounced 5 s, in the transfer job below |
| A provider listing while the last sync is older than 2 min | Burst in the background, and the listing is answered at once from what is known |
| Periodic WorkManager job, default every 30 min, needing a network | One burst |

A background job runs one burst, bounded by `SYNC_NOW_BOUND`. If it reports `remaining` work
that does not fit, the rest waits for a transfer job or the next trigger.

**Transfer job.** Sending something the owner just made, or keeping files they marked, runs
as expedited WorkManager work with a foreground notification showing progress. It repeats
bursts until `remaining` reaches zero, the job is stopped, or a burst makes no progress. It
declares type `dataSync`, so Android's limit of 6 hours per 24 applies, and it stops cleanly
when `onTimeout` fires. The app asks once for the battery-optimization exemption and works
without it, only later.

**Mobile data.** One setting, "Sync on mobile data", off by default. When it is off and the
network is metered, the foreground, listing and periodic triggers run metadata-only bursts, so
listings stay fresh and no file bytes move. Transfer jobs wait for an unmetered network. Opening
a file always works, because the owner asked for that file, and "Sync now" runs a full burst for
the same reason. Whether the connection counts as metered is Android's call. `sync-now` gains a
`metadata_only` option for this.

A network can turn metered while files are moving. So while a burst or the transfer job runs, the
app watches the default network. When it turns metered with the setting off, or the setting goes off
on a metered network, the app calls `end-bursts`, drops full bursts waiting their turn, and puts a
running job back to waiting for an unmetered network. Only "Sync now" goes on. The job takes its current
network requirement again whenever the setting changes and when the app starts.

### Keeping a burst light

A background check is only light if it does not replay the log, so a passive dialer's feed
cursors must survive sessions and process restarts. An unchanged folder then costs about one
request per burst, however long its history is.

- Cursors are saved per (peer installation, repo), not per endpoint.
- `received` is saved only after the events it covers are durable on the phone.
- `sent` is saved only after the listener has made those events durable. Applied events are not
  fsynced before the `meta.exchange` reply today, which is harmless while everything is replayed
  but would lose events once it is not. A passive dialer asks for a durable apply, and the
  listener runs `sync_metadata` before replying whenever the request carried events or the reply
  hands out events, since a sequence number it hands out could otherwise roll back. The dialer also
  durably re-applies its own page when the reply left out the echoes.
- Replies carry the identity of that store's journal and its top sequence, read before the
  request's events apply so a rollback cannot hide behind them. If the identity differs, or the top
  is below the cursor on either side, the phone starts over from zero, which is what happens on
  every session today.
- Settings has "Resync from the start" for what that cannot detect, such as a PC restored from an
  old backup that has since grown past the cursor.
- Active dialers keep today's behaviour, so the desktop is not affected.

### Confirming what the PC holds

Freeing bytes and saying a file was sent both need the same answer, so there is one peer
operation for it: **`objects.have`** `{repo, digests}`, returning `{held: [digest]}`. It runs on a
bulk lane, gated by a new `content-have` feature, with the same `require_paired` and folder lookup
as `objects.fetch`. The listener flushes its packs before replying, so a digest in `held` is
durable. Requests are paged, since a 1 GiB file has about 16,000 chunks.

An older PC cannot confirm anything: files stay waiting, nothing is freed, and the app says to
update the PC.

### What "waiting to send" means

The Folders screen must be honest with the PC off, so this is durable and local.

- `tkfs-android` keeps a table of unconfirmed files per folder, filled by scanning this device's
  own events in the journal, including renames and deletes.
- After a burst, a file leaves the table when a peer has both received its latest event (`sent`
  has passed it) and, for content, `objects.have` confirmed every chunk of its head.
- One confirming peer is enough. Diagnostics shows the per-peer detail.
- A file edited again goes back in the table by its new event.
- Waiting is the usual state of a file, and this is asked after every burst, so the question is kept
  small: up to 64 digests spread through the file, and full pages only if all of those are held. On a
  metered network a file the PC last said it lacks is not asked about again until something was pushed.

The burst's `remaining` is not used for this. It counts what the listener lacks from anyone,
including bytes the phone does not hold either, so it may never reach zero.

### Keeping and freeing space

**Keep on this phone.** A file or folder can be marked in the app. A marked folder covers
what later arrives in it. Marked files download during transfer jobs, and the mark follows the
file by entity id through renames. A small SQLite database inside `tkfs-android` holds the marks
and each file's last-opened time, separate from the core's store.

**Freeing.** When free space on the volume falls below `max(2 GiB, 10% of the volume)`, or on
"Free up space now", the app frees the least recently opened files first, until the floor is
met. It never frees:

- a marked file;
- a file with an open handle, or a draft or scratch based on it;
- a conflict version the owner has not resolved;
- manifests or trees (they are small, and manifests let a file report what it is missing);
- **anything no peer has just confirmed holding.**

Freeing happens only for digests a peer named in an `objects.have` reply, and only just after it.
This needs a reachable PC. Offline and low on space, nothing is freed and the app says why. To
avoid arriving there, the check also runs at the end of a burst whenever the floor is close.

A phone can stay below the floor because of other apps, for as long as they like, and without
care that would empty its downloads over and over, each to be downloaded again when next opened. So
freeing that nobody just asked for also leaves any file opened in the last 24 hours, and below the
floor it runs at most once an hour, unless free space moved by more than 100 MiB. "Free up space now"
and "Remove download" still take what they are asked to. Whenever free space stays below the floor
after freeing, the Folders screen has a "Low on space" strip.

The space itself comes back through `PackStore::collect` with the live set minus what is being
freed, since packs are 64 MiB segments and freed objects only leave when a segment is rewritten.
Rewriting needs some free space, which the floor provides.

"Remove download" and "Delete" are separate actions with clear wording: the first frees this
phone's bytes, and the second deletes the file everywhere.

### Sharing in

The app is a share target (`ACTION_SEND`, `ACTION_SEND_MULTIPLE`): pick a folder and subfolder,
and a foreground service copies the files in, followed by a transfer job. This is the transfer buffer's main path: from a
photo, Share, then Telekinesis, and it is on the PC at the next chance. Name clashes get a
numbered name rather than overwriting.

### Screens

Compose with Material 3, kept plain. See the
[mockup](2026-11-05-android-client-mockup.html).

- **Folders (home).** Each received folder with its state in words: "Synced 2 min ago",
  "3 files waiting to send", "PC not reachable since 14:02", "2 conflicts", or the error. Also a
  "Sync now" action and space used.
- **Browse.** A folder's files, showing what is on the phone. Tap opens it in another app
  (`ACTION_VIEW` with a provider URI). A long press offers Keep on this phone, Remove download,
  Share, Rename and Delete. This screen exists because the Files app has no place for those
  actions, and some phone makers hide it.
- **Add a PC.** Enter the endpoint and pairing code (see "Pairing"), then pick from the PC's
  published folders (`remote-list`) and receive them, followed by a burst.
- **Conflicts.** Each conflicted file, its versions with device, time and size, the same
  `preview-version` as the desktop, and "Keep this one".
- **Settings.** Mobile data, background check interval, free space floor, battery exemption,
  paired PCs with Change address and Unpair, a copyable diagnostics view (status, peers, recent
  errors), and Resync from the start.

### Pairing

Pairing codes stay 64 hex characters, and typing that on a phone is the worst moment of first
use. **Open question for the owner.**

What holds either way: the PC stores only a verifier of its code, so a code exists to read only
right after `reset-pairing-code` creates it, and creating one revokes every host that dials this
PC. This design adds no second kind of code, because that would be a new way to authenticate.

My recommendation is a QR code on that same moment:

- The phone's Add a PC screen scans a QR, using Google's code scanner, which needs no camera
  permission. The fields stay available for pasting.
- The PC shows a QR for `tkfs://pair?endpoint=…&code=…` where it shows a new code today: the
  desktop GUI's pairing dialog and `tkfs manage reset-pairing-code --qr`.

The alternative is pasting only, which adds nothing on the PC, and the owner moves the code across
however they like.

**Changing address.** Peers are keyed by endpoint, and pairing at a new address needs a code, which
revokes everything. So a PC whose address changes would stop the phone syncing and cost a re-pair.
Settings gets "Change address", backed by a new local action, `manage set-endpoint INSTALLATION
ENDPOINT`. It replaces that peer's endpoint and reopens its lanes with the existing credential,
which also proves the PC at the new address is the same one.

### Changes to the core

Small, and desktop-tested where they can be, except that durable cursors need care:

- Bring the spike's embedded support across, renamed from "Android spike" to what it is.
- The embedded orchestrator skips its control endpoint and discovery file.
- Durable feed cursors for a passive dialer, durable apply on request, and journal identity in
  `meta.exchange` replies.
- `sync-now` with `metadata_only`.
- `objects.have` and the `content-have` feature.
- `manage set-endpoint`, and `--qr` if the owner chooses QR.
- An evict entry point: `collect` with a live set minus named digests, refusing anything pinned.
- A hydrate entry point for keeping files, and read-ahead on demand.
- A presence query: how much of a file is local.
- A stalled session I/O thread does not call its peer dead, so a thaw restarts nothing.

### Documentation

`docs/ANDROID.md`: building, installing with `adb`, the PC settings it needs, and what will not
work (no real paths, no gallery indexing). Add a line to the README and to `docs/ARCHITECTURE.md`.
New error codes go in `model::errors` and the README table.

## Acceptance

All waits bounded. The PC side is a headless orchestrator, with no mounts. "Emulator" means the
existing `Medium_Phone_API_36.0` AVD, reaching the PC at `10.0.2.2`.

**Core, on the desktop test suite**

1. `objects.have` names only what is durably held, and is refused for an unpaired peer or an
   unpublished repo.
2. After eviction, a freed file reads back identically once the peer supplies it. A file with no
   confirming peer is not freed. A pinned object, an open handle, a draft's basis and an unresolved
   conflict version all survive.
3. An embedded worker survives a simulated freeze with no restart, and a killed one is still
   detected.
4. Sequential first read through the demand path meets the target over a fast link, and a partial
   read does not fetch the whole file.
5. With a 10,000-event folder and no changes, a burst after a new session and after a restart moves
   under 64 KiB of feed traffic.
6. Killing the listener right after an exchange reply loses no event a passive dialer counted as
   sent. A listener that lost its journal, or whose top is below the cursor, gets a full exchange.
7. A `metadata_only` burst moves events and no objects.
8. `set-endpoint` moves a peer to a new address with no code, and a different installation at that
   address is refused.
9. Existing suites still pass, including `tests/peer_sync.rs` and `tests/orchestrator.rs`.

**App, on the emulator against a real PC orchestrator, driven from outside the app with `adb`**

10. Pair, receive, browse in the Files app, and open a file. Bytes match.
11. Create, edit, rename and delete through the provider, with the PC showing each, bytes included.
12. Share a file in while the PC is off. The app counts it as waiting, across a force-stop too. It
    reaches the PC after the PC returns, with no action taken on the phone beyond a trigger from the
    table, and only then stops counting as waiting.
13. Force-stop the app during a large write and during a large receive. Nothing corrupts, and the
    transfer finishes after a restart.
14. Background the app until it freezes, then open a new file from the Files app. It opens, and no
    worker restarted.
15. Listing a folder of images, including thumbnails in the Files app, transfers no file contents.
16. Fill the emulator's storage to below the floor. Unmarked files are freed oldest first, marked
    ones stay, a file not yet on the PC stays, and the PC-off case says why.
17. With a metered network and the setting off, opening the app syncs listings and moves no file
    bytes, a waiting file stays waiting, and an opened file still downloads.
18. Conflicting edits on both sides appear on the phone, and choosing one from the phone settles
    it on both.

**On the owner's own phone**

19. The same flow over real Wi-Fi: pair, open a photo in a viewer, share a photo in, and see it on
    the PC.
20. A day of ordinary use: note battery attribution and data used, with no bound set yet. If it
    bothers the owner it becomes the next fix, per how the owner expects to work.

## Order of work

Each step leaves something the owner can install and use:

1. **Read side, real app:** embedded core, real screens, pairing, change address, bursts, thaw
   handling, durable cursors, metadata-only bursts.
2. **First-read speed.**
3. **Write side:** provider writes, share target, transfer job, `objects.have`, waiting to send,
   conflicts. This is the point where it is a working transfer buffer.
4. **Space:** keep on this phone, freeing.
5. **Owner's phone:** acceptance 19 and 20.

## Settled after approval

The owner approved the design without choosing on the two open questions, so:

1. **Pairing:** QR as recommended, with the fields kept for pasting.
2. **History:** left alone for now. Commit `c21d0bfb` added about 60 MiB of Android build output to
   `two-way-content` by accident, and `cc2ee487` removed it from the tip only. Rewriting needs the
   owner's explicit OK, and it must be dealt with before merging to `master`.

Three details changed while planning, because the code allowed something simpler or the first idea
would not have worked:

- **Thaw handling moved to session I/O.** The worker's liveness probe in `folder.rs` already recovers
  when its `hello` succeeds. What fails after a thaw is the loopback session, whose `dead_after` check
  sees old activity, so that is what gets fixed.
- **No Android path for `queue_sealed`.** It only feeds the mount invalidator. A passive phone's
  changes come from bursts and provider writes, and both already call `notifyChange`.
- **Sharing in copies through a foreground service,** not WorkManager, because the URI grant travels
  with the intent to a service and cannot be carried by a work request.

## Left to the builder

- Exact Gradle, Kotlin and Compose versions. AGP 8.12.3 and Gradle 8.13 are already cached from
  the spike and `SoftLeaderAndroid`.
- Read-ahead window size, and how it interacts with the pump's batch sizing.
- How `tkfs-android` exposes engine calls over JNI.
- Lane and connection counts on the phone, tuned against the spike's roughly 200 MB of memory.
- Whether `openDocument` needs a pipe in any write mode that a proxy descriptor cannot serve. The
  provider must work for the Files app's copy path and for `w`, `wt` and `rw` from a normal app.
- How Kotlin learns that a burst moved events, for `notifyChange`.
- Keep or drop per-peer detail in Settings.

## Calls I made that the owner should check

- **Ask the PC before freeing (`objects.have`)**, rather than a local record of what was sent. The
  two-way design assumed store replies would serve this. Asking is stronger, because it holds even
  if the PC was reset or restored. The cost is that nothing can be freed while offline, and the PC
  needs this build.
- **Durable feed cursors, for passive dialers only.** This is the one piece of core work with a
  correctness risk, because replay on every session is what currently hides lost events. Without it
  the background check is not light, and I did not want to depend on folder size to find out.
- **"Waiting to send" is a durable table confirmed by the PC**, not a count from the burst. One
  confirming peer clears a file, which matters only with a second PC.
- **Metadata-only bursts on metered networks**, rather than skipping sync or moving files.
- **"Change address" in version one**, because a changed DHCP address would otherwise cost a
  code rotation. If the PC's address is fixed, this can be dropped.
- **An in-app file browser**, in addition to the provider, because keep and remove need somewhere
  to live. The earlier agreed picture was only the Files app and pickers.
- **A share target in version one**, as the main way into the transfer buffer.
- **Mobile data off by default** for background work. Earlier discussion only named the VPN case.
- **30 minutes** for the background check, against the earlier "very passively".
- **Free space floor** of `max(2 GiB, 10%)`, with no separate size limit on downloads.
- **Compose and Material 3**, where the spike used plain `Activity` widgets. Requires newer
  Kotlin than the spike's 1.9.24, or the matching older compiler.
- **Branching from `two-way-content`**, which is unmerged and has two open issues: "slow peers
  locked out" is taken on here because a phone hits it, and "delayed edits" (`SWEEP_PACE`) stays
  separate because a passive phone does not poll.
- **Document ids stay path-based.** An app holding a long-lived reference to a file loses it if the
  file is renamed. Stable ids need an engine lookup by entity, and can follow if this bothers.
- **No search or recents** in the provider in version one.
