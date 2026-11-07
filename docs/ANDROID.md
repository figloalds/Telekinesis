# Android client

The Android app is a phone-side client for the folders on your PCs. It runs the normal
orchestrator and workers inside the app, pairs with a PC like any other computer, and shows the
folders to other apps through a documents provider (Files, photo viewers, share sheets). It is a
transfer buffer: files come when you open them, files you save go to the PC when it is reachable,
and files you do not keep are freed when the phone runs low on space. The design is
[the Android client design](superpowers/specs/2026-11-05-android-client-design.md) and its screens
are [the mockup](superpowers/specs/2026-11-05-android-client-mockup.html).

## Building

You need the Android SDK (`ANDROID_HOME`), NDK 27.0.12077973, JDK 21 and the Rust Android targets.

```sh
python scripts/build_android.py                # both ABIs: arm64-v8a for phones, x86_64 for emulators
python scripts/build_android.py --abi x86_64   # the emulator only
```

The result is `target/runnables/android/tkfs-debug.apk`. The build checks that the arm64 library is
16 KiB page aligned, which a phone with 16 KiB pages needs to load it. The app is built with rustls
and bundled roots, so a private certificate authority is not supported.

## Installing

```sh
adb install -r target/runnables/android/tkfs-debug.apk
```

The app is debug signed and installed with `adb`, not from a file manager: that is the one way left
to install an app that is not registered with Google's developer verification. `-r` keeps what the
app holds (folders, pairings, marks, files waiting to send, its configuration), and
`python scripts/android_e2e.py --scenario reinstall` checks that it does.

Keep using the same debug key. An app signed with another key cannot be installed over this one, and
the only way past that is `adb uninstall dev.tkfs`, which **deletes files that were not sent yet**.
Send or copy what is waiting before you do it.

The debug build is the only build there is, and it includes two things a release build would not: a
path permission on the documents provider that the `adb shell` user holds, and `tkfs.*` shell calls
(`tkfs.manage`, `tkfs.document`, `tkfs.list_all` and others) that answer only a shell or root caller.
They are how `scripts/android_e2e.py` drives the app from outside. An ordinary app gets no answer
from them, but they are on the phone all the same.

## What the PC needs

- A build from this branch. Freeing space, "waiting to send" and durable sync positions need peer
  features that older builds do not have; the app says so when the PC is too old (`PC_TOO_OLD`).
- A listener the phone can reach. The default `network.listen` is `127.0.0.1:43180`, this computer
  only. Set it to `0.0.0.0:43180` (or an address on your LAN) and allow the port through the
  firewall; see [configuration](CONFIGURATION.md). The phone dials the PC and never the other way,
  so the PC needs no address for the phone.
- Pairing: open Add a PC on the phone and scan the QR from `tkfs manage reset-pairing-code --qr`, or
  paste the address and the 64 hex code. If the PC runs `reset-pairing-code` or unpairs the phone,
  the app says to pair again, and pairing again keeps the folder's data.

## Plain `ws://` is for your own network

A `ws://` connection is not encrypted, and it is not checked. Each new session sends this phone's
credential for that PC across the network as it is, and nothing proves who received it. Pairing sends
a key made from the pairing code, which stands in for the code, and then the secret the credential
is made from. Anyone who can read that traffic can use them as this phone.

A phone roams, and that makes this worse than it is on a desktop. The background check runs on
whatever network the phone is on, so on another network it offers the credential to whatever answers
at the PC's address, and believes that host: for the events it sends, and for `objects.have`, which
is what decides that a file can be freed from the phone. Change address has the same limit: over
`ws://`, a host accepting the credential does not show it is the same PC.

So use `ws://` only on your own network or over a VPN, and use a VPN or a `wss://` proxy for anything
else (see [configuration](CONFIGURATION.md)). Settings says this on every `ws://` PC. There is no way to
pin a PC yet: with `wss://` the phone checks the proxy's certificate against the bundled roots.

## Settings

- Sync on mobile data: off means that on a metered network only file lists update, and files you
  open still download.
- Background check: every 15, 30, 60 or 120 minutes, and only with a network. Android runs a
  periodic job no more often than every 15 minutes.
- Keep this much free, with Free up space now; Battery, with a way to allow background running. When
  space runs low the app frees what the PC holds by itself, at most once an hour, and leaves what you
  opened in the last day. Free up space now and Remove download do not leave those.
- Each PC, with Change address and Unpair.
- Diagnostics. Copy puts the state of each PC and folder, and the errors still showing, on the
  clipboard; it holds no pairing code or credential. Resync from the start makes every folder check
  its whole history with the PC again (`manage reset-cursors`) and then runs a sync.

## What will not work

- No real paths. Files are reached through the documents provider, so an app that wants a path
  under `/sdcard/` cannot use them.
- No gallery indexing: photos in a folder do not show up in Gallery or Photos. Open them from Files
  or share them from there.
- No folder creation on the phone. A folder starts on a PC and the phone receives it.
- One address per PC. If a PC has two, pick the one the phone can reach with Change address.
- With the PC away nothing leaves the phone. What you save waits, the Folders screen says so, and it
  goes when the PC is back. Opening a file that was never downloaded needs the PC.
- If this app is killed while another app is overwriting a file, the file stays as it was and what
  had been written is saved beside it as `name (recovered).ext`, and syncs like any file. If the other
  app is what dies, Android reports that as a close, so what it wrote becomes the file.

## Running the tests

```sh
cargo test --manifest-path android/native/Cargo.toml
android\gradlew.bat -p android :app:testDebugUnitTest
python scripts/android_e2e.py --scenario all
python scripts/android_e2e.py --scenario freeze --skip-build --keep-emulator
```

`scripts/android_e2e.py` runs the emulator (AVD `Medium_Phone_API_36.0`) against a headless
orchestrator on this PC. It never mounts anything, every wait is bounded, and a process left behind
is a failure. `--scenario all` builds, boots the AVD and runs every scenario. The freeze scenario
alone waits over two minutes for Android to freeze the app. Each run writes
`test-runs/android-e2e/<UTC>/` with `report.json`, `logcat.txt` and each scenario's orchestrator
home. A crash of the app in a scenario's logcat fails that scenario, and what the harness tried again
is under its `retries` in `report.json`. The harness's own tests are `python -m unittest
test_android_e2e`, run from `scripts/`.

The `speed` scenario reads a new 64 MiB random file from a release-built PC, by two roads. Through
`adb reverse` a first read must reach 10 MiB/s: that leaves out the emulator's network, so it comes
down to the app's own limit. Over the emulator's normal network, which carries only 8.4 to
10.2 MiB/s raw, the rate is only reported, since a bound there would measure the emulator.

### What only a real phone can test

None of this has been tested, and acceptance 19 and 20 are **not done**:

- Real Wi-Fi: pairing, opening a photo in a viewer, sharing a photo in and seeing it on the PC. The
  emulator reaches the PC through the host's loopback.
- The first-read target of at least 10 MiB/s over home Wi-Fi. The `speed` scenario shows that the
  app's own limit is past it, and not that a real phone on Wi-Fi gets there.
- Battery use and data used over a day of ordinary use, and how the background check behaves under
  the phone's own battery optimization.
- Scanning the pairing QR with the camera. The harness checks that the PC draws a QR and that
  pairing with the fields works; nothing scans one.
- Third-party file managers and photo viewers. The harness drives the provider through Android's own
  APIs as another app would, but no real third-party app has opened a file yet.
