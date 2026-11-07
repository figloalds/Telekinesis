"""End-to-end checks of the Android app: an emulator against a headless orchestrator on this PC.

    python scripts/android_e2e.py --scenario read
    python scripts/android_e2e.py --scenario all --skip-build --keep-emulator
    python scripts/android_e2e.py --scenario read --release-pc

It drives the app from outside with adb: `content` reaches the provider, and `content call`
with method tkfs.manage reaches the orchestrator inside it (debug builds only). It never
mounts anything. Every wait is bounded, and a process left behind is a failure. Each run writes
test-runs/android-e2e/<UTC>/: report.json, logcat.txt, the build and emulator logs and each
scenario's orchestrator home.

This file holds the command line and the scenarios; android_e2e_lib.py holds what they work
with. To add a scenario, write `def scenario_name(s: Session)` and list it in SCENARIOS, which
`all` runs. It starts with the app cleared and a new orchestrator on the PC, and
`s.folder_with(files)` gives it a folder the phone has received. `s.check` records what it finds;
a failed check ends the scenario.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import random
import re
import sys
import threading
import time
import traceback
from typing import Callable

from android_e2e_lib import (ADB_BOUND, BUILD_BOUND, CONTROL_URI, DEFAULT_AVD, DIRECTORY_MIME, FOLDER_READY_BOUND, PACKAGE, ROOT, ROOTS_URI,
                             ContentError, Device, Emulator, Folder, HarnessError, ManageError, PcHost, Report,
                             Session, SYNC_BOUND, UI_BOUND, children_uri, document_uri, png_noise, poll, run_bounded, sha256,
                             bounds_center, size_text, with_parents)
from build_android import APK, find_sdk

# ---- scenarios ----------------------------------------------------------------------------

HARD_NAME = "a b%#:é.txt"
HARD_DIRECTORY = "c d%#:é"


def read_files() -> dict[str, bytes]:
    photo = b"\x89PNG\r\n\x1a\n" + random.Random(8).randbytes(2 * 1024 * 1024 - 8)
    return {
        "notes.txt": "Notes from the PC.\n".encode() * 100,
        "docs/sub.txt": b"Below a directory.\n",
        HARD_NAME: "Um nome difícil.\n".encode(),
        f"{HARD_DIRECTORY}/{HARD_NAME}": "Um nome difícil numa pasta difícil.\n".encode(),
        "photo.png": photo,
    }


def scenario_read(s: Session) -> None:
    """Pairs, receives and reads each file through the provider. Listing downloads nothing."""
    device = s.device
    folder = s.folder_with({**read_files(), **image_files()})

    before = device.pack_bytes(folder.state)
    s.check("nothing is downloaded before a read", before < len(folder.files["photo.png"]), f"{before} pack bytes")
    tree = check_listing_downloads_nothing(s, folder, "before any read")

    roots = device.content_query(ROOTS_URI)
    s.check("the folder is a root", [(root["document_id"], root["title"]) for root in roots]
            == [(folder.doc(), folder.label)], roots)
    for directory in ("docs", HARD_DIRECTORY):
        s.check(f"{directory} lists as a directory", tree[directory]["mime_type"] == DIRECTORY_MIME, tree[directory])
    s.check("a PNG lists with its type", tree["photo.png"]["mime_type"] == "image/png", tree["photo.png"])
    for path, content in folder.files.items():
        row = tree[path]
        s.check(f"{path} lists with its name and size",
                row["_display_name"] == path.rsplit("/", 1)[-1] and row["_size"] == str(len(content)), row)
    hard = device.content_query(document_uri(folder.doc(HARD_NAME)))
    s.check("a name with a space, %, #, : and é has its own document",
            [row["_display_name"] for row in hard] == [HARD_NAME], hard)

    # Reading every one of the images through adb would take minutes; two of them are enough to read.
    opened = ["images/img00.png", "images/img01.png"]
    for path, content in folder.files.items():
        if path.startswith("images/") and path not in opened:
            continue
        want = sha256(content)
        on_pc = sha256(s.pc.pc_cat(folder.pc_state, path))
        s.check(f"{path} on the PC has the known SHA-256", on_pc == want, want)
        started = time.monotonic()
        read = device.content_read(folder.doc(path))
        took = time.monotonic() - started
        s.check(f"{path} reads the same on the phone", sha256(read) == want,
                f"{len(read)} of {len(content)} bytes in {took:.2f} s, {sha256(read)}")

    downloaded = device.pack_bytes(folder.state)
    s.check("reading downloaded the files", downloaded >= before + len(folder.files["photo.png"]),
            f"{before} -> {downloaded} pack bytes")
    check_listing_downloads_nothing(s, folder, "after the reads")
    check_browsing_the_images(s, folder, opened)
    check_cold_start(s, folder, "photo.png")
    check_offline_listing_is_quick(s, folder)


def check_listing_downloads_nothing(s: Session, folder: Folder, when: str) -> dict[str, dict[str, str]]:
    """Acceptance 15, for one listing of every directory. A failed query reads as no rows, so
    this also needs every name to come back. Thumbnails are checked by `check_browsing_the_images`."""
    before = s.device.pack_bytes(folder.state)
    tree = s.tree(folder)
    s.check(f"listing every directory {when} finds every name", set(tree) == with_parents(folder.files),
            sorted(tree))
    after = s.device.pack_bytes(folder.state)
    s.check(f"listing every directory {when} downloads nothing", after == before,
            f"{before} -> {after} pack bytes")
    return tree


def check_cold_start(s: Session, folder: Folder, path: str) -> None:
    s.device.force_stop()
    s.check("force-stop ended the app", s.device.app_pid() is None)
    read = s.device.content_read(folder.doc(path))
    s.check("a downloaded file reads on a cold start", sha256(read) == sha256(folder.files[path]),
            f"{len(read)} bytes")
    s.check("that request is what started the app", s.device.app_pid() is not None)


def scenario_cold_start(s: Session) -> None:
    """A force-stopped app starts on a provider request and serves a downloaded file."""
    folder = s.folder_with({"notes.txt": b"On the phone before the stop.\n"})
    s.check("the file downloads", s.device.content_read(folder.doc("notes.txt")) == folder.files["notes.txt"])
    check_cold_start(s, folder, "notes.txt")


def scenario_harness(s: Session) -> None:
    """The helpers later scenarios build on, and who the control path answers."""
    device = s.device
    status = device.shell_manage("status")
    s.check("shell_manage returns the reply", "daemon" in status and status["folders"] == [], status["daemon"])
    try:
        device.shell_manage("no-such-action")
        refused = ""
    except ManageError as error:
        refused = str(error)
    s.check("shell_manage raises the orchestrator's refusal", refused.startswith("UNSUPPORTED_ACTION"), refused)

    # A panic with a Java exception pending, under the JNI checks a debuggable app runs with.
    running = device.app_pid()
    fault = device.call_method("tkfs.fault")
    s.check("a panic in native code fails the call with its code", "error=NATIVE_PANIC: faultForTest" in fault, fault)
    s.check("the process survives it", running is not None and device.app_pid() == running, running)
    s.check("and still answers", "daemon" in device.shell_manage("status"))
    logged = device.logged_errors("tkfs")
    s.check("the panic is in logcat", "faultForTest" in logged, logged[-300:])

    label = """It's "é" $HOME `id` %41 \\ {x}: ;&|"""
    made = device.shell_manage("create", {"label": label})
    listed = device.shell_manage("list")
    s.check("a hard JSON payload arrives intact", [folder["label"] for folder in listed] == [label], listed)
    roots = device.content_query(ROOTS_URI)
    s.check("and comes back through a query", [root["title"] for root in roots] == [label], roots)

    # The same command with an ordinary app's uid. Android refuses it before the provider runs,
    # so this says nothing of ShellControl.admits, which its JVM test covers.
    other = device.shell("run-as", PACKAGE, "content", "call", "--uri", CONTROL_URI, "--method", "tkfs.manage",
                         "--arg", "list")
    s.check("an ordinary app's uid gets no answer from the control path",
            "result=" not in other.out and made["id"] not in other.out + other.err,
            (other.err or other.out)[:200])
    silent = device.call_method("tkfs.no-such-method")
    s.check("an unknown method goes to super.call", silent == "Result: null", silent)

    config = f"/data/user/0/{PACKAGE}/files/orchestrator.toml"
    written = device.shell("run-as", PACKAGE, "stat", "-c", "%Y %s", config).require_success().out
    text = device.shell("run-as", PACKAGE, "cat", config).require_success().out
    wanted = ['listen = ""', 'advertise = ""', 'sync_profile = "passive"', "maximum_running = 8", "connections = 4",
              "worker_lanes = 2"]
    s.check("the app configures itself as a passive dialer", all(line in text for line in wanted), text)
    time.sleep(1.1)  # stat's times have one-second steps.
    device.force_stop()
    s.check("a restarted app has its folder", len(device.content_query(ROOTS_URI)) == 1)
    again = device.shell("run-as", PACKAGE, "stat", "-c", "%Y %s", config).require_success().out
    s.check("the configuration is written only once", again == written, f"{written} -> {again}")

    device.start_app()
    shown = [node.get("text") for node in device.ui_dump().iter("node") if node.get("package") == PACKAGE]
    s.check("ui_dump sees the app's own screen", "Telekinesis" in shown, shown)
    s.check("tap_text finds a view by its text", bool(device.tap_text("Telekinesis")))
    try:
        device.tap_text("No such view", bound=3)
        missing = ""
    except HarnessError as error:
        missing = str(error)
    s.check("tap_text gives up within its bound", "No such view" in missing, missing)

    device.shell("input", "keyevent", "KEYCODE_HOME").require_success()
    took = device.wait_frozen(PACKAGE)
    s.check("wait_frozen sees the app frozen in the background", True, f"after {took:.0f} s")
    s.check("a frozen app still answers a provider request", len(device.content_query(ROOTS_URI)) == 1)


def periodic_wait_minutes(device: Device) -> int:
    """How long until the app's periodic background job next runs, in minutes: JobScheduler's own account of
    it (`dumpsys jobscheduler`). WorkManager gives a periodic job a delay of its whole interval, so a
    setting that only changed a label would show here."""
    dump = device.shell("dumpsys", "jobscheduler").require_success().out
    lines = [line for line in dump.splitlines() if f"@{PACKAGE}/androidx.work.impl.background.systemjob" in line
             and "TIME=+" in line]
    if len(lines) != 1:
        raise HarnessError(f"expected one delayed job of the app, found {len(lines)}: {lines}")
    window = re.search(r"TIME=\+(?:(\d+)h)?(?:(\d+)m)?", lines[0])
    if not window:
        raise HarnessError(f"no delay in {lines[0]}")
    return int(window.group(1) or 0) * 60 + int(window.group(2) or 0)


def check_settings_rows(s: Session, found: list[str]) -> None:
    """The rows of Settings the mockup has, and that "Background check" changes the job and not only its label."""
    device = s.device
    for row in ("Sync on mobile data", "Background check", "Keep this much free", "Battery", "Diagnostics", "Copy",
                "Resync from the start"):
        s.check(f"Settings has {row!r}", row in found)
    s.check("Background check starts at 30 min", "Every 30 min" in found)
    before = poll(UI_BOUND, "the periodic job queued", lambda: (m := periodic_wait_minutes(device)) >= 0 and m)
    s.check("the periodic job is due within 30 min", before <= 30, f"{before} min")
    device.tap_text("Change")
    device.wait_screen("the new interval", lambda t: "Every 1 h" in t)
    after = poll(UI_BOUND, "the job to take the new interval", lambda: (m := periodic_wait_minutes(device)) > 30 and m)
    s.check("the periodic job is now due within the hour", 30 < after <= 60, f"{before} -> {after} min")
    device.tap_text("Change")
    device.wait_screen("the next interval", lambda t: "Every 2 h" in t)
    # Round to where it started, so what follows sees the default.
    device.tap_text("Change")
    device.wait_screen("the shortest interval", lambda t: "Every 15 min" in t)
    device.tap_text("Change")
    device.wait_screen("the first interval again", lambda t: "Every 30 min" in t)

    # "Resync from the start" resets the cursors and runs a burst: a new exchange with the PC follows.
    last_exchange = lambda: max(p["last_exchange_ms"] for f in device.shell_manage("status")["folders"]
                                for p in f["sync"].values() if p.get("last_exchange_ms"))
    exchanged = last_exchange()
    device.tap_text("Resync from the start")
    device.wait_screen("the confirmation", lambda t: "Resync from the start?" in t)
    device.tap_text("Resync")
    newer = poll(SYNC_BOUND, "an exchange after the resync", lambda: last_exchange() > exchanged and last_exchange(), interval=2)
    s.check("Resync from the start runs a burst", True, f"{exchanged} -> {newer}")

    # "Copy": what it put on the clipboard is pasted into a field of the app.
    device.tap_text("Copy")
    device.tap_text("Change address")
    device.wait_screen("the address dialog", lambda t: "PC address" in t)
    device.tap_text("PC address")
    device.shell("input", "keyevent", "KEYCODE_PASTE").require_success()
    pasted = " ".join(node.get("text") or "" for node in device.ui_dump().iter("node") if node.get("package") == PACKAGE)
    s.check("Copy puts the status, the PC and the errors on the clipboard",
            all(part in pasted for part in ("PCs: 1", "ws://10.0.2.2:", "Documents", "Recent errors:")), pasted[:300])
    s.check("and no key or pairing code is in it", not re.search(r"[0-9a-fA-F]{64}", pasted), pasted[:100])
    device.tap_text("Cancel")
    device.wait_screen("the Settings screen", lambda t: "Diagnostics" in t and "Cancel" not in t)


def scenario_ui_folders(s: Session) -> None:
    """The Folders, Add a PC and Settings screens, driven as a person would. A PC that is away,
    a PC that revoked the phone, and pairing again with the folder's bytes kept. Screenshots of
    each screen are in this run's `screens` directory."""
    device, pc = s.device, s.pc
    shots = s.report.path.parent / "screens"
    files = {"notes.txt": b"Notes from the PC.\n" * 50, "photo.bin": random.Random(9).randbytes(600 * 1024)}
    repo, pc_state = pc.create_folder("Documents", files)

    def shown(what: str, wanted: Callable[[list[str]], bool], bound: float = UI_BOUND) -> list[str]:
        return device.wait_screen(what, wanted, bound)

    def has(*texts: str) -> Callable[[list[str]], bool]:
        return lambda found: all(text in found for text in texts)

    def has_part(part: str) -> Callable[[list[str]], bool]:
        return lambda found: any(part in text for text in found)

    def both(*wanted: Callable[[list[str]], bool]) -> Callable[[list[str]], bool]:
        return lambda found: all(check(found) for check in wanted)

    def pair_with_fields(code: str) -> None:
        device.type_into("PC address", pc.endpoint)
        device.type_into("Pairing code", code)
        device.screenshot(shots / f"add-pc-filled-{code[:4]}.png")
        device.tap_text("Pair")

    device.start_app()
    shown("the empty Folders screen", has("No PC is paired yet.", "Add a PC"))
    device.screenshot(shots / "folders-empty.png")
    device.tap_text("Add a PC")
    shown("the Add a PC screen", has("Scan QR code", "PC address", "Pairing code"))
    device.screenshot(shots / "add-pc.png")
    pair_with_fields(pc.pairing_code())
    # The first PC is the moment the app asks to show notifications (Android 13 and later).
    shown("the notification permission request", has("Allow Telekinesis to send you notifications?", "Allow"))
    device.tap_text("Allow")
    found = shown("the PC's folders", has("Receive 1 folder"))
    s.check("pairing lists the PC's folder with a checkbox", "Documents" in found, found)
    device.screenshot(shots / "add-pc-choose.png")
    device.tap_text("Receive 1 folder")
    found = shown("the folder with a Synced line", both(has("Documents"), has_part("Synced")), SYNC_BOUND)
    s.check("the Folders screen shows the folder and a Synced line",
            "Documents" in found and any(text.startswith("Synced") for text in found), found)
    device.screenshot(shots / "folders-synced.png")

    folder_id = device.shell_manage("list")[0]["id"]
    state = device.shell_manage("inspect", {"state": folder_id})["record"]["state"]
    folder = Folder(folder_id, "Documents", repo, state, pc_state, files)
    want = sha256(files["notes.txt"])
    s.check("the folder reads through the provider", sha256(device.content_read(folder.doc("notes.txt"))) == want)
    downloaded = device.pack_bytes(state)
    s.check("and its bytes are on the phone", downloaded >= len(files["notes.txt"]), f"{downloaded} pack bytes")

    device.tap_text("Settings")
    found = shown("the Settings screen", has("My PC", pc.endpoint, "Change address", "Unpair"))
    s.check("Settings names the PC with its address and the two actions", True, found)
    device.screenshot(shots / "settings.png")
    check_settings_rows(s, found)
    device.shell("input", "keyevent", "KEYCODE_BACK").require_success()
    shown("the Folders screen again", has("Telekinesis", "Sync now"))

    # The PC goes away: the strip says so and each folder keeps its last sync time.
    failures, summary = pc.stop()
    s.check("the PC stops cleanly", not failures, failures or summary)
    found = shown("the unreachable strip", has_part("My PC isn't reachable."))
    s.check("the strip says when it last synced", any("Last synced at" in text for text in found), found)
    s.check("the folder says when it last synced", any(text.startswith("Synced at ") for text in found), found)
    device.screenshot(shots / "folders-unreachable.png")
    pc.start()
    device.tap_text("Sync now")
    found = shown("the strip gone and a Synced line back",
                  lambda texts: not has_part("isn't reachable")(texts) and has_part("Synced")(texts), SYNC_BOUND)
    s.check("the strip is gone once the PC is back", True, found)

    # The PC makes a new code, which revokes the phone.
    new_code = pc.pairing_code()
    device.tap_text("Sync now")
    found = shown("Pair again", has_part("Pair again"), SYNC_BOUND)
    s.check("a revoked phone is told to pair again", any("no longer recognizes this phone" in t for t in found), found)
    status = device.shell_manage("status")
    seen = [(peer["endpoint"], [(lane["state"], lane.get("last_error")) for lane in peer["lanes"]])
            for peer in status["peers"]]
    s.check("the lane error is what the screen reads", any("PEER_REVOKED" in str(error) for _, lanes in seen
                                                         for _, error in lanes), seen)
    device.screenshot(shots / "folders-revoked.png")

    # Pair again: the folder and its bytes stay, and sync works.
    device.tap_text("Pair again")
    shown("the Add a PC screen", has("Scan QR code"))
    pair_with_fields(new_code)
    found = shown("Folders with a Synced line", both(has("Documents"), has_part("Synced")), SYNC_BOUND)
    s.check("pairing again keeps the folder on screen", not has_part("Pair again")(found), found)
    s.check("and its bytes", device.pack_bytes(state) >= downloaded, f"{downloaded} -> {device.pack_bytes(state)}")
    s.check("and its files still read", sha256(device.content_read(folder.doc("notes.txt"))) == want)
    pc.write_file(pc_state, "later.txt", b"Written after pairing again.\n")
    pc.folder(pc_state, "flush")
    device.tap_text("Sync now")
    poll(SYNC_BOUND, "the new file on the phone",
         lambda: b"after pairing again" in device.content_read(folder.doc("later.txt")), interval=2)
    s.check("a file written on the PC after pairing again reaches the phone", True)
    device.screenshot(shots / "folders-paired-again.png")


SYNC_APPEARS = 45
IDLE_SECONDS = 60


def requests_to_pc(pc: PcHost) -> int:
    """What the phone has asked of the PC so far, by the PC's own count."""
    return sum(peer["requests_served"] for peer in pc.manage("peers"))


def exchange_counters(device: Device, folder: Folder) -> dict:
    """What a burst moves, from `inspect`: per PC, the exchange time and the cursor counts."""
    sync = device.shell_manage("inspect", {"state": folder.id})["sync"]
    return {peer: (row["last_exchange_ms"], row["sent"], row["received"]) for peer, row in sync.items()}


def scenario_sync(s: Session) -> None:
    """When syncing happens: a foreground and a stale listing each start a burst behind the
    answer, the periodic job is scheduled, and an idle minute exchanges nothing."""
    device, pc = s.device, s.pc
    folder = s.folder_with({"first.txt": b"Here from the start.\n"})
    names = lambda: {row["_display_name"] for row in device.content_query(children_uri(folder.doc()))}

    # The foreground: a sync 60 s old is too young for a listing and old enough for the foreground.
    pc.write_file(folder.pc_state, "from-pc.txt", b"Written on the PC.\n")
    pc.folder(folder.pc_state, "flush")
    device.set_last_sync(60)
    s.check("a listing 60 s after the last sync does not have the new name yet", "from-pc.txt" not in names(), sorted(names()))
    started = time.monotonic()
    device.start_app()
    poll(SYNC_APPEARS, "the new name after the app came to the foreground", lambda: "from-pc.txt" in names())
    s.check("the foreground started a burst that brought the new name", True,
            f"{time.monotonic() - started:.1f} s after am start")

    # A listing older than 2 min: answered at once from what is known, with the burst behind it.
    pc.write_file(folder.pc_state, "from-listing.txt", b"Written on the PC, later.\n")
    pc.folder(folder.pc_state, "flush")
    device.set_last_sync(10 * 60)
    started = time.monotonic()
    answered = names()
    took = time.monotonic() - started
    s.check("the stale listing is answered from what is known", "from-listing.txt" not in answered,
            f"{took:.1f} s: {sorted(answered)}")
    poll(SYNC_APPEARS, "the new name after a stale listing", lambda: "from-listing.txt" in names())
    s.check("and the burst it started brought the new name", True)

    jobs = device.shell("dumpsys", "jobscheduler").out
    ours = [line for line in jobs.splitlines() if PACKAGE in line and "SystemJobService" in line]
    s.check("the periodic job is in jobscheduler", bool(ours), ours[:3] or jobs[-300:])

    # Idle: nothing in the app polls, so no exchange and no request to the PC.
    time.sleep(5)  # Let the bursts above settle.
    before, asked_before = exchange_counters(device, folder), requests_to_pc(pc)
    time.sleep(IDLE_SECONDS)
    after, asked_after = exchange_counters(device, folder), requests_to_pc(pc)
    s.check(f"an idle {IDLE_SECONDS} s causes no exchange", before == after, f"{before} -> {after}")
    s.check("and the PC was asked nothing", asked_after == asked_before, f"{asked_before} -> {asked_after}")


SPEED_FILE_BYTES = 64 * 1024 * 1024
SPEED_WANTED_MIB_S = 10


def first_read_rate(s: Session, folder: Folder, path: str) -> tuple[float, str]:
    """Reads `path` for the first time, and again from the phone, which shows what the rest of the
    path allows. The first read's MiB/s, and both in words."""
    content = folder.files[path]
    read, took = s.device.content_read_timed(folder.doc(path))
    s.check(f"{path} reads the same on the phone", sha256(read) == sha256(content), f"{len(read)} bytes")
    _, again = s.device.content_read_timed(folder.doc(path))
    rate = len(read) / (1024 * 1024) / took
    return rate, f"{rate:.1f} MiB/s ({took:.2f} s); {len(read) / (1024 * 1024) / again:.1f} MiB/s read again"


def lanes_all_ready(device: Device) -> bool:
    peers = device.shell_manage("peers")
    return bool(peers) and all(peer["lanes"] and all(lane["state"] == "Ready" for lane in peer["lanes"])
                               for peer in peers)


def scenario_speed(s: Session) -> None:
    """A first read of a new 64 MiB random file through the provider, from a release-built PC, by
    two roads, neither of which says anything of Wi-Fi (docs/ANDROID.md says what stays to measure).

    The emulator's normal network carries only 8.4 to 10.2 MiB/s raw, so a bound of 10 MiB/s there
    would measure the emulator: that rate is only reported. The bound is on a read through `adb
    reverse`, which leaves that network out and so comes down to the app's own limit."""
    device, pc = s.device, s.pc
    folder = s.folder_with({"normal.bin": random.Random(11).randbytes(SPEED_FILE_BYTES),
                            "reverse.bin": random.Random(12).randbytes(SPEED_FILE_BYTES)})
    _, normal = first_read_rate(s, folder, "normal.bin")
    s.check("the rate over the emulator's normal network, reported and not judged", True, normal)

    port = device.reverse(pc.port)
    try:
        installation = device.shell_manage("peers")[0]["installation"]
        device.shell_manage("set-endpoint", {"installation": installation, "endpoint": f"ws://127.0.0.1:{port}"})
        poll(SYNC_BOUND, "the lanes through adb reverse", lambda: lanes_all_ready(device), interval=1)
        rate, reverse = first_read_rate(s, folder, "reverse.bin")
        s.check(f"a first read through adb reverse goes at least {SPEED_WANTED_MIB_S} MiB/s",
                rate >= SPEED_WANTED_MIB_S, reverse)
    finally:
        device.remove_reverse(port)


# The flags Android gives these in DocumentsContract.
FLAG_WRITE, FLAG_DELETE, FLAG_DIR_CREATE, FLAG_RENAME, FLAG_MOVE = 2, 4, 8, 64, 256
FILE_FLAGS = FLAG_WRITE | FLAG_DELETE | FLAG_RENAME | FLAG_MOVE
DIRECTORY_FLAGS = FLAG_DIR_CREATE | FLAG_DELETE | FLAG_RENAME | FLAG_MOVE
ROOT_SUPPORTS_CREATE, ROOT_SUPPORTS_IS_CHILD = 1, 16

RENAMED_HARD_NAME = "é r%#:enomeado.txt"
CHUNK_BYTES = 64 * 1024
# Larger than a chunk, so it has a manifest and the phone has to fetch that too.
LARGE_FILE_BYTES = 300 * 1024


def refusal(call: Callable[[], object]) -> str:
    """What the provider refused `call` with; empty if it did not."""
    try:
        call()
    except (ManageError, ContentError) as error:
        return str(error)
    return ""


def names_in(s: Session, doc: str) -> dict[str, dict[str, str]]:
    return {row["_display_name"]: row for row in s.device.content_query(children_uri(doc))}


def check_on_pc(s: Session, folder: Folder, path: str, content: bytes, what: str) -> None:
    s.sync_now(folder)
    on_pc = s.pc.pc_cat(folder.pc_state, path)
    s.check(f"{what}: the PC has it after a burst", on_pc == content,
            f"{len(on_pc)} of {len(content)} bytes, {sha256(on_pc)}")


def scenario_write(s: Session) -> None:
    """Acceptance 11 and Review Focus 1 and 2: what apps do to files through the provider, each
    checked on the PC. It never mounts anything; a real write goes through a descriptor."""
    large = random.Random(13)
    files = {
        "from-pc.txt": b"Written on the PC.\n",
        "sub/below.txt": b"Below a directory.\n",
        "kill-after-release.txt": b"Before the kill.\n",
        "kill-before-a-write.txt": b"Before the kill, with nothing written.\n",
        "kill-while-open.txt": b"Before the kill, while open.\n",
        "kill-while-open.bin": large.randbytes(LARGE_FILE_BYTES),
        "restart-while-open.txt": b"0123456789 before the restart.\n",
        "replaced-away.bin": large.randbytes(LARGE_FILE_BYTES),
        "replaced-away.txt": b"A small file nobody read.\n",
        "killed-away.bin": large.randbytes(LARGE_FILE_BYTES),
        "edited-away.bin": large.randbytes(LARGE_FILE_BYTES),
        "edited-away.txt": b"A small file nobody read, either.\n",
    }
    folder = s.folder_with(files)
    check_write_flags(s, folder)
    # First, while what the others write does not count as files on the phone.
    check_the_pc_away(s, folder)
    check_names_and_the_namespace(s, folder)
    check_the_open_modes(s, folder)
    check_open_listings_hear_of_changes(s, folder)
    check_a_kill_after_a_release(s, folder)
    check_a_kill_while_open(s, folder)
    check_a_worker_restart_while_open(s, folder)


def check_write_flags(s: Session, folder: Folder) -> None:
    device = s.device
    root = device.content_query(ROOTS_URI)[0]
    s.check("the root says it can create", int(root["flags"]) == ROOT_SUPPORTS_CREATE | ROOT_SUPPORTS_IS_CHILD, root)
    own = device.content_query(document_uri(folder.doc()))[0]
    s.check("a folder's root can only be created in", int(own["flags"]) == FLAG_DIR_CREATE, own)
    names = names_in(s, folder.doc())
    s.check("a file can be written, deleted, renamed and moved", int(names["from-pc.txt"]["flags"]) == FILE_FLAGS,
            names["from-pc.txt"])
    s.check("a directory can be created in, deleted, renamed and moved", int(names["sub"]["flags"]) == DIRECTORY_FLAGS,
            names["sub"])


def check_names_and_the_namespace(s: Session, folder: Folder) -> None:
    """Create through `content write`, edit, rename, move and delete, all with hard names."""
    device, pc = s.device, s.pc
    first, second = "Primeira versão.\n".encode() * 200, "Segunda.\n".encode()

    doc = device.create_document(folder.doc(), HARD_NAME, "text/plain")
    s.check("a hard name is created with the id the provider promised", doc == folder.doc(HARD_NAME), doc)
    device.content_write(doc, first)
    opened = [line for line in device.provider_log() if line.startswith(f"open {doc} ")]
    s.check("`content write` opens the document in mode w", opened == [f"open {doc} w"], opened)
    s.check("what it wrote reads back", device.content_read(doc) == first)
    s.check("and lists with its name and size", names_in(s, folder.doc())[HARD_NAME]["_size"] == str(len(first)))
    check_on_pc(s, folder, HARD_NAME, first, "a created file")

    device.content_write(doc, second)
    s.check("a shorter `content write` leaves no tail of the longer one", device.content_read(doc) == second)
    check_on_pc(s, folder, HARD_NAME, second, "an edit")

    clash = device.create_document(folder.doc(), HARD_NAME, "text/plain")
    s.check("a name that is taken gets the first free number before its extension",
            clash == folder.doc("a b%#:é (1).txt"), clash)
    s.check("and the file that had it is untouched", device.content_read(doc) == second)
    device.delete_document(clash)

    for name in ("a/b", "", ".", ".."):
        s.check(f"creating {name!r} is refused with its code",
                refusal(lambda: device.create_document(folder.doc(), name)).startswith("INVALID_NAME"))
        s.check(f"renaming to {name!r} is refused with its code",
                refusal(lambda: device.rename_document(doc, name)).startswith("INVALID_NAME"))

    directory = device.create_document(folder.doc(), HARD_DIRECTORY, DIRECTORY_MIME)
    s.check("a directory with a hard name is created", directory == folder.doc(HARD_DIRECTORY), directory)
    s.check("and lists as a directory", names_in(s, folder.doc())[HARD_DIRECTORY]["mime_type"] == DIRECTORY_MIME)

    renamed = device.rename_document(doc, RENAMED_HARD_NAME)
    s.check("a rename says the new id", renamed == folder.doc(RENAMED_HARD_NAME), renamed)
    s.check("the old name is gone from the listing", HARD_NAME not in names_in(s, folder.doc()))
    s.check("and the file reads under the new one", device.content_read(renamed) == second)
    check_on_pc(s, folder, RENAMED_HARD_NAME, second, "a rename")
    s.check("the old name is gone on the PC", not pc.pc_has(folder.pc_state, HARD_NAME))

    taken = refusal(lambda: device.rename_document(renamed, "from-pc.txt"))
    s.check("a rename onto a name that is taken is refused", taken.startswith("ALREADY_EXISTS"), taken)
    s.check("and overwrites nothing", device.content_read(folder.doc("from-pc.txt")) == folder.files["from-pc.txt"])

    across = refusal(lambda: device.move_document(renamed, folder.doc(), "another-folder:/"))
    s.check("a move across folders is refused, so Android copies and deletes",
            across.startswith("UNSUPPORTED_OPERATION"), across)
    s.check("and moves nothing", device.content_read(renamed) == second)

    inside = f"{HARD_DIRECTORY}/{RENAMED_HARD_NAME}"
    moved = device.move_document(renamed, folder.doc(), directory)
    s.check("a move says the new id", moved == folder.doc(inside), moved)
    s.check("the file reads where it went", device.content_read(moved) == second)
    s.check("and is gone from where it was", RENAMED_HARD_NAME not in names_in(s, folder.doc()))
    check_on_pc(s, folder, inside, second, "a move into a subfolder")
    s.check("the old place is empty on the PC", not pc.pc_has(folder.pc_state, RENAMED_HARD_NAME))

    gone = refusal(lambda: device.content_write(folder.doc("no-such-file.txt"), b"lost"))
    s.check("a write to a document that does not exist fails", "NOT_FOUND" in gone, gone[:300])
    s.check("and creates nothing", "no-such-file.txt" not in names_in(s, folder.doc()))

    device.delete_document(moved)
    s.check("a delete takes it out of the listing", not names_in(s, directory))
    s.sync_now(folder)
    s.check("a deleted file is gone on the PC", not pc.pc_has(folder.pc_state, inside))

    # What Android's Files app sends for a directory: one delete, with everything in it.
    device.content_write(device.create_document(directory, "inner.txt"), b"Inside.\n")
    device.delete_document(directory)
    s.check("deleting a directory takes it from the listing", HARD_DIRECTORY not in names_in(s, folder.doc()))
    s.sync_now(folder)
    s.check("and from the PC", not pc.pc_has(folder.pc_state, HARD_DIRECTORY))

    # A change asks for its own burst: nobody calls sync-now here, and nothing touches the app
    # while this waits, so it is cached and frozen, with its network cut. Only the transfer job's
    # foreground notification gets the file across.
    device.clear_logcat()
    alone = device.create_document(folder.doc(), "burst-by-itself.txt", "text/plain")
    device.content_write(alone, b"Nobody asked.\n")

    def arrived() -> bool:
        return (pc.pc_has(folder.pc_state, "burst-by-itself.txt")
                and pc.pc_cat(folder.pc_state, "burst-by-itself.txt") == b"Nobody asked.\n")

    poll(SYNC_BOUND, "a written file on the PC with no sync-now and the app left alone", arrived, interval=1)
    s.check("a write asks for a burst by itself, with the app in the background", True, device.transfer_log())


def check_the_open_modes(s: Session, folder: Folder) -> None:
    device = s.device
    doc = device.create_document(folder.doc(), "modes.txt", "text/plain")

    s.check("wt writes", device.write_document(doc, "wt", b"0123456789") == 10)
    s.check("and fsync goes through", device.write_document(doc, "rw", b"AB", offset=2, fsync=True) == 2)
    s.check("rw keeps what it does not write over", device.content_read(doc) == b"01AB456789")
    device.write_document(doc, "wa", b"tail")
    s.check("wa appends, whatever the offset", device.content_read(doc) == b"01AB456789tail")
    device.write_document(doc, "rwt", b"cut")
    s.check("rwt truncates", device.content_read(doc) == b"cut")
    device.content_write(doc, b"")
    s.check("w with nothing to write leaves an empty file", device.content_read(doc) == b"")
    device.write_document(doc, "w", b"w")
    s.check("w truncates", device.content_read(doc) == b"w")
    check_on_pc(s, folder, "modes.txt", b"w", "what the modes left")

    large = random.Random(14).randbytes(2 * 1024 * 1024)
    doc = device.create_document(folder.doc(), "large-upload.bin")
    started = time.monotonic()
    device.content_write(doc, large)
    took = time.monotonic() - started
    s.check("2 MiB goes through `content write`", sha256(device.content_read(doc)) == sha256(large), f"{took:.1f} s")
    check_on_pc(s, folder, "large-upload.bin", large, "a large file")


def check_open_listings_hear_of_changes(s: Session, folder: Folder) -> None:
    device = s.device
    root_doc, sub = folder.doc(), folder.doc("sub")

    def heard(what: str, directories: list[str], change: Callable[[], str | None]) -> str | None:
        before = {directory: device.changes_heard(directory) for directory in directories}
        result = change()
        poll(UI_BOUND, f"{what} heard in {directories}",
             lambda: all(device.changes_heard(directory) > count for directory, count in before.items()))
        s.check(f"{what} tells an open listing of {len(directories)} director{'y' if len(directories) == 1 else 'ies'}", True)
        return result

    doc = heard("a create", [root_doc], lambda: device.create_document(root_doc, "heard.txt"))
    heard("a closed write", [root_doc], lambda: device.content_write(doc, b"Heard.\n"))
    doc = heard("a rename", [root_doc], lambda: device.rename_document(doc, "heard again.txt"))
    doc = heard("a move", [root_doc, sub], lambda: device.move_document(doc, root_doc, sub))
    heard("a closed write in a subfolder", [sub], lambda: device.write_document(doc, "wa", b"More.\n"))
    heard("a delete", [sub], lambda: device.delete_document(doc))


def check_a_kill_after_a_release(s: Session, folder: Folder) -> None:
    """A release seals before it returns, and a force-stop right after it loses nothing."""
    device = s.device
    doc, content = folder.doc("kill-after-release.txt"), b"After the kill.\n"
    before = device.releases(doc)
    device.content_write(doc, content)
    device.wait_release(doc, before)
    device.force_stop()
    s.check("force-stop right after a release ended the app", device.app_pid() is None)
    s.sync_with_the_pc_back(folder)
    on_pc = s.pc.pc_cat(folder.pc_state, "kill-after-release.txt")
    s.check("what was written reaches the PC after the restart", on_pc == content, on_pc[:80])

    # A client that holds `w` and has written nothing has changed nothing yet.
    held, kept = folder.doc("kill-before-a-write.txt"), folder.files["kill-before-a-write.txt"]
    device.write_document(held, "w", b"", close=False)
    device.force_stop()
    s.check("force-stop with a writer that wrote nothing ended the app", device.app_pid() is None)
    s.check("and the file reads as it was", device.content_read(held) == kept)
    s.sync_with_the_pc_back(folder)
    on_pc = s.pc.pc_cat(folder.pc_state, "kill-before-a-write.txt")
    s.check("and it is as it was on the PC", on_pc == kept, on_pc[:80])

    # The writer has its bytes in the draft by now. Its release may have run, or be running.
    content = b"After the second kill.\n"
    device.content_write(doc, content)
    device.force_stop()
    s.check("force-stop with no wait for the release ended the app", device.app_pid() is None)
    s.sync_with_the_pc_back(folder)
    on_pc = s.pc.pc_cat(folder.pc_state, "kill-after-release.txt")
    s.check("what was written reaches the PC all the same", on_pc == content, on_pc[:80])


def check_a_kill_while_open(s: Session, folder: Folder) -> None:
    """No release at all: a real restart seals the draft through `seal::recover`. For the large
    file the phone holds only the chunk it edited, and at that point nothing can fetch the rest."""
    device, pc = s.device, s.pc
    small, large = folder.files["kill-while-open.txt"], folder.files["kill-while-open.bin"]
    held = device.pack_bytes(folder.state)
    device.write_document(folder.doc("kill-while-open.txt"), "rw", b"AFTER", close=False)
    device.write_document(folder.doc("kill-while-open.bin"), "rw", b"AFTER", offset=10, close=False)
    fetched = device.pack_bytes(folder.state) - held
    s.check("the edit fetched only part of the large file", fetched < LARGE_FILE_BYTES / 2, f"{fetched} pack bytes")
    device.force_stop()
    s.check("force-stop with two writers open ended the app", device.app_pid() is None)
    s.sync_with_the_pc_back(folder)
    want = b"AFTER" + small[5:]
    on_pc = pc.pc_cat(folder.pc_state, "kill-while-open.txt")
    s.check("a draft killed while open reaches the PC", on_pc == want, on_pc[:80])
    want = large[:10] + b"AFTER" + large[15:]
    on_pc = pc.pc_cat(folder.pc_state, "kill-while-open.bin")
    s.check("a large draft killed while open reaches the PC with only that spot changed",
            sha256(on_pc) == sha256(want), f"{len(on_pc)} bytes, {sha256(on_pc)}")
    read = device.content_read(folder.doc("kill-while-open.bin"))
    s.check("and reads the same on the phone, which fetches the rest", sha256(read) == sha256(want), f"{len(read)} bytes")


def check_a_worker_restart_while_open(s: Session, folder: Folder) -> None:
    """A writable handle never reopens: going on from a new token would lose or misplace what it
    wrote. After a restart its writes fail, and what it wrote before is safe."""
    device = s.device
    doc, content = folder.doc("restart-while-open.txt"), folder.files["restart-while-open.txt"]
    device.write_document(doc, "rw", b"FIRST", close=False)
    before = device.releases(doc)
    device.shell_manage("force-stop", {"state": folder.id}, bound=SYNC_BOUND)
    device.shell_manage("start", {"state": folder.id})
    poll(FOLDER_READY_BOUND, "the folder running again",
         lambda: device.shell_manage("inspect", {"state": folder.id})["phase"]["phase"] == "Running")
    failed = refusal(lambda: device.write_document(doc, "rw", b"SECOND", offset=5))
    s.check("a write on a handle from before a worker restart fails with an I/O error", "I/O error" in failed, failed[:300])
    device.wait_release(doc, before)
    logged = [line for line in device.provider_log() if line.startswith(f"{doc}: sync: ")]
    s.check("its release logs the code of the failed sync", any("INVALID_HANDLE" in line for line in logged), logged)
    want = b"FIRST" + content[5:]
    s.check("and what it wrote before is there, once", device.content_read(doc) == want)
    s.sync_with_the_pc_back(folder)
    on_pc = s.pc.pc_cat(folder.pc_state, "restart-while-open.txt")
    s.check("and reaches the PC", on_pc == want, on_pc[:80])


def check_the_pc_away(s: Session, folder: Folder) -> None:
    """Review Focus 2. `wt` needs nothing of a file the phone never downloaded, and writing part
    of one cannot work: it fails, and the file stays as it was on both sides."""
    device, pc = s.device, s.pc
    files = folder.files
    held = device.pack_bytes(folder.state)
    s.check("the files for this are not on the phone", held < LARGE_FILE_BYTES, f"{held} pack bytes")
    new_large, new_small = random.Random(15).randbytes(LARGE_FILE_BYTES // 2), b"Replaced with the PC away.\n"

    failures, summary = pc.stop()
    s.check("the PC stops cleanly", not failures, failures or summary)

    for path, content in (("replaced-away.bin", new_large), ("replaced-away.txt", new_small)):
        doc = folder.doc(path)
        started = time.monotonic()
        device.content_write(doc, content)
        s.check(f"w on {path}, never downloaded, works with the PC away", device.content_read(doc) == content,
                f"{time.monotonic() - started:.1f} s")
        s.check("and lists with its new size", names_in(s, folder.doc())[path]["_size"] == str(len(content)))

    # An open that keeps the contents and is not all here asks for a PC first, and fails with none.
    for path in ("edited-away.txt", "edited-away.bin"):
        doc = folder.doc(path)
        started = time.monotonic()
        failed = refusal(lambda: device.write_document(doc, "rw", b"part", offset=3))
        s.check(f"rw writing part of {path}, never downloaded, fails with the PC away", "PC_UNREACHABLE" in failed,
                f"{time.monotonic() - started:.1f} s: {failed[:300]}")
        s.check("and its listed size stays", names_in(s, folder.doc())[path]["_size"] == str(len(files[path])))

    # Past the end of a file whose short last chunk is not here: the same, and no seal to fail.
    doc, past = folder.doc("edited-away.txt"), 2 * CHUNK_BYTES
    failed = refusal(lambda: device.write_document(doc, "rw", b"past", offset=past))
    s.check("rw past the end of a short file never downloaded fails with the PC away", "PC_UNREACHABLE" in failed, failed[:300])
    size = names_in(s, folder.doc())["edited-away.txt"]["_size"]
    s.check("and its listed size stays", size == str(len(files["edited-away.txt"])), size)

    # What was done with the PC away survives a restart too, sealed or not. The second file's
    # draft has no release and its old manifest was never here, and the folder must still open.
    new_killed = random.Random(16).randbytes(8 * 1024)
    device.write_document(folder.doc("killed-away.bin"), "wt", new_killed, close=False)
    device.force_stop()
    s.check("force-stop with the PC away and a writer open ended the app", device.app_pid() is None)
    s.check("a replaced file still reads after it", device.content_read(folder.doc("replaced-away.txt")) == new_small)
    s.check("and so does the one whose writer was killed", device.content_read(folder.doc("killed-away.bin")) == new_killed)

    pc.start()
    pc.wait_phase(folder.label, "Running")
    s.sync_with_the_pc_back(folder)
    for path, content in (("replaced-away.bin", new_large), ("replaced-away.txt", new_small),
                          ("killed-away.bin", new_killed)):
        on_pc = pc.pc_cat(folder.pc_state, path)
        s.check(f"{path} reaches the PC later", on_pc == content, f"{len(on_pc)} of {len(content)} bytes")
    for path in ("edited-away.txt", "edited-away.bin"):
        on_pc = pc.pc_cat(folder.pc_state, path)
        s.check(f"{path} stays as it was on the PC", on_pc == files[path], f"{len(on_pc)} bytes")
        s.check(f"and reads as it was on the phone", device.content_read(folder.doc(path)) == files[path])


SHARE_BOUND = 1800
SHARE_BYTES = 256 * 1024 * 1024
MIB = 1024 * 1024
# How long a burst a change started takes to be over with.
SYNC_SETTLE = 10


def random_bytes(rng: random.Random, size: int) -> bytes:
    """`size` random bytes: `randbytes` itself cannot make 256 MiB at once."""
    return b"".join(rng.randbytes(min(16 * MIB, size - done)) for done in range(0, size, 16 * MIB))


def pc_pack_bytes(folder: Folder) -> int:
    """What the PC's copy of the folder holds in its packs, so a transfer can be seen under way."""
    return sum(path.stat().st_size for path in Path(folder.pc_state).rglob("*")
               if path.is_file() and "packs" in path.parts)


def trigger_foreground(device: Device) -> None:
    """The foreground trigger of the sync table: the app coming to the front, 30 s after its last sync."""
    device.home()  # An app that is already in front gets no onStart.
    time.sleep(1)
    device.set_last_sync(60)
    device.start_app()


def share_one(s: Session, name: str, data: bytes | Path, mime: str = "application/octet-stream") -> None:
    """Another app shares `data`; the owner taps through Save to Telekinesis with the defaults."""
    device = s.device
    device.start_share(device.push_download(name, data), mime)
    texts = device.wait_screen("the share screen", lambda t: "Save 1 file" in t)
    for wanted in ("Save to Telekinesis", "Inside this folder", "They go to My PC when it's reachable."):
        s.check(f"the share screen for {name} says {wanted!r}", wanted in texts, texts)
    device.tap_text("Save 1 file")


def pc_has_whole(s: Session, folder: Folder, path: str, content: bytes | Folder, ref_path: str = "",
                 packs_before: int = 0) -> bool:
    """The PC holds `path` with these bytes. Small ones are read back. A large one is compared with a
    copy of the same bytes imported into another PC folder (`content` is that folder, `ref_path` the
    file), and counts once the PC's packs have grown by at least the file's size since `packs_before`."""
    if isinstance(content, Folder):
        reference = s.pc.pc_content(content.pc_state, ref_path)
        return (reference is not None and s.pc.pc_content(folder.pc_state, path) == reference
                and pc_pack_bytes(folder) - packs_before >= len(content.files[ref_path]))
    return s.pc.pc_has(folder.pc_state, path) and sha256(s.pc.pc_cat(folder.pc_state, path)) == sha256(content)


def deliver(s: Session, folder: Folder, path: str, content: bytes | Folder, what: str, bound: float = SHARE_BOUND,
            nudge: Callable[[], None] | None = None, ref_path: str = "", packs_before: int = 0) -> None:
    """Waits for `path` on the PC, asking the phone for nothing but `nudge` (a trigger of the table)."""
    last = [0.0]

    def arrived() -> bool:
        if nudge and time.monotonic() - last[0] > 30:
            last[0] = time.monotonic()
            nudge()
        return pc_has_whole(s, folder, path, content, ref_path, packs_before)

    poll(bound, f"{what} on the PC", arrived, interval=2)
    s.check(f"{what} is on the PC with identical bytes", True)


def check_nothing_waits(s: Session, folder: Folder, what: str) -> None:
    """The table on disk, then the Folders screen, hold nothing waiting for `what`, asking the phone
    for nothing but a trigger of the table."""
    device = s.device
    last = [0.0]

    def empty() -> bool:
        if time.monotonic() - last[0] > 30:
            last[0] = time.monotonic()
            trigger_foreground(device)
        return not device.waiting_files(folder.id)

    poll(SYNC_BOUND, f"nothing waiting to send for {what}", empty, interval=2)
    device.wait_screen("no file waiting to send",
                       lambda texts: not any("waiting to send" in shown for shown in texts), 120)
    s.check(f"nothing is waiting to send for {what}", True)


def wait_transfer_ends(device: Device) -> None:
    """Until the transfer job has said it is done, so the next step starts a job of its own."""
    poll(120, "the transfer job to end", lambda: any("Stop(" in line for line in device.transfer_log()))


def scenario_share(s: Session) -> None:
    """Acceptance 12 and 13: sharing into a folder, and the transfer job that sends it. The app is
    left in the background for the delivery, because that is where Android cuts its network."""
    device, pc = s.device, s.pc
    rng = random.Random(14)
    clash_old = rng.randbytes(50 * 1024)
    big_from_pc = random_bytes(rng, SHARE_BYTES)
    folder = s.folder_with({"clash.bin": clash_old, "seed.txt": b"Here from the start.\n",
                            "from-pc-large.bin": big_from_pc})
    names = lambda: {row["_display_name"]: row for row in device.content_query(children_uri(folder.doc()))}

    # The PC away: the app keeps what is shared, and a clash gets a number.
    failures, summary = pc.stop()
    s.check("the PC stops cleanly", not failures, failures or summary)
    one, clash_new = rng.randbytes(3 * MIB), rng.randbytes(70 * 1024)
    share_one(s, "share-one.bin", one)
    poll(SHARE_BOUND, "the shared file in the folder", lambda: "share-one.bin" in names())
    s.check("a shared file is kept with the PC away", device.content_read(folder.doc("share-one.bin")) == one)

    # What has not reached the PC is said on the Folders screen, from a table on disk: after a kill too.
    says = lambda text: lambda texts: any(text in shown for shown in texts)
    only_share_one = lambda: [file["path"] for file in device.waiting_files(folder.id)] == ["/share-one.bin"]
    device.start_app()
    device.wait_screen("one file waiting to send", says("1 file waiting to send"), 120)
    s.check("the Folders screen says 1 file waiting to send, with the PC away", True)
    s.check("and the table names that file", only_share_one(), device.waiting_files(folder.id))
    device.force_stop()
    device.start_app()
    device.wait_screen("one file waiting after a kill", says("1 file waiting to send"), 120)
    s.check("it still says so after the app was force-stopped", True)
    s.check("and the file is still named in the table", only_share_one(), device.waiting_files(folder.id))

    share_one(s, "clash.bin", clash_new)
    poll(SHARE_BOUND, "the numbered name", lambda: "clash (1).bin" in names())
    s.check("a name that is taken gets a number", device.content_read(folder.doc("clash (1).bin")) == clash_new)
    listed = names()
    s.check("and the file that had the name is untouched", listed["clash.bin"]["_size"] == str(len(clash_old)))
    s.check("no working file is left in the folder", not any(name.startswith(".tkfs-part-") for name in listed),
            sorted(listed))

    # A file that cannot be saved is reported, and nothing is left under any name: the folder's
    # worker is stopped while the share screen is up, so the copy fails at its first call. A file
    # that goes away from under the grant is refused by Android before any copy starts (a toast).
    # Everything above ran without the notification permission, as the job must. This needs it,
    # to read the notification back.
    device.shell("pm", "grant", PACKAGE, "android.permission.POST_NOTIFICATIONS").require_success()
    device.clear_logcat()
    device.start_share(device.push_download("cannot-save.bin", rng.randbytes(1024)))
    device.wait_screen("the share screen", lambda t: "Save 1 file" in t)
    device.shell_manage("force-stop", {"state": folder.id}, bound=SYNC_BOUND)
    device.tap_text("Save 1 file")

    def reported() -> bool:
        return "could not be saved" in device.shell("dumpsys", "notification", "--noredact").out

    poll(60, "the notification that names the failure", reported, interval=2)
    s.check("a stream that cannot be copied is named in a notification", True)
    device.shell_manage("start", {"state": folder.id})
    poll(FOLDER_READY_BOUND, "the folder running again",
         lambda: device.shell_manage("inspect", {"state": folder.id})["phase"]["phase"] == "Running")
    s.check("and leaves nothing in the folder", set(names()) == set(listed), sorted(names()))

    # The Folders screen says so too, until dismissed, and does not depend on the notification permission.
    def strip_is(name: str) -> None:
        device.start_app()
        # A copy into a folder whose worker is down waits for it before it fails.
        device.wait_screen(f"the strip for {name}", lambda t: f"Couldn't save {name}. Share it again." in t, 120)

    strip_is("cannot-save.bin")
    s.check("the Folders screen says what could not be saved", True)
    device.tap_text("Dismiss")
    device.wait_screen("the strip gone", lambda t: not any(text.startswith("Couldn't save") for text in t))
    s.check("and the strip goes when dismissed", True)

    # With the permission denied: a failed copy, and a file taken from under its grant.
    device.shell("pm", "revoke", PACKAGE, "android.permission.POST_NOTIFICATIONS").require_success()
    device.shell_manage("force-stop", {"state": folder.id}, bound=SYNC_BOUND)
    device.start_share(device.push_download("cannot-save-two.bin", rng.randbytes(1024)))
    device.wait_screen("the share screen", lambda t: "Save 1 file" in t)
    device.tap_text("Save 1 file")
    strip_is("cannot-save-two.bin")
    s.check("a failed copy is on the Folders screen with the notification permission denied", True)
    s.check("and no notification said so", "cannot-save-two.bin could not be saved" not in
            device.shell("dumpsys", "notification", "--noredact").out)
    device.tap_text("Dismiss")
    device.shell_manage("start", {"state": folder.id})
    poll(FOLDER_READY_BOUND, "the folder running again",
         lambda: device.shell_manage("inspect", {"state": folder.id})["phase"]["phase"] == "Running")

    device.start_share(device.push_download("gone.bin", rng.randbytes(1024)))
    device.wait_screen("the share screen", lambda t: "Save 1 file" in t)
    device.shell("rm", "-f", "/sdcard/Download/gone.bin")
    device.tap_text("Save 1 file")
    strip_is("gone.bin")
    s.check("a file taken from under its grant is on the Folders screen too", True)
    device.tap_text("Dismiss")

    # The PC back: no action on the phone but a trigger of the table.
    pc.start()
    pc.wait_phase(folder.label, "Running")
    for path, content in (("share-one.bin", one), ("clash (1).bin", clash_new)):
        deliver(s, folder, path, content, f"{path} shared with the PC away", nudge=lambda: trigger_foreground(device))
    s.check("the file that had the name is as it was on the PC",
            sha256(pc.pc_cat(folder.pc_state, "clash.bin")) == sha256(clash_old))

    # Once the PC holds the bytes and has the events, they are no longer waiting.
    check_nothing_waits(s, folder, "the shares the PC took")

    # A PC that takes pushes but cannot say what it holds (no `content-have`): the file stays waiting even
    # once its bytes are there, and the app says to update the PC.
    failures, summary = pc.stop()
    s.check("the PC stops cleanly to become one without content-have", not failures, failures or summary)
    pc.extra_env = {"TKFS_FAULT_POINT": "session.features.no-content-have"}
    pc.start()
    pc.wait_phase(folder.label, "Running")
    old_pc = rng.randbytes(1 * MIB)
    device.content_write(device.create_document(folder.doc(), "old-pc.bin"), old_pc)
    nudged = [0.0]

    def says_update() -> bool:
        if time.monotonic() - nudged[0] > 30:
            nudged[0] = time.monotonic()
            trigger_foreground(device)
        return any("Update the PC" in shown for shown in device.screen_texts())

    poll(SYNC_BOUND, "the Folders screen to say to update the PC", says_update, interval=2)
    shown = [text for text in device.screen_texts() if "waiting to send" in text]
    s.check("an unsent file shows 'Update the PC' against a PC without content-have",
            shown == ["1 file waiting to send. Update the PC."], shown)
    def bytes_arrived() -> bool:
        if time.monotonic() - nudged[0] > 30:
            nudged[0] = time.monotonic()
            trigger_foreground(device)
        return pc_has_whole(s, folder, "old-pc.bin", old_pc)

    poll(SYNC_BOUND, "the bytes on the PC that cannot say it has them", bytes_arrived, interval=2)
    s.check("the PC has the bytes, and the file still waits because it cannot say so",
            [file["path"] for file in device.waiting_files(folder.id)] == ["/old-pc.bin"])
    s.check("and is named in the table", [file["path"] for file in device.waiting_files(folder.id)] == ["/old-pc.bin"])
    failures, summary = pc.stop()
    s.check("the PC without content-have stops cleanly", not failures, failures or summary)
    pc.extra_env = {}
    pc.start()
    pc.wait_phase(folder.label, "Running")
    check_nothing_waits(s, folder, "the file once the PC can say what it holds")
    s.check("the PC has the bytes of that file", pc_has_whole(s, folder, "old-pc.bin", old_pc))

    # Delivery from the background, with the app exempt from battery optimization and not.
    for exempt in (False, True):
        device.battery_exempt(exempt)
        device.clear_logcat()
        name = f"background-{'exempt' if exempt else 'optimized'}.bin"
        content = rng.randbytes(2 * MIB)
        share_one(s, name, content)
        device.home()
        # Nothing here asks the phone anything: the job has to get it across by itself.
        deliver(s, folder, name, content, f"{name} from the background", bound=SYNC_BOUND)
        wait_transfer_ends(device)
        s.check(f"a share reaches the PC from the background, exemption {'on' if exempt else 'off'}", True,
                device.transfer_log())

        # The same with a write through the provider: the app is cached by the time the job starts,
        # so this is where a foreground service may be refused to an app that is not exempt.
        device.clear_logcat()
        name = f"written-{'exempt' if exempt else 'optimized'}.bin"
        content = rng.randbytes(2 * MIB)
        device.content_write(device.create_document(folder.doc(), name), content)
        deliver(s, folder, name, content, f"{name} from the background", bound=SYNC_BOUND)
        wait_transfer_ends(device)
        s.check(f"a write reaches the PC from the background, exemption {'on' if exempt else 'off'}", True,
                device.transfer_log())
    device.battery_exempt(True)

    # A force-stop during the copy of a large file shared in. The grant does not survive a kill, so
    # the share is reported, not resumed: no real name appears, the working file is swept from both
    # sides, and the Folders screen says what happened.
    device.battery_exempt(True)
    device.clear_logcat()
    killed = random_bytes(rng, 448 * MIB)
    share_one(s, "kill-mid-copy.bin", killed)
    device.home()

    def parts() -> list[str]:
        return [name for name in device.raw_names(folder.id) if name.startswith(".tkfs-part-")]

    poll(SHARE_BOUND, "the working file under way", lambda: bool(parts()), interval=1)
    device.force_stop()
    s.check("force-stop mid-copy ended the app", device.app_pid() is None)
    s.check("no real name appears after the kill", "kill-mid-copy.bin" not in names(), sorted(names()))
    poll(120, "the working file swept from the phone", lambda: not parts(), interval=2)
    s.check("the working file is swept from the phone at the next start", True)
    s.check("and the provider never listed it", not any(n.startswith(".tkfs-part-") for n in names()))
    strip_is("kill-mid-copy.bin")
    s.check("the Folders screen says what the kill lost", True)
    device.tap_text("Dismiss")

    def gone_from_pc() -> bool:
        # A removed entry stays in the tree as a deleted one: that is how the removal syncs.
        tree = json.loads(pc.folder(folder.pc_state, "tree"))
        live = [entry["name"] for entry in tree.values() if not entry["deleted"]]
        return not any(name.startswith(".tkfs-part-") for name in live) and "kill-mid-copy.bin" not in live

    deliver_nothing = lambda: trigger_foreground(device)
    last_nudge = [0.0]

    def pc_clean() -> bool:
        if time.monotonic() - last_nudge[0] > 30:
            last_nudge[0] = time.monotonic()
            deliver_nothing()
        return gone_from_pc()

    poll(SYNC_BOUND, "the working file gone from the PC", pc_clean, interval=2)
    s.check("and its removal reached the PC: no working file and no real name there", True)

    # A force-stop during the push of a large file. The PC is away while it is shared in, so that
    # the copy is over, and the file whole on the phone, before anything is sent: a burst during
    # the copy would send its working file and leave no push to interrupt.
    large = random_bytes(rng, SHARE_BYTES)
    reference_repo, reference_state = pc.create_folder("Reference", {"large.bin": large})
    reference = Folder("", "Reference", reference_repo, "", reference_state, {"large.bin": large})
    failures, summary = pc.stop()
    s.check("the PC stops cleanly for the large share", not failures, failures or summary)
    device.clear_logcat()
    share_one(s, "large-push.bin", large)
    device.home()
    poll(SHARE_BOUND, "the large file whole on the phone", lambda: "large-push.bin" in names(), interval=5)
    s.check("the large file is on the phone, whole",
            names()["large-push.bin"]["_size"] == str(len(large)), names()["large-push.bin"])
    pc.start()
    pc.wait_phase(folder.label, "Running")
    before = pc_pack_bytes(folder)
    last = [0.0]

    def under_way() -> bool:
        if time.monotonic() - last[0] > 30:
            last[0] = time.monotonic()
            trigger_foreground(device)
        return pc_pack_bytes(folder) - before >= 16 * MIB

    poll(SHARE_BOUND, "the push under way", under_way, interval=1)
    gained = pc_pack_bytes(folder) - before
    s.check("the large file is only partly on the PC when the app is killed", gained < SHARE_BYTES * 0.9,
            f"{gained} of {SHARE_BYTES} pack bytes there")
    device.force_stop()
    s.check("force-stop mid-push ended the app", device.app_pid() is None)
    deliver(s, folder, "large-push.bin", reference, "the large file after a restart",
            nudge=lambda: trigger_foreground(device), ref_path="large.bin", packs_before=before)

    # A force-stop during the receive of a large file.
    held = device.pack_bytes(folder.state)
    outcome: list[str] = []

    def read() -> None:
        try:
            device.content_read(folder.doc("from-pc-large.bin"), bound=SHARE_BOUND)
            outcome.append("read")
        except HarnessError as error:
            outcome.append(f"stopped: {error}")

    reader = threading.Thread(target=read, daemon=True)
    reader.start()
    poll(SHARE_BOUND, "the receive under way", lambda: device.pack_bytes(folder.state) - held >= 16 * MIB, interval=1)
    s.check("the large file is only partly here when the app is killed",
            device.pack_bytes(folder.state) - held < SHARE_BYTES)
    device.force_stop()
    reader.join(60)
    s.check("force-stop mid-receive ended the app and the reader",
            device.app_pid() is None and not reader.is_alive(), outcome)
    again = device.content_read(folder.doc("from-pc-large.bin"), bound=SHARE_BOUND)
    s.check("the receive completes after a restart with identical bytes", sha256(again) == sha256(big_from_pc),
            f"{len(again)} bytes")


def scenario_metered(s: Session) -> None:
    """Acceptance 17: on a metered network, with "Sync on mobile data" off, opening the app brings the PC's
    names and nothing else. A file shared in stays waiting and moves no bytes, and opening a file that
    was never downloaded still downloads it, since the owner asked for that one.

    The network is made metered the way Settings does it for a Wi-Fi network (`cmd netpolicy set
    metered-network`), and the app is asked, through its own ConnectivityManager call, whether it sees it."""
    device, pc = s.device, s.pc
    rng = random.Random(17)
    from_pc = rng.randbytes(3 * MIB)
    folder = s.folder_with({"seed.txt": b"Here from the start.\n", "download-me.bin": from_pc})
    names = lambda: {row["_display_name"]: row for row in device.content_query(children_uri(folder.doc()))}
    waiting_one = lambda texts: any("1 file waiting to send" in text for text in texts)
    device.battery_exempt(True)
    try:
        s.check("the app does not see a metered network to begin with", not device.app_sees_metered())
        device.set_metered(True)
        s.check("the app sees the network as metered, by isActiveNetworkMetered", device.app_sees_metered())

        # Opening the app: the PC's new name comes, and no file does.
        before = device.pack_bytes(folder.state)
        pc.write_file(folder.pc_state, "name-from-pc.txt", b"Written on the PC.\n")
        pc.folder(folder.pc_state, "flush")
        trigger_foreground(device)
        poll(SYNC_BOUND, "the PC's new name on the phone", lambda: "name-from-pc.txt" in names(), interval=2)
        s.check("opening the app on a metered network brings the PC's new name", True)
        s.check("and the file itself was not downloaded", device.pack_bytes(folder.state) == before,
                f"{before} -> {device.pack_bytes(folder.state)}")

        # A shared file stays here: nothing is pushed.
        packs = pc_pack_bytes(folder)
        shared = rng.randbytes(3 * MIB)
        share_one(s, "metered-share.bin", shared)
        poll(SHARE_BOUND, "the shared file in the folder", lambda: "metered-share.bin" in names())
        device.start_app()
        device.wait_screen("one file waiting to send", waiting_one, 120)
        s.check("a file shared in on a metered network is waiting to send", True)
        # Long enough for the job's 5 s debounce and for three foreground bursts.
        for _ in range(3):
            time.sleep(15)
            trigger_foreground(device)
        s.check("the shared file moved no bytes: the PC's packs are as they were",
                pc_pack_bytes(folder) == packs, f"{packs} -> {pc_pack_bytes(folder)}")
        s.check("and it is still waiting",
                [file["path"] for file in device.waiting_files(folder.id)] == ["/metered-share.bin"],
                device.waiting_files(folder.id))
        device.wait_screen("still one file waiting", waiting_one, 120)
        s.check("the Folders screen still says so", True)

        # A file the owner opens downloads, whatever the network.
        held = device.pack_bytes(folder.state)
        read = device.content_read(folder.doc("download-me.bin"), bound=SYNC_BOUND)
        s.check("opening an undownloaded file on a metered network downloads it", sha256(read) == sha256(from_pc),
                f"{len(read)} bytes")
        s.check("and its bytes are on the phone now", device.pack_bytes(folder.state) - held >= len(from_pc))
        s.check("while the shared file still moved nothing", pc_pack_bytes(folder) == packs)
    finally:
        device.set_metered(False)
    s.check("the app sees the network as unmetered again", not device.app_sees_metered())

    # Back on an unmetered network the file goes by itself, and only then is it no longer waiting.
    deliver(s, folder, "metered-share.bin", shared, "the file shared on a metered network",
            nudge=lambda: trigger_foreground(device))
    check_nothing_waits(s, folder, "the shared file once the network allows it")


def scenario_transfer_timeout(s: Session) -> None:
    """Android 15 limits a `dataSync` service, and calls `Service.onTimeout` when the limit is reached.
    WorkManager maps that to a stop of the job. The limit is set to 20 s, and a job is held in its
    foreground state by writes that keep arriving: it must stop cleanly, the app must not crash, and
    the writes must still reach the PC."""
    device, pc = s.device, s.pc
    folder = s.folder_with({"seed.txt": b"Here from the start.\n"})
    device.battery_exempt(True)
    device.set_data_sync_timeout(20_000)
    try:
        device.clear_logcat()
        doc = device.create_document(folder.doc(), "held.txt", "text/plain")
        pid = device.app_pid()
        began = time.monotonic()
        count = 0
        # Each write is a local change, and the job waits for a quiet 5 s before its first burst.
        while time.monotonic() - began < 50:
            count += 1
            device.content_write(doc, f"Write number {count}.\n".encode())
            time.sleep(1.5)
        log = device.transfer_log()
        s.check("the job stopped when Android called onTimeout",
                any(line.startswith("transfer: stopped") for line in log), log)
        s.check("without the app crashing", device.app_pid() == pid and "FATAL" not in device.adb(
            "logcat", "-d", "-b", "crash").out, device.app_pid())
        final = f"Write number {count}.\n".encode()
        deliver(s, folder, "held.txt", final, "the last write after the stop", bound=SYNC_BOUND)
    finally:
        device.set_data_sync_timeout(None)


IMAGE_COUNT = 50
# Acceptance 15: browsing may move this much from the PC (events and the like), and no file contents.
BROWSE_METADATA_BOUND = 64 * 1024
THUMBNAIL_SIDE = 64
THUMBNAIL_FLAG = 1  # DocumentsContract.Document.FLAG_SUPPORTS_THUMBNAIL


def image_files() -> dict[str, bytes]:
    """`IMAGE_COUNT` real images, each a few chunks, so that no listing could hide a download."""
    return {f"images/img{index:02d}.png": png_noise(index) for index in range(IMAGE_COUNT)}


def check_browsing_the_images(s: Session, folder: Folder, opened: list[str]) -> None:
    """Acceptance 15, for a folder of images of which only `opened` were ever read: the listing and a
    thumbnail request for every image move no file contents, only the opened ones list as on the phone
    with a thumbnail offered, and the thumbnails are decoded down to the size asked.

    What the PC sent is measured by what reached the phone's packs: that is the one way a PC's file
    contents get there, and the PC offers no byte counter that does not also count the metadata. The
    PC's own request count is recorded beside it, for the report, not bounded."""
    device = s.device
    packs, requests = device.pack_bytes(folder.state), requests_to_pc(s.pc)
    tree = s.tree(folder)
    images = {path: row for path, row in tree.items() if path.startswith("images/") and "." in path}
    s.check("every image is listed", len(images) == IMAGE_COUNT, len(images))
    ids = [row["document_id"] for row in images.values()]
    offered = device.thumbnails(ids, THUMBNAIL_SIDE, THUMBNAIL_SIDE)
    moved, asked = device.pack_bytes(folder.state) - packs, requests_to_pc(s.pc) - requests
    s.check("listing and asking for thumbnails of every image sent no file contents",
            0 <= moved <= BROWSE_METADATA_BOUND, f"{moved} pack bytes, {asked} requests served")

    wrong_summary, wrong_thumbnail, wrong_size = [], [], []
    for path, row in images.items():
        local = path in opened
        if row["summary"] != ("On this phone" if local else "Not downloaded"):
            wrong_summary.append((path, row["summary"]))
        flagged = int(row["flags"]) & THUMBNAIL_FLAG != 0
        got = offered[row["document_id"]]
        if flagged != local or (got is not None) != local:
            wrong_thumbnail.append((path, flagged, got))
        if got is not None and not (THUMBNAIL_SIDE <= got["width"] < 2 * THUMBNAIL_SIDE
                                    and THUMBNAIL_SIDE <= got["height"] < 2 * THUMBNAIL_SIDE):
            wrong_size.append((path, got))
    s.check(f"exactly the {len(opened)} opened images say they are on the phone, and the rest that they are not downloaded",
            not wrong_summary, wrong_summary)
    s.check("and only those offer a thumbnail, by flag and by request", not wrong_thumbnail, wrong_thumbnail)
    s.check("a thumbnail is decoded down to the size asked, not to the full picture", not wrong_size, wrong_size)
    s.check(f"browsing {IMAGE_COUNT} images, as the PC saw it", True, f"{moved} pack bytes, {asked} requests served")


def check_offline_listing_is_quick(s: Session, folder: Folder) -> None:
    """With the PC gone a listing, a stat and a thumbnail request all answer at once, from what is here."""
    failures, summary = s.pc.stop()
    s.check("the PC stops cleanly", not failures, failures or summary)
    started = time.monotonic()
    tree = s.tree(folder)
    offered = s.device.thumbnails([row["document_id"] for path, row in tree.items() if path.startswith("images/") and "." in path],
                                  THUMBNAIL_SIDE, THUMBNAIL_SIDE)
    took = time.monotonic() - started
    s.check("with the PC away every listing and thumbnail request still answers", len(tree) == len(with_parents(folder.files))
            and len(offered) == IMAGE_COUNT, f"{len(tree)} documents, {len(offered)} thumbnails")
    # adb's own calls take a second or so each; a fetch that waited for the PC would take 20 s or more each.
    s.check("and answers at once", took < 20, f"{took:.1f} s")


def scenario_ui_browse(s: Session) -> None:
    """Acceptance 14 and 15 on screen: Browse lists a folder's files with what is on the phone and what
    waits, a tap opens a file in another app, a long press holds the actions, and a file the phone lacks
    fails fast and says so with no PC. Screenshots are in this run's `screens` directory."""
    device, pc = s.device, s.pc
    shots = s.report.path.parent / "screens"
    rng = random.Random(16)
    photo = png_noise(2041)
    video = rng.randbytes(8_120_000)
    report_pdf = rng.randbytes(2_300_000)
    files = {"IMG_2041.png": photo, "video-obra.mp4": video, "notes.txt": b"Notes.\n" * 20, "docs/sub.txt": b"Below a directory.\n"}
    folder = s.folder_with(files, "Transfer")
    device.shell("pm", "grant", PACKAGE, "android.permission.POST_NOTIFICATIONS").require_success()
    s.check("the photo reads on the phone", sha256(device.content_read(folder.doc("IMG_2041.png"))) == sha256(photo))

    def shown(what: str, wanted: Callable[[list[str]], bool], bound: float = UI_BOUND) -> list[str]:
        return device.wait_screen(what, wanted, bound)

    def has(*texts: str) -> Callable[[list[str]], bool]:
        return lambda found: all(text in found for text in texts)

    def names() -> set[str]:
        return {row["_display_name"] for row in device.content_query(children_uri(folder.doc()))}

    def clear_notifications() -> None:
        device.shell("cmd", "notification", "cancel_all", bound=ADB_BOUND)

    device.start_app()
    shown("the folder on Folders", has("Transfer"))
    device.tap_text("Transfer")
    photo_line = f"On this phone, {size_text(len(photo))}"
    video_line = f"Not downloaded, {size_text(len(video))}"
    found = shown("the files with what is on the phone", has("IMG_2041.png", photo_line, "video-obra.mp4", video_line, "docs", "notes.txt"))
    s.check("a file on the phone says so, and one never opened says it is not downloaded", True, found)
    device.screenshot(shots / "browse.png")

    # A file made on the phone, with no PC to take it: waiting to send.
    failures, summary = pc.stop()
    s.check("the PC stops cleanly", not failures, failures or summary)
    made = device.create_document(folder.doc(), "relatorio-outubro.pdf", "application/pdf")
    device.content_write(made, report_pdf)
    waiting_line = f"Waiting to send, {size_text(len(report_pdf))}"
    found = shown("the new file waiting to send", has("relatorio-outubro.pdf", waiting_line), 60)
    s.check("a file made on the phone and not yet sent says it is waiting", True, found)
    device.screenshot(shots / "browse-waiting.png")

    # The long press: the actions and their wording.
    device.long_press_text("IMG_2041.png")
    actions = ("Keep on this phone", "Remove download", "Share", "Rename", "Delete everywhere")
    found = shown("the actions", has("IMG_2041.png", *actions))
    s.check("the sheet holds the mockup's five actions, in its words", True, found)
    s.check("for a file that is on the phone, every one of them is on", all(device.is_enabled(a) for a in actions))
    device.screenshot(shots / "browse-sheet.png")
    device.shell("input", "keyevent", "KEYCODE_BACK").require_success()
    shown("the sheet gone", lambda t: "Keep on this phone" not in t)

    # A tap opens the file in another app: a viewer's intent, or if the phone has none, a sentence saying so.
    resolved = device.shell("cmd", "package", "resolve-activity", "--brief", "-a", "android.intent.action.VIEW", "-t", "image/png").out
    has_viewer = "/" in resolved.splitlines()[-1] if resolved.strip() else False
    device.tap_text("IMG_2041.png")
    if has_viewer:
        def viewed() -> bool:
            dump = device.shell("dumpsys", "activity", "activities").out
            return "android.intent.action.VIEW" in dump and "dev.tkfs.documents" in dump

        poll(UI_BOUND, "the viewer's intent", viewed, interval=1)
        s.check("a tap opens the file in another app, as a VIEW of the provider's URI", True, resolved.strip())
        device.screenshot(shots / "browse-viewer.png")
        device.shell("input", "keyevent", "KEYCODE_BACK").require_success()
    else:
        found = shown("the sentence for no app", has("No app on this phone can open IMG_2041.png."))
        s.check("with no app to open a file, the screen says so in words", True, found)
        device.tap_text("Dismiss")
    shown("Browse again", has("IMG_2041.png", "docs"))

    # A folder opens in place; a turn of the screen keeps where it is; back goes up.
    device.tap_text("docs")
    shown("the inside of docs", has("sub.txt"))
    device.rotate(landscape=True)
    shown("docs after the screen turned", has("sub.txt"))
    s.check("the path survives a rotation", True)
    device.screenshot(shots / "browse-docs-landscape.png")
    device.rotate(landscape=False)
    shown("docs after the screen turned back", has("sub.txt"))
    # The window is recreated by a turn, and a key sent in the middle of it is lost.
    time.sleep(2)
    device.shell("input", "keyevent", "KEYCODE_BACK").require_success()
    shown("the folder's top level again", has("IMG_2041.png", "video-obra.mp4"))
    s.check("back goes up a directory before it leaves", True)

    # Rename validates as the provider does.
    device.long_press_text("notes.txt")
    shown("the sheet", has("Rename"))
    device.tap_text("Rename")
    shown("the Rename dialog", has("Cancel"))
    device.clear_field()
    device.shell("input", "text", "a/b").require_success()
    found = shown("the refusal of a bad name", lambda t: any(text.startswith("A name cannot be empty") for text in t))
    s.check("a name with a separator is refused on the spot, and cannot be confirmed", not device.is_enabled("Rename", last=True), found)
    device.screenshot(shots / "browse-rename-refused.png")
    device.clear_field()
    device.shell("input", "text", "renamed.txt").require_success()
    device.tap_button("Rename")
    shown("the new name", lambda t: "renamed.txt" in t and "notes.txt" not in t)
    s.check("a rename shows on the screen", True)
    s.check("and in the provider", "renamed.txt" in names() and "notes.txt" not in names(), sorted(names()))

    # Delete everywhere asks first.
    device.long_press_text("renamed.txt")
    shown("the sheet", has("Delete everywhere"))
    device.tap_text("Delete everywhere")
    found = shown("the confirmation", has("Delete renamed.txt everywhere?", "Cancel"))
    device.screenshot(shots / "browse-delete.png")
    device.tap_text("Cancel")
    shown("the file still there", has("renamed.txt"))
    s.check("cancelling the confirmation deletes nothing", "renamed.txt" in names(), sorted(names()))
    device.long_press_text("renamed.txt")
    device.tap_text("Delete everywhere")
    shown("the confirmation", has("Delete renamed.txt everywhere?"))
    device.tap_button("Delete everywhere")
    shown("the file gone", lambda t: "renamed.txt" not in t and "IMG_2041.png" in t)
    s.check("confirming deletes it, here at once", "renamed.txt" not in names(), sorted(names()))

    # A file the phone never opened, with no PC: it fails within the bound and says why, with and
    # without the notification permission.
    unreachable = "My PC isn't reachable, so {} can't open yet."
    clear_notifications()
    device.shell("pm", "revoke", PACKAGE, "android.permission.POST_NOTIFICATIONS").require_success()
    # A revoke can end the app's process; the timing is of the open, not of the app's start.
    device.content_query(ROOTS_URI)
    started = time.monotonic()
    refused = refusal(lambda: device.content_read(folder.doc("video-obra.mp4")))
    took = time.monotonic() - started
    s.check("opening a file never opened, with no PC, fails with PC_UNREACHABLE", refused.startswith("PC_UNREACHABLE") or "PC_UNREACHABLE" in refused, refused)
    s.check("and does so within 5 s", took < 5, f"{took:.1f} s")
    s.check("with the permission denied, no notification said so",
            "video-obra.mp4 can't open yet" not in device.shell("dumpsys", "notification", "--noredact").out)
    device.shell("pm", "grant", PACKAGE, "android.permission.POST_NOTIFICATIONS").require_success()
    started = time.monotonic()
    refused = refusal(lambda: device.content_read(folder.doc("docs/sub.txt")))
    took = time.monotonic() - started
    s.check("a small file never opened fails the same way", "PC_UNREACHABLE" in refused and took < 5, f"{refused} in {took:.1f} s")

    def notified() -> bool:
        return unreachable.format("sub.txt") in device.shell("dumpsys", "notification", "--noredact").out

    poll(30, "the notification that names the file", notified, interval=1)
    s.check("a notification names the file: My PC isn't reachable, so sub.txt can't open yet.", True)

    # The Folders screen says so for both, until dismissed, whatever the permission.
    device.start_app()
    found = shown("the strip", lambda t: any("video-obra.mp4" in text and "can't open yet" in text for text in t))
    s.check("the Folders screen names the files that could not open", any("sub.txt" in text for text in found), found)
    device.screenshot(shots / "folders-open-failed.png")
    device.tap_text("Dismiss")
    shown("the strip gone", lambda t: not any("can't open yet" in text for text in t))
    s.check("and the strip goes when dismissed", True)

    # The PC is back: a burst lists what it has again without leaving the screen.
    pc.start()
    pc.wait_phase("Transfer", "Running")
    pc.write_file(folder.pc_state, "later.txt", b"Written while the screen was open.\n")
    pc.folder(folder.pc_state, "flush")
    device.tap_text("Transfer")
    shown("Browse", has("IMG_2041.png"))
    s.sync_with_the_pc_back(folder)
    shown("the file the burst brought", has("later.txt"), SYNC_BOUND)
    s.check("after a burst the listing shows what it brought", True)
    device.screenshot(shots / "browse-after-burst.png")
    video_read = device.content_read(folder.doc("video-obra.mp4"))
    s.check("with the PC back the file opens and reads whole", sha256(video_read) == sha256(video))


def conflict_heads(rows: list[dict]) -> list[str]:
    """The unresolved conflicts' entities, from the rows of a `conflicts` op."""
    return [row["entity"] for row in rows if not row.get("resolved")]


def phone_conflicts(s: Session, folder: Folder) -> list[dict]:
    return s.device.shell_manage("folder", {"state": folder.id, "op": "conflicts", "payload": {}})


def pc_conflicts(s: Session, folder: Folder) -> list[dict]:
    return json.loads(s.pc.folder(folder.pc_state, "conflicts"))


def tap_below(device: Device, line: str, button: str, bound: float = UI_BOUND) -> None:
    """Taps the first `button` under the view whose text starts with `line`: each version of a conflict has its own."""

    def find():
        nodes = list(device.ui_dump(bound).iter("node"))
        above = [bounds_center(n.get("bounds"))[1] for n in nodes if (n.get("text") or "").startswith(line)]
        below = [bounds_center(n.get("bounds")) for n in nodes if n.get("text") == button
                 and above and bounds_center(n.get("bounds"))[1] > above[0]]
        if not below:
            raise HarnessError(f"no {button!r} under {line!r}")
        return min(below, key=lambda point: point[1])

    x, y = poll(bound, f"{button!r} under {line!r}", find, interval=1)
    device.shell("input", "tap", x, y).require_success()


def scenario_conflicts(s: Session) -> None:
    """Acceptance 18: a file edited on the PC and on the phone between bursts is a conflict. The phone says
    "1 conflict to settle", the Conflicts screen shows both versions and previews each, and keeping one settles
    it on both sides after a burst, with the chosen bytes. Screenshots are in this run's `screens` directory."""
    device, pc = s.device, s.pc
    shots = s.report.path.parent / "screens"
    base = b"Agreed text.\n" * 50
    pc_bytes = b"Edited on the PC.\n" * 50
    phone_bytes = b"Edited on the phone, differently.\n" * 60
    folder = s.folder_with({"contrato.txt": base, "quieto.txt": b"Never edited.\n"}, "Contracts")
    s.check("the phone reads the file before either side edits it", device.content_read(folder.doc("contrato.txt")) == base)
    s.check("no conflict to begin with", not conflict_heads(phone_conflicts(s, folder)) and not conflict_heads(pc_conflicts(s, folder)))

    # Both sides edit between bursts: the PC through its folder, the phone through the provider.
    pc.write_file(folder.pc_state, "contrato.txt", pc_bytes)
    pc.folder(folder.pc_state, "flush")
    device.content_write(folder.doc("contrato.txt"), phone_bytes)

    def both_see_it() -> bool:
        s.sync_now(folder)
        return bool(conflict_heads(phone_conflicts(s, folder))) and bool(conflict_heads(pc_conflicts(s, folder)))

    poll(SYNC_BOUND, "the conflict on both sides", both_see_it, interval=2)
    s.check("after a burst each side holds the conflict", True)

    def shown(what: str, wanted: Callable[[list[str]], bool], bound: float = UI_BOUND) -> list[str]:
        return device.wait_screen(what, wanted, bound)

    def has(*texts: str) -> Callable[[list[str]], bool]:
        return lambda found: all(text in found for text in texts)

    device.start_app()
    found = shown("the conflicts line", has("Contracts", "1 conflict to settle"))
    s.check("the Folders screen says 1 conflict to settle", True, found)
    device.screenshot(shots / "conflicts-folders.png")
    device.tap_text("1 conflict to settle")
    found = shown("both versions", has("Both versions stay until you choose.", "contrato.txt"))
    phone_line = f"This phone, Today at"
    s.check("the screen names the file and both versions", sum(text.startswith(("This phone, Today", "My PC, Today")) for text in found) == 2, found)
    s.check("with the sizes of each", any(text.endswith(size_text(len(phone_bytes))) for text in found)
            and any(text.endswith(size_text(len(pc_bytes))) for text in found), found)
    device.screenshot(shots / "conflicts.png")

    # Preview the PC's version: its first bytes are fetched, off the main thread, and read in a dialog.
    tap_below(device, "My PC, Today", "Preview")
    found = shown("the preview", lambda t: any("Edited on the PC." in text for text in t))
    s.check("the preview of the PC's version shows its text", True)
    device.screenshot(shots / "conflicts-preview.png")
    device.tap_text("Close")
    shown("the preview closed", lambda t: "Close" not in t)
    s.check("nothing is settled by previewing", bool(conflict_heads(phone_conflicts(s, folder))))

    # Keep the phone's version.
    tap_below(device, phone_line, "Keep this one")

    def settled() -> bool:
        return not conflict_heads(phone_conflicts(s, folder))

    poll(UI_BOUND, "the phone to settle", settled, interval=1)
    s.check("keeping a version settles the conflict on the phone", True)
    s.check("and the file holds the chosen bytes there", device.content_read(folder.doc("contrato.txt")) == phone_bytes)

    def pc_settled() -> bool:
        s.sync_now(folder)
        return not conflict_heads(pc_conflicts(s, folder)) and pc.pc_cat(folder.pc_state, "contrato.txt") == phone_bytes

    poll(SYNC_BOUND, "the choice on the PC", pc_settled, interval=2)
    s.check("after a burst the PC has the chosen bytes and no conflict", True)
    s.check("and the phone still has none", not conflict_heads(phone_conflicts(s, folder)))
    found = shown("nothing left to settle", lambda t: "Nothing to settle in Contracts." in t, 60)
    s.check("the screen says nothing is left", True, found)
    device.screenshot(shots / "conflicts-settled.png")
    device.shell("input", "keyevent", "KEYCODE_BACK").require_success()
    found = shown("Folders without the line", lambda t: "Contracts" in t and not any("conflict" in text for text in t), 60)
    s.check("and the Folders screen no longer shows a conflict", True, found)


# ---- keeping and freeing space ----


SPACE_FILE_BYTES = 64 * MIB
# How far below the floor it is filled: more than two of those files and less than three.
BELOW_THE_FLOOR = 130 * MIB
LOW_ON_SPACE = "Low on space. Can't free anything until My PC is reachable."
KEPT_DOES_NOT_FIT = "Low on space. Not everything you marked to keep fits on this phone."
BACK_IN_SPACE = 256 * MIB


def scenario_space(s: Session) -> None:
    """Acceptance 16 and Review Focus 4. Files are downloaded and marked, /data is filled with `dd` in
    the shared storage it shares a partition with, and a burst frees what it may, oldest first. With
    the PC away nothing leaves and the Folders screen says why. Then the volume is filled to under
    1 MiB: a write and a receive fail and say so, nothing is corrupted, and once the filler goes they
    both work with no folder in need of repair. The filler goes whatever happens: other scenarios run
    on this AVD after this one."""
    device = s.device
    rng = random.Random(19)
    files = {name: random_bytes(rng, SPACE_FILE_BYTES) for name in ("kept.bin", "a.bin", "b.bin", "c.bin", "d.bin")}
    files |= {
        "dir/inside.bin": rng.randbytes(3 * MIB),
        "not-here.bin": rng.randbytes(8 * MIB),
        "good.txt": b"The good contents of this file.\n" * 100,
        "no-room.bin": rng.randbytes(MIB),
    }
    folder = s.folder_with(files, "Space")
    before = device.free_bytes()
    try:
        check_keeping(s, folder)
        check_freeing(s, folder)
        check_freeing_with_the_pc_away(s, folder)
        check_what_is_kept_does_not_fit(s, folder)
        check_a_full_disk(s, folder)
    finally:
        # Each on its own: one failing must not leave the other undone, or hide what ended the scenario.
        try:
            device.remove_filler()
            s.report.record("the filler is gone", not device.fillers(), device.fillers())
        except HarnessError as failure:
            s.report.record("the filler is gone", False, str(failure))
        try:
            device.set_metered(False)
        except HarnessError as failure:
            s.report.record("the network is no longer metered", False, str(failure))
    after = device.free_bytes()
    s.check("the AVD has its space back, less what the app holds now", after >= before - 512 * MIB, f"{before} -> {after}")


def entry_in(s: Session, folder: Folder, path: str) -> dict:
    """What `Native.list` says of the file. A listing through the provider could start a burst, and a burst frees."""
    parent, _, name = path.rpartition("/")
    for entry in s.device.entries(folder.id, "/" + parent):
        if entry["name"] == name:
            return entry
    raise HarnessError(f"{path} is not listed")


def is_here(s: Session, folder: Folder, path: str) -> bool:
    entry = entry_in(s, folder, path)
    return entry["local_bytes"] >= entry["size"]


def what_is_here(s: Session, folder: Folder, paths) -> dict[str, int]:
    return {path: entry_in(s, folder, path)["local_bytes"] for path in paths}


def press_action(s: Session, name: str, action: str) -> list[str]:
    """Long-presses `name` on Browse and taps `action` in the sheet."""
    device = s.device
    device.long_press_text(name)
    device.wait_screen(f"the sheet of {name}", lambda t: action in t)
    device.tap_text(action)
    return device.wait_screen("the sheet gone", lambda t: action not in t)


def to_folders(s: Session) -> None:
    """The Folders screen, from wherever the app is."""
    device = s.device
    device.start_app()
    for _ in range(5):
        if "Sync now" in device.screen_texts():
            return
        device.shell("input", "keyevent", "KEYCODE_BACK").require_success()
        time.sleep(1)
    device.start_app()
    device.wait_screen("the Folders screen", lambda t: "Sync now" in t)


def check_keeping(s: Session, folder: Folder) -> None:
    """Marks on the screen, and a job that downloads what they cover, a folder's later arrivals too."""
    device, pc = s.device, s.pc
    shots = s.report.path.parent / "screens"
    # In this order, so that the file marked is the one opened longest ago.
    for name in ("kept.bin", "a.bin", "b.bin", "c.bin", "d.bin"):
        digest = device.content_digest(folder.doc(name))
        s.check(f"{name} downloads whole", digest == sha256(folder.files[name]), digest)
    s.check("what was not opened is not here", not is_here(s, folder, "dir/inside.bin"))
    device.start_app()
    device.wait_screen("the folder on Folders", lambda t: "Space" in t)
    device.tap_text("Space")
    device.wait_screen("the files", lambda t: "kept.bin" in t and "dir" in t)
    press_action(s, "kept.bin", "Keep on this phone")
    kept_line = f"Kept on this phone, {size_text(SPACE_FILE_BYTES)}"
    device.wait_screen("the kept file", lambda t: kept_line in t)
    s.check("a kept file says so on Browse", True, kept_line)
    device.screenshot(shots / "space-kept.png")
    device.long_press_text("kept.bin")
    device.wait_screen("the sheet", lambda t: "Stop keeping on this phone" in t)
    s.check("a kept file's download cannot be removed until it stops being kept", not device.is_enabled("Remove download"))
    device.shell("input", "keyevent", "KEYCODE_BACK").require_success()
    device.wait_screen("the sheet gone", lambda t: "Stop keeping on this phone" not in t)

    # A marked folder's files come in the transfer job, with nothing opening them.
    press_action(s, "dir", "Keep on this phone")
    poll(SYNC_BOUND, "the marked folder's file on the phone", lambda: is_here(s, folder, "dir/inside.bin"), interval=2)
    s.check("what a marked folder holds downloads by itself", True)
    # And what later arrives in it.
    arrives = random.Random(20).randbytes(2 * MIB)
    pc.write_file(folder.pc_state, "dir/arrives.bin", arrives)
    pc.folder(folder.pc_state, "flush")
    to_folders(s)
    device.tap_text("Sync now")
    poll(SYNC_BOUND, "the later arrival on the phone", lambda: is_here(s, folder, "dir/arrives.bin"), interval=2)
    s.check("a marked folder covers what arrives later", entry_in(s, folder, "dir/arrives.bin")["kept"])
    digest = device.content_digest(folder.doc("dir/arrives.bin"))
    s.check("and it reads the same", digest == sha256(arrives), digest)


def check_freeing(s: Session, folder: Folder) -> None:
    """Below the floor at the end of a burst: unmarked files go oldest first, up to what is needed. On a
    metered network the burst takes only names, so a new file has a row here and its bytes nowhere else."""
    device = s.device
    device.set_metered(True)
    unsent = random.Random(21).randbytes(8 * MIB)
    made = device.create_document(folder.doc(), "unsent.bin")
    device.content_write(made, unsent)
    # The table takes in what this phone made when it is next asked.
    poll(ADB_BOUND, "the new file waiting", lambda: device.confirm(folder.id)["waiting"] == 1, interval=2)
    s.check("the new file is waiting to send", [f["path"] for f in device.waiting_files(folder.id)] == ["/unsent.bin"])

    floor = device.space()["floor"]
    free = device.fill_until_free(floor - BELOW_THE_FLOOR)
    s.check("the filler takes free space below the floor", free < floor, f"{free} free, floor {floor}")
    held = what_is_here(s, folder, ("kept.bin", "a.bin", "b.bin", "c.bin", "d.bin", "unsent.bin"))
    trigger_foreground(device)
    poll(SYNC_BOUND, "the oldest files freed", lambda: not any(what_is_here(s, folder, ("a.bin", "b.bin", "c.bin")).values()), interval=2)
    s.check("the unmarked files opened longest ago are gone from the phone", True)
    now = what_is_here(s, folder, held)
    s.check("the next one stays, as enough was freed", now["d.bin"] == held["d.bin"], now)
    s.check("the marked file stays, though it was opened before them all", now["kept.bin"] == held["kept.bin"], now)
    s.check("a file not yet on the PC stays", now["unsent.bin"] == held["unsent.bin"], now)
    s.check("and it is still waiting", [f["path"] for f in device.waiting_files(folder.id)] == ["/unsent.bin"])
    free = poll(ADB_BOUND, "free space above the floor", lambda: (found := device.free_bytes()) >= floor and found)
    s.check("free space is above the floor again", True, f"{free} free, floor {floor}")
    s.check("nothing says it cannot free", device.space()["blocked"] is None, device.space())

    # What left comes back from the PC, and is the same.
    for name in ("a.bin", "b.bin", "c.bin"):
        digest = device.content_digest(folder.doc(name))
        s.check(f"{name}, freed, reads back identically", digest == sha256(folder.files[name]), digest)
    s.check("that took it below the floor again", device.free_bytes() < floor, device.free_bytes())


def check_freeing_with_the_pc_away(s: Session, folder: Folder) -> None:
    """Nothing leaves, the Folders screen says why, and the actions on one file each say why."""
    device, pc = s.device, s.pc
    shots = s.report.path.parent / "screens"
    names = ("kept.bin", "a.bin", "b.bin", "c.bin", "d.bin", "unsent.bin")
    held = what_is_here(s, folder, names)
    failures, summary = pc.stop()
    s.check("the PC stops cleanly", not failures, failures or summary)
    to_folders(s)
    device.tap_text("Sync now")
    found = device.wait_screen("the strip that says why", lambda t: LOW_ON_SPACE in t, SYNC_BOUND)
    s.check("offline and low on space, the Folders screen says why", True, found)
    device.screenshot(shots / "space-offline.png")
    now = what_is_here(s, folder, names)
    s.check("offline, nothing was freed", now == held, now)
    s.check("the app says the same", device.space()["blocked"] == "PC_UNREACHABLE", device.space())

    device.tap_text("Space")
    device.wait_screen("the files", lambda t: "a.bin" in t)
    press_action(s, "a.bin", "Remove download")
    away = "My PC isn't reachable, so the download of a.bin can't be removed yet."
    device.wait_screen("the sentence for a PC that is away", lambda t: away in t, UI_BOUND)
    s.check("Remove download with the PC away says so", True, away)
    device.tap_text("Dismiss")
    press_action(s, "unsent.bin", "Remove download")
    not_there = "Not on My PC yet, so it can't be removed."
    device.wait_screen("the sentence for a file not on the PC", lambda t: not_there in t, UI_BOUND)
    s.check("Remove download of a file not on the PC says so", True, not_there)
    device.screenshot(shots / "space-not-on-pc.png")
    device.tap_text("Dismiss")
    now = what_is_here(s, folder, names)
    s.check("and neither was removed", now == held, now)

    # On Settings: what it keeps free, and the same refusal.
    to_folders(s)
    device.tap_text("Settings")
    device.wait_screen("the row", lambda t: "Keep this much free" in t and "Free up space now" in t)
    floor_line = next(text for text in device.screen_texts() if text.endswith("Oldest downloads go first."))
    s.check("Settings says how much it keeps free", floor_line == f"{size_text(device.space()['floor'])}. Oldest downloads go first.", floor_line)
    device.tap_text("Free up space now")
    found = device.wait_screen("what freeing did", lambda t: "Can't free more until My PC is reachable." in t, SYNC_BOUND)
    s.check("Free up space now says why it could not", True, found)
    device.screenshot(shots / "space-settings.png")

    # The PC is back: what was held back goes, the file reaches it, and the strip goes by itself.
    pc.start()
    pc.wait_phase("Space", "Running")
    device.set_metered(False)
    s.sync_with_the_pc_back(folder)
    poll(SYNC_BOUND, "the new file confirmed", lambda: device.confirm(folder.id)["waiting"] == 0, interval=2)
    to_folders(s)
    device.tap_text("Sync now")
    device.wait_screen("the strip gone", lambda t: LOW_ON_SPACE not in t and "Sync now" in t, SYNC_BOUND)
    floor = device.space()["floor"]
    poll(SYNC_BOUND, "free space above the floor", lambda: device.free_bytes() >= floor, interval=2)
    s.check("with the PC back it frees, and the strip goes", is_here(s, folder, "kept.bin"), device.free_bytes())

    # Remove download works now, on what could not be removed.
    device.tap_text("Space")
    device.wait_screen("the files", lambda t: "c.bin" in t)
    s.check("c.bin is still here to remove", entry_in(s, folder, "c.bin")["local_bytes"] > 0, entry_in(s, folder, "c.bin"))
    press_action(s, "c.bin", "Remove download")
    poll(SYNC_BOUND, "the download removed", lambda: entry_in(s, folder, "c.bin")["local_bytes"] == 0, interval=2)
    s.check("with the PC back, a file's download can be removed", True)
    device.wait_screen("the row", lambda t: f"Not downloaded, {size_text(SPACE_FILE_BYTES)}" in t)
    device.screenshot(shots / "space-removed.png")
    digest = device.content_digest(folder.doc("unsent.bin"))
    s.check("and the new file reads the same, whether or not it was freed", digest == sha256(random.Random(21).randbytes(8 * MIB)), digest)
    to_folders(s)


def check_what_is_kept_does_not_fit(s: Session, folder: Folder) -> None:
    """Below the floor with nothing left to free, a marked file does not download towards a full disk,
    and the Folders screen says so. It comes once there is room."""
    device = s.device
    shots = s.report.path.parent / "screens"
    floor = device.space()["floor"]
    # Further below the floor than all that could be freed comes to.
    device.fill_until_free(floor - 768 * MIB)
    to_folders(s)
    device.tap_text("Space")
    device.wait_screen("the files", lambda t: "no-room.bin" in t)
    press_action(s, "no-room.bin", "Keep on this phone")
    to_folders(s)
    device.tap_text("Sync now")
    found = device.wait_screen("the strip that says so", lambda t: KEPT_DOES_NOT_FIT in t, SYNC_BOUND)
    s.check("what is kept and does not fit says so on the Folders screen", True, found)
    device.screenshot(shots / "space-kept-does-not-fit.png")
    # Long enough for a job to have started, had one been asked for.
    time.sleep(2 * SYNC_SETTLE)
    s.check("and does not download below the floor", entry_in(s, folder, "no-room.bin")["local_bytes"] == 0,
            entry_in(s, folder, "no-room.bin"))
    s.check("the rest of what is kept stayed", is_here(s, folder, "kept.bin"))

    device.remove_filler()
    device.tap_text("Sync now")
    poll(SYNC_BOUND, "the kept file on the phone once there is room", lambda: is_here(s, folder, "no-room.bin"), interval=2)
    device.wait_screen("the strip gone", lambda t: KEPT_DOES_NOT_FIT not in t and "Sync now" in t, SYNC_BOUND)
    s.check("once there is room it comes, and the strip goes", True)


def freeing_answer(device: Device) -> dict | str:
    """What "Free up space now" did, or why it could not."""
    try:
        return device.free_now()
    except ManageError as error:
        return str(error)


def read_refused(device: Device, doc: str) -> str:
    """Why a read of the document failed. A read that gives bytes, the right ones or not, refused nothing."""
    return refusal(lambda: device.content_digest(doc))


def check_a_full_disk(s: Session, folder: Folder) -> None:
    """Review Focus 4. /data is filled until an app has less than 1 MiB of it, with what is reserved
    for the system left to the system, which runs on it. Nothing here may start a burst: at its end
    the app would free space, so the file listings are through `entry_in` and a document's own row."""
    device, pc = s.device, s.pc
    good = folder.doc("good.txt")
    payload = random.Random(22).randbytes(16 * MIB)
    fresh = device.create_document(folder.doc(), "fresh.bin")
    late = device.create_document(folder.doc(), "late.bin")
    s.check("good.txt is on the phone", device.content_digest(good) == sha256(folder.files["good.txt"]))
    s.check("not-here.bin is not", not entry_in(s, folder, "not-here.bin")["local_bytes"])
    # On /data, so before there is no room for it.
    upload = device.push_upload(payload)
    # What the creates above started, and its floor check, come before the disk fills.
    poll(SYNC_BOUND, "the new files on the PC", lambda: pc.pc_has(folder.pc_state, "late.bin"), interval=2)
    time.sleep(2 * SYNC_SETTLE)

    def on_the_pc() -> bool:
        s.sync_with_the_pc_back(folder)
        return device.confirm(folder.id)["waiting"] == 0

    # Room for the write and not for its seal. Nothing is left to tell by then, and nothing is lost:
    # it reads whole from its draft, and seals once there is room.
    device.fill_until_free(24 * MIB)
    device.content_write_upload(late, upload)
    # A second save onto that draft, with no room for this one either: it must not take the first with it.
    # If the app has found room by now this one works, and the file still ends up with these same bytes.
    s.report.record("a second save with no room answers", True, refusal(lambda: device.content_write_upload(late, upload))[:200])
    s.check("a write with no room for its seal reads back whole", device.content_digest(late) == sha256(payload))
    device.remove_filler()
    poll(SYNC_BOUND, "that write confirmed by the PC", on_the_pc, interval=2)
    s.check("and it seals and reaches the PC once there is room", True)
    # Its burst's floor check comes before the next fill.
    time.sleep(2 * SYNC_SETTLE)
    names = set(device.raw_names(folder.id))

    # Room for part of it: the write gets some way and then fails.
    free = device.fill_until_free(6 * MIB)
    s.check("there is less room than the write needs", free < len(payload), f"{free} bytes")
    refused = refusal(lambda: device.content_write_upload(good, upload))
    s.check("a write that runs out of room partway returns an error", bool(refused), refused)
    s.check("and the file holds all of what it held", device.content_digest(good) == sha256(folder.files["good.txt"]))
    row = device.content_query(document_uri(good))[0]
    s.check("at its old size", row["_size"] == str(len(folder.files["good.txt"])), row)
    s.check("what it wrote does not stay to take up the room", device.free_bytes() >= free - MIB, device.free_bytes())
    # Freeing has a little room to work in, where the floor would have left it plenty. A rewrite needs room
    # for what else is in a segment, so it may do nothing. It must say so, and break nothing.
    s.report.record("with a few MiB free, freeing answers", True, freeing_answer(device))
    # It may have taken this file with the rest, and what comes next needs it here.
    s.check("the file reads the same after that", device.content_digest(good) == sha256(folder.files["good.txt"]))
    # Hardly any room at all.
    free = device.fill_until_free(0)
    s.check("there is less than 1 MiB free", free < MIB, f"{free} bytes")
    refused = refusal(lambda: device.content_write_upload(good, upload))
    s.check("a write onto a file with a full disk returns an error", bool(refused), refused)
    refused = refusal(lambda: device.content_write_upload(fresh, upload))
    s.check("so does one onto an empty file", bool(refused), refused)
    s.check("the file holds what it held", device.content_digest(good) == sha256(folder.files["good.txt"]))
    row = device.content_query(document_uri(good))[0]
    s.check("at its old size", row["_size"] == str(len(folder.files["good.txt"])), row)
    s.check("no partial file shows", entry_in(s, folder, "fresh.bin")["size"] == 0, entry_in(s, folder, "fresh.bin"))
    s.check("and no other name", set(device.raw_names(folder.id)) == names, device.raw_names(folder.id))

    refused = read_refused(device, folder.doc("not-here.bin"))
    s.check("a receive with a full disk reports an error", bool(refused), refused)
    # A full burst, and freeing, with no room at all: they may do nothing, and must say so.
    pc.write_file(folder.pc_state, "while-full.txt", b"Written while the phone was full.\n")
    pc.folder(folder.pc_state, "flush")
    try:
        burst = device.shell_manage("sync-now", {"state": folder.id}, bound=SYNC_BOUND)["folders"][0]
    except ManageError as error:
        burst = str(error)
    s.report.record("a burst with a full disk answers", True, burst)
    # Android clears other apps' caches on its own when it is this full, so this is only what the next step finds.
    s.report.record("what is free for the step after", True, device.free_bytes())
    # With next to no room, the same.
    s.report.record("freeing with a full disk answers", True, freeing_answer(device))

    device.remove_filler()
    free = device.free_bytes()
    s.check("the filler is removed, and there is room again", free > BACK_IN_SPACE, f"{free} bytes")
    device.content_write_upload(good, upload)
    s.check("a write works once space returns", device.content_digest(good) == sha256(payload))
    device.content_write_upload(fresh, upload)
    s.check("onto the other file too", device.content_digest(fresh) == sha256(payload))
    digest = poll(SYNC_BOUND, "the receive to work", lambda: device.content_digest(folder.doc("not-here.bin")), interval=2)
    s.check("a receive works once space returns", digest == sha256(folder.files["not-here.bin"]), digest)
    s.sync_with_the_pc_back(folder)
    poll(SYNC_BOUND, "what was written while full", lambda: "while-full.txt" in device.raw_names(folder.id), interval=2)
    s.check("the burst it could not finish finishes", True)
    poll(SYNC_BOUND, "the new contents confirmed by the PC", on_the_pc, interval=2)
    content = json.loads(pc.pc_content(folder.pc_state, "good.txt"))
    s.check("the PC has the file at its new size", content["total_len"] == len(payload), content)

    # What freeing with no room did, it left readable.
    for name in ("kept.bin", "a.bin", "d.bin"):
        digest = device.content_digest(folder.doc(name))
        s.check(f"{name} reads the same after all that", digest == sha256(folder.files[name]), digest)

    # No folder needed repair: running with no error, also from a cold start, and nothing set aside.
    device.shell("rm", "-f", upload)
    s.check("nothing panicked", "panic" not in device.logged_errors("tkfs"), device.logged_errors("tkfs")[-500:])
    device.force_stop()
    digest = poll(FOLDER_READY_BOUND, "the folder after a cold start", lambda: device.content_digest(good), interval=2)
    s.check("from a cold start the file reads the same", digest == sha256(payload), digest)
    inspected = device.shell_manage("inspect", {"state": folder.id})
    s.check("the folder is running", inspected["phase"]["phase"] == "Running", inspected["phase"])
    status = [row for row in device.shell_manage("status")["folders"] if row["record"]["id"] == folder.id][0]
    s.check("with no error", status.get("error") is None, status.get("error"))
    drafts = device.shell("run-as", PACKAGE, "ls", "-a", f"{folder.state}/scratch").out.split()
    s.check("no draft was set aside, and no working file left", "quarantine" not in drafts
            and not any(name.endswith(".tmp") for name in drafts), drafts)


# Longer than the session's DEAD_AFTER (20 s): a link that was kept alive by nothing would have been called dead.
PAST_DEAD_AFTER = 30


def worker_of(device: Device, folder: Folder) -> dict:
    """Which worker is serving the folder: `inspect`'s instance, since `status` does not carry one, and when
    it started."""
    phase = device.shell_manage("inspect", {"state": folder.id})["phase"]
    return {key: phase.get(key) for key in ("phase", "instance", "since_ms")}


def scenario_freeze(s: Session) -> None:
    """Acceptance 14. With the app in the background and Android's freezer on it, past what a session would
    call dead, a file never opened opens with the right bytes. No worker restarted and the app is the
    same process, which a restart of either would break; nothing was fetched twice. Then a writable
    handle left open across the freeze goes on writing, once, and its bytes reach the PC."""
    device = s.device
    rng = random.Random(14)
    files = {"seen.bin": rng.randbytes(3 * MIB), "never-opened.bin": rng.randbytes(2 * MIB),
             "handle.txt": b"Held across the freeze.\n" * 50}
    folder = s.folder_with(files, "Freeze")
    seen = device.content_digest(folder.doc("seen.bin"))
    s.check("a file opened before the freeze reads whole", seen == sha256(files["seen.bin"]), seen)
    held = device.write_document(folder.doc("handle.txt"), "rw", b"BEFORE", close=False)
    s.check("a writable handle is left open", held == 6, held)

    device.start_app()
    device.wait_screen("the Folders screen", lambda t: "Sync now" in t)
    before = worker_of(device, folder)
    pid = device.app_pid()
    packs = device.pack_bytes(folder.state)
    s.check("the worker has an instance to compare", bool(before["instance"]) and before["phase"] == "Running", before)
    s.check("the app has a process", pid is not None, pid)

    device.home()
    took = device.wait_frozen(PACKAGE)
    s.check("Android freezes the app in the background", True, f"after {took:.0f} s")
    time.sleep(PAST_DEAD_AFTER)

    fresh = device.content_read(folder.doc("never-opened.bin"))
    s.check("a file never opened opens after the freeze with the right bytes",
            sha256(fresh) == sha256(files["never-opened.bin"]), f"{len(fresh)} bytes")
    again = device.content_read(folder.doc("seen.bin"))
    s.check("a file opened before it still reads right", sha256(again) == sha256(files["seen.bin"]))
    fetched = device.pack_bytes(folder.state) - packs
    size = len(files["never-opened.bin"])
    s.check("only the file never opened was fetched, once", size <= fetched < size * 1.5 + len(files["handle.txt"]),
            f"{fetched} pack bytes for {size}")
    s.check("no worker restarted", worker_of(device, folder) == before, f"{before} -> {worker_of(device, folder)}")
    s.check("and the app is the same process", device.app_pid() == pid, f"{pid} -> {device.app_pid()}")

    doc = folder.doc("handle.txt")
    released = device.releases(doc)
    device.write_document(doc, "rw", b"AFTER!", offset=6)
    device.wait_release(doc, released)
    want = b"BEFORE" + b"AFTER!" + files["handle.txt"][12:]
    s.check("a handle open across the freeze goes on writing", device.content_read(doc) == want)
    s.sync_with_the_pc_back(folder)
    on_pc = s.pc.pc_cat(folder.pc_state, "handle.txt")
    s.check("and what it wrote reaches the PC", on_pc == want, on_pc[:80])
    s.check("still the same worker and process", worker_of(device, folder) == before and device.app_pid() == pid)


def scenario_reinstall(s: Session) -> None:
    """Review Focus 3. `adb install -r` of the same APK over a phone with a folder, a pairing, a mark and a
    file waiting for a PC that is away keeps all four and the config on disk, and the file reaches the PC."""
    device, pc = s.device, s.pc
    files = {"marked.txt": b"Keep me on the phone.\n" * 40, "other.txt": b"Not marked.\n"}
    folder = s.folder_with(files, "Reinstall")
    device.start_app()
    device.wait_screen("the folder on Folders", lambda t: "Reinstall" in t)
    device.tap_text("Reinstall")
    device.wait_screen("the files", lambda t: "marked.txt" in t)
    press_action(s, "marked.txt", "Keep on this phone")
    poll(SYNC_BOUND, "the mark kept", lambda: entry_in(s, folder, "marked.txt")["kept"], interval=2)
    s.check("a file is marked to keep", True)

    failures, summary = pc.stop()
    s.check("the PC stops cleanly", not failures, failures or summary)
    waiting = b"Written on the phone, the PC away.\n" * 20
    made = device.create_document(folder.doc(), "waiting.txt")
    device.content_write(made, waiting)
    poll(ADB_BOUND, "the new file waiting", lambda: device.confirm(folder.id)["waiting"] == 1, interval=2)
    s.check("a file waits to send", [f["path"] for f in device.waiting_files(folder.id)] == ["/waiting.txt"])

    config = f"/data/user/0/{PACKAGE}/files/orchestrator.toml"
    stat = lambda: device.shell("run-as", PACKAGE, "stat", "-c", "%Y %s", config).require_success().out
    written, peers = stat(), device.shell_manage("status")["peers"]
    s.check("there is a pairing to keep", any(peer.get("endpoint") == pc.endpoint for peer in peers), [p.get("endpoint") for p in peers])
    time.sleep(1.1)  # stat's times have one-second steps.
    installed = device.app_pid()
    device.install(APK)
    s.check("the install ended the running app", device.app_pid() != installed or installed is None, installed)

    roots = device.content_query(ROOTS_URI)
    s.check("the folder is still there", [root["title"] for root in roots] == ["Reinstall"], roots)
    peers = device.shell_manage("status")["peers"]
    s.check("the pairing is still there", any(peer.get("endpoint") == pc.endpoint for peer in peers), [p.get("endpoint") for p in peers])
    s.check("the mark is still there", entry_in(s, folder, "marked.txt")["kept"])
    s.check("the file is still waiting", [f["path"] for f in device.waiting_files(folder.id)] == ["/waiting.txt"])
    s.check("and reads as it was", device.content_read(folder.doc("waiting.txt")) == waiting)
    s.check("the config on disk is the one from before", stat() == written, f"{written} -> {stat()}")

    pc.start()
    pc.wait_phase(folder.label, "Running")
    s.sync_with_the_pc_back(folder)
    on_pc = pc.pc_cat(folder.pc_state, "waiting.txt")
    s.check("the waiting file reaches the PC", on_pc == waiting, f"{len(on_pc)} of {len(waiting)} bytes")
    s.check("the marked file is still marked and the PC still has it", entry_in(s, folder, "marked.txt")["kept"]
            and pc.pc_cat(folder.pc_state, "marked.txt") == files["marked.txt"])


SCENARIOS: dict[str, Callable[[Session], None]] = {
    "freeze": scenario_freeze,
    "reinstall": scenario_reinstall,
    "ui_folders": scenario_ui_folders,
    "ui_browse": scenario_ui_browse,
    "conflicts": scenario_conflicts,
    "space": scenario_space,
    "read": scenario_read,
    "write": scenario_write,
    "share": scenario_share,
    "metered": scenario_metered,
    "transfer_timeout": scenario_transfer_timeout,
    "cold_start": scenario_cold_start,
    "harness": scenario_harness,
    "sync": scenario_sync,
    "speed": scenario_speed,
}

# A debug PC spends its time on its own hashing and copying, so these would measure that.
RELEASE_PC_SCENARIOS = {"speed"}


# ---- running ------------------------------------------------------------------------------


def chosen_scenarios(scenario: str) -> list[str]:
    return list(SCENARIOS) if scenario == "all" else [scenario]


def uses_release_pc(name: str, release_pc: bool) -> bool:
    return release_pc or name in RELEASE_PC_SCENARIOS


def build(run_dir: Path, names: list[str], release_pc: bool) -> None:
    """The x86_64 APK for the emulator and each tkfs the scenarios use on the PC, each logging
    to a file."""
    python = sys.executable
    run_bounded([python, ROOT / "scripts/build_android.py", "--abi", "x86_64"], BUILD_BOUND, cwd=ROOT,
                log=run_dir / "build-android.log").require_success()
    for release in sorted({uses_release_pc(name, release_pc) for name in names}):
        profile = ["--release"] if release else []
        log = run_dir / ("build-tkfs-release.log" if release else "build-tkfs.log")
        run_bounded(["cargo", "build", "--locked", *profile, "--bin", "tkfs"], BUILD_BOUND, cwd=ROOT,
                    log=log).require_success()


def tkfs_binary(release: bool = False) -> Path:
    return ROOT / "target" / ("release" if release else "debug") / ("tkfs.exe" if os.name == "nt" else "tkfs")


def prepare_device(device: Device, apk: Path) -> None:
    """Before anything needs the room: a hard kill of an earlier run can leave the `space` scenario's filler."""
    device.remove_filler()
    device.install(apk)


def run_scenario(name: str, device: Device, report: Report, run_dir: Path, reset: bool,
                 release_pc: bool = False) -> None:
    report.begin(name)
    pc = PcHost(tkfs_binary(uses_release_pc(name, release_pc)), run_dir / name)
    error = None
    try:
        device.clear_logcat()
        if reset:
            device.reset_app()
        # `metered` leaves the system's own judgment of the network in place; an earlier run that was
        # cut short may not have. A metered network would hold back every push in the scenario.
        device.set_metered(False)
        pc.start()
        session = Session(device, pc, report)
        session.check_pairing_qr()
        SCENARIOS[name](session)
    except HarnessError as failure:
        error = str(failure)
    except Exception:  # A bug here must still stop the PC and write the report.
        error = traceback.format_exc()
    finally:
        try:
            device.append_logcat(run_dir / "logcat.txt", name)
        except Exception as failure:
            report.fail(f"logcat: {failure}")
        try:
            failures, summary = pc.stop()
        except Exception as failure:
            failures, summary = [f"could not stop the PC: {failure}"], ""
        report.record("the PC orchestrator stops and leaves no process", not failures,
                      "; ".join(failures) or summary)
        report.end(error)


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--scenario", default="all",
                        help=f"{'|'.join(SCENARIOS)}|all (default: all)")
    parser.add_argument("--avd", default=DEFAULT_AVD)
    parser.add_argument("--keep-emulator", action="store_true", help="Leave an emulator this run booted running")
    parser.add_argument("--skip-build", action="store_true", help="Use the APK and tkfs already built")
    parser.add_argument("--no-reset", action="store_true", help="Keep the app's data: no pm clear")
    parser.add_argument("--release-pc", action="store_true",
                        help=f"Build and use target/release/tkfs on the PC, as {', '.join(RELEASE_PC_SCENARIOS)} always do")
    args = parser.parse_args(argv)
    if args.scenario != "all" and args.scenario not in SCENARIOS:
        parser.error(f"unknown scenario {args.scenario!r}; choose from {', '.join(SCENARIOS)}, all")
    return args


def main() -> int:
    for stream in (sys.stdout, sys.stderr):
        stream.reconfigure(encoding="utf-8", errors="backslashreplace")
    args = parse_args()
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_dir = ROOT / "test-runs/android-e2e" / stamp
    run_dir.mkdir(parents=True)
    report = Report(run_dir, vars(args))
    names = chosen_scenarios(args.scenario)
    emulator = None
    try:
        if not args.skip_build:
            build(run_dir, names, args.release_pc)
        sdk = find_sdk()
        adb = sdk / "platform-tools" / ("adb.exe" if os.name == "nt" else "adb")
        tool = sdk / "emulator" / ("emulator.exe" if os.name == "nt" else "emulator")
        emulator = Emulator(adb, tool, args.avd, run_dir)
        device = emulator.start()
        report.data["device"] = {"serial": device.serial, "avd": args.avd, "booted_here": emulator.booted_here}
        prepare_device(device, APK)
        for name in names:
            run_scenario(name, device, report, run_dir, reset=not args.no_reset, release_pc=args.release_pc)
    except (HarnessError, RuntimeError, OSError) as error:
        report.fail(str(error))
    except Exception:  # A bug here must still stop the emulator and finish the report.
        report.fail(traceback.format_exc())
    finally:
        if emulator and emulator.booted_here and not args.keep_emulator:
            try:
                for failure in emulator.stop():
                    report.fail(failure)
            except Exception as failure:
                report.fail(f"could not stop the emulator: {failure}")
    return 0 if report.finish() else 1


if __name__ == "__main__":
    raise SystemExit(main())
