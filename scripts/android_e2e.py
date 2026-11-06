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
`all` runs but for those in BY_NAME_ONLY. It starts
with the app cleared and a new orchestrator on the PC, and `s.folder_with(files)` gives it a
folder the phone has received. `s.check` records what it finds; a failed check ends the scenario.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import random
import sys
import threading
import time
import traceback
from typing import Callable

from android_e2e_lib import (BUILD_BOUND, CONTROL_URI, DEFAULT_AVD, DIRECTORY_MIME, FOLDER_READY_BOUND, PACKAGE, ROOT, ROOTS_URI,
                             ContentError, Device, Emulator, Folder, HarnessError, ManageError, PcHost, Report,
                             Session, SYNC_BOUND, UI_BOUND, children_uri, document_uri, poll, run_bounded, sha256, with_parents)
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
    folder = s.folder_with(read_files())

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

    for path, content in folder.files.items():
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

    check_cold_start(s, folder, "photo.png")


def check_listing_downloads_nothing(s: Session, folder: Folder, when: str) -> dict[str, dict[str, str]]:
    """Acceptance 15, for one listing of every directory. A failed query reads as no rows, so
    this also needs every name to come back. Thumbnails join this in a later task."""
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


def scenario_speed(s: Session) -> None:
    """A first read of a new 64 MiB random file through the provider, from a release-built PC. It
    crosses the emulator's own network, so it says nothing of Wi-Fi. The second read comes from
    the phone, which shows what the rest of the path allows.

    Not part of `--scenario all`, so that `all` stays a meaningful gate: this network carries
    about 9 MiB/s raw and object bytes cost 3.57 times that as JSON, so this fails its bound of
    10 MiB/s for now. It still runs by name and the bound stands. Task 21, binary object bodies,
    comes after Task 20 and puts it back."""
    content = random.Random(11).randbytes(SPEED_FILE_BYTES)
    folder = s.folder_with({"big.bin": content})
    read, took = s.device.content_read_timed(folder.doc("big.bin"))
    s.check("the file reads the same on the phone", sha256(read) == sha256(content), f"{len(read)} bytes")
    _, again = s.device.content_read_timed(folder.doc("big.bin"))
    rate = len(read) / (1024 * 1024) / took
    cached = len(read) / (1024 * 1024) / again
    s.check(f"a first read goes at least {SPEED_WANTED_MIB_S} MiB/s", rate >= SPEED_WANTED_MIB_S,
            f"{rate:.1f} MiB/s ({took:.2f} s) over the emulator's network, not Wi-Fi; {cached:.1f} MiB/s read again")


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

    # The small file opens, and its write fails. The large one's manifest is not here, so its open does.
    for path, wanted in (("edited-away.txt", "I/O error"), ("edited-away.bin", "OBJECT_MISSING")):
        doc = folder.doc(path)
        started = time.monotonic()
        failed = refusal(lambda: device.write_document(doc, "rw", b"part", offset=3))
        s.check(f"rw writing part of {path}, never downloaded, fails with an I/O error", wanted in failed,
                f"{time.monotonic() - started:.1f} s: {failed[:300]}")
        s.check("and its listed size stays", names_in(s, folder.doc())[path]["_size"] == str(len(files[path])))

    # Past the end of a file whose short last chunk is not here: the write fails, not its seal.
    doc, past = folder.doc("edited-away.txt"), 2 * CHUNK_BYTES
    failed = refusal(lambda: device.write_document(doc, "rw", b"past", offset=past))
    s.check("rw past the end of a short file never downloaded fails with an I/O error", "I/O error" in failed, failed[:300])
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

    # An older PC cannot say what it holds, so the file stays waiting and the app says to update the PC.
    failures, summary = pc.stop()
    s.check("the PC stops cleanly to become an older one", not failures, failures or summary)
    pc.extra_env = {"TKFS_FAULT_POINT": "session.features.none"}
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
    shown = [text for text in device.screen_texts() if "Update the PC" in text]
    s.check("an unsent file shows 'Update the PC' against a PC without content-have", bool(shown), shown)
    s.check("and is named in the table", [file["path"] for file in device.waiting_files(folder.id)] == ["/old-pc.bin"])
    failures, summary = pc.stop()
    s.check("the older PC stops cleanly", not failures, failures or summary)
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


SCENARIOS: dict[str, Callable[[Session], None]] = {
    "ui_folders": scenario_ui_folders,
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

# A debug PC spends its time on its own JSON, so these would measure that.
RELEASE_PC_SCENARIOS = {"speed"}
# Scenarios `all` leaves out. See scenario_speed.
BY_NAME_ONLY = {"speed"}


# ---- running ------------------------------------------------------------------------------


def chosen_scenarios(scenario: str) -> list[str]:
    if scenario == "all":
        return [name for name in SCENARIOS if name not in BY_NAME_ONLY]
    return [scenario]


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


def run_scenario(name: str, device: Device, report: Report, run_dir: Path, reset: bool,
                 release_pc: bool = False) -> None:
    report.begin(name)
    pc = PcHost(tkfs_binary(uses_release_pc(name, release_pc)), run_dir / name)
    error = None
    try:
        device.clear_logcat()
        if reset:
            device.reset_app()
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
                        help=f"{'|'.join(SCENARIOS)}|all (default: all, which leaves out {', '.join(BY_NAME_ONLY)})")
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
        device.install(APK)
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
