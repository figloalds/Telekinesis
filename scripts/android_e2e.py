"""End-to-end checks of the Android app: an emulator against a headless orchestrator on this PC.

    python scripts/android_e2e.py --scenario read
    python scripts/android_e2e.py --scenario all --skip-build --keep-emulator

It drives the app from outside with adb: `content` reaches the provider, and `content call`
with method tkfs.manage reaches the orchestrator inside it (debug builds only). It never
mounts anything. Every wait is bounded, and a process left behind is a failure. Each run writes
test-runs/android-e2e/<UTC>/: report.json, logcat.txt, the build and emulator logs and each
scenario's orchestrator home.

This file holds the command line and the scenarios; android_e2e_lib.py holds what they work
with. To add a scenario, write `def scenario_name(s: Session)` and list it in SCENARIOS. It starts
with the app cleared and a new orchestrator on the PC, and `s.folder_with(files)` gives it a
folder the phone has received. `s.check` records what it finds; a failed check ends the scenario.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import os
from pathlib import Path
import random
import sys
import time
import traceback
from typing import Callable

from android_e2e_lib import (BUILD_BOUND, CONTROL_URI, DEFAULT_AVD, DIRECTORY_MIME, PACKAGE, ROOT, ROOTS_URI,
                             ContentError, Device, Emulator, Folder, HarnessError, ManageError, PcHost, Report,
                             Session, SYNC_BOUND, UI_BOUND, document_uri, poll, run_bounded, sha256, with_parents)
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

    check_writes_refused(s, folder)
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


def check_writes_refused(s: Session, folder: Folder) -> None:
    """Until the provider takes writes."""
    source = s.pc.home / "refused-write"
    source.write_bytes(b"not for the provider")
    try:
        s.device.content_write(folder.doc("notes.txt"), source)
        refused = ""
    except ContentError as error:
        refused = str(error)
    s.check("a write is refused while the provider is read-only", "READ_ONLY" in refused, refused[:300])
    s.check("the refused write changed nothing", s.device.content_read(folder.doc("notes.txt"))
            == folder.files["notes.txt"])


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


SCENARIOS: dict[str, Callable[[Session], None]] = {
    "ui_folders": scenario_ui_folders,
    "read": scenario_read,
    "cold_start": scenario_cold_start,
    "harness": scenario_harness,
}


# ---- running ------------------------------------------------------------------------------


def build(run_dir: Path) -> None:
    """The x86_64 APK for the emulator and the PC's tkfs, each logging to a file."""
    python = sys.executable
    run_bounded([python, ROOT / "scripts/build_android.py", "--abi", "x86_64"], BUILD_BOUND, cwd=ROOT,
                log=run_dir / "build-android.log").require_success()
    run_bounded(["cargo", "build", "--locked", "--bin", "tkfs"], BUILD_BOUND, cwd=ROOT,
                log=run_dir / "build-tkfs.log").require_success()


def tkfs_binary() -> Path:
    return ROOT / "target/debug" / ("tkfs.exe" if os.name == "nt" else "tkfs")


def run_scenario(name: str, device: Device, report: Report, run_dir: Path, reset: bool) -> None:
    report.begin(name)
    pc = PcHost(tkfs_binary(), run_dir / name)
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
    parser.add_argument("--scenario", default="all", help=f"{'|'.join(SCENARIOS)}|all (default: all)")
    parser.add_argument("--avd", default=DEFAULT_AVD)
    parser.add_argument("--keep-emulator", action="store_true", help="Leave an emulator this run booted running")
    parser.add_argument("--skip-build", action="store_true", help="Use the APK and tkfs already built")
    parser.add_argument("--no-reset", action="store_true", help="Keep the app's data: no pm clear")
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
    names = list(SCENARIOS) if args.scenario == "all" else [args.scenario]
    emulator = None
    try:
        if not args.skip_build:
            build(run_dir)
        sdk = find_sdk()
        adb = sdk / "platform-tools" / ("adb.exe" if os.name == "nt" else "adb")
        tool = sdk / "emulator" / ("emulator.exe" if os.name == "nt" else "emulator")
        emulator = Emulator(adb, tool, args.avd, run_dir)
        device = emulator.start()
        report.data["device"] = {"serial": device.serial, "avd": args.avd, "booted_here": emulator.booted_here}
        device.install(APK)
        for name in names:
            run_scenario(name, device, report, run_dir, reset=not args.no_reset)
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
