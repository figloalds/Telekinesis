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
                             Session, document_uri, run_bounded, sha256, with_parents)
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

    device.shell("am", "start", "-W", "-n", f"{PACKAGE}/.MainActivity").require_success()
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


SCENARIOS: dict[str, Callable[[Session], None]] = {
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
