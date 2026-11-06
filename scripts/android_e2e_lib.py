"""What the end-to-end scenarios in android_e2e.py work with: this PC's processes, what adb
prints, the emulator, the orchestrator on the PC and the report. Every wait here is bounded.
"""
from __future__ import annotations

import base64
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import socket
import subprocess
import tempfile
import time
from typing import Any, Callable, NamedTuple
from urllib.parse import quote
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
PACKAGE = "dev.tkfs"
AUTHORITY = "dev.tkfs.documents"
CONTROL_URI = f"content://{AUTHORITY}"
ROOTS_URI = f"content://{AUTHORITY}/root"
DIRECTORY_MIME = "vnd.android.document/directory"
# The emulator's name for this PC's loopback.
PC_FROM_EMULATOR = "10.0.2.2"
REMOTE_TMP = "/data/local/tmp"
DEFAULT_AVD = "Medium_Phone_API_36.0"
FIRST_EMULATOR_PORT, LAST_EMULATOR_PORT = 5554, 5584

# Every wait, in seconds.
BUILD_BOUND = 1800
BOOT_BOUND = 300
AUTHORIZE_BOUND = 60
EMULATOR_STOP_BOUND = 60
INSTALL_BOUND = 180
ADB_BOUND = 60
# A cold start waits up to 15 s for the orchestrator and 15 s for the worker, per native call.
PROVIDER_BOUND = 120
# How long a request keeps trying while Android cannot bring the provider up, and how often.
PROVIDER_START_BOUND = 60
PROVIDER_RETRY = 1
# sync-now gives up by itself after 175 s.
SYNC_BOUND = 185
PC_START_BOUND = 30
PC_COMMAND_BOUND = 60
PC_STOP_BOUND = 60
FOLDER_READY_BOUND = 60
FROZEN_BOUND = 120
UI_BOUND = 30
KILL_BOUND = 10
POLL = 0.25


class HarnessError(Exception):
    """A step could not be done."""


class CheckFailed(HarnessError):
    """What the app did is not what a check wants."""


class ManageError(HarnessError):
    """The orchestrator refused an action; `str(error)` starts with its code."""


class ContentError(HarnessError):
    """The provider refused a content request."""


class GiveUp(HarnessError):
    """Raised inside a `poll`: waiting longer cannot help."""


@dataclass
class Completed:
    argv: list[str]
    code: int
    stdout: bytes
    stderr: bytes

    @property
    def out(self) -> str:
        return self.stdout.decode("utf-8", "replace").replace("\r\n", "\n").strip()

    @property
    def err(self) -> str:
        return self.stderr.decode("utf-8", "replace").replace("\r\n", "\n").strip()

    def require_success(self) -> Completed:
        if self.code != 0:
            raise HarnessError(f"{describe(self.argv)} exited {self.code}: {self.err or self.out}")
        return self


def describe(argv: list[str], limit: int = 200) -> str:
    text = subprocess.list2cmdline(argv)
    return text if len(text) <= limit else text[:limit] + "..."


def run_bounded(argv, bound: float, *, cwd=None, env=None, log: Path | None = None) -> Completed:
    """Runs `argv` for at most `bound` seconds with no stdin. A timeout is an error.

    Its output goes through files, never pipes: a daemon it leaves behind (adb's server,
    Gradle's) inherits them, and would hold a pipe open long after the command has ended.
    """
    argv = [str(word) for word in argv]
    with tempfile.TemporaryFile() as out, tempfile.TemporaryFile() as err:
        sink = open(log, "ab") if log else None
        try:
            try:
                process = subprocess.Popen(argv, stdin=subprocess.DEVNULL, stdout=sink or out,
                                           stderr=sink or err, cwd=cwd, env=env)
            except OSError as error:
                raise HarnessError(f"could not run {argv[0]}: {error}") from error
            try:
                code = process.wait(bound)
            except subprocess.TimeoutExpired:
                gone = kill_tree(process)
                left = "" if gone else f"; STRANDED_PROCESS: pid {process.pid}"
                raise HarnessError(f"{describe(argv)} did not finish within {bound} s{left}") from None
        finally:
            if sink:
                sink.close()
        out.seek(0)
        err.seek(0)
        return Completed(argv, code, out.read(), err.read())


def kill_tree(process: subprocess.Popen) -> bool:
    """Kills `process` and what it started; whether it went within KILL_BOUND."""
    try:
        if os.name == "nt":
            subprocess.run(["taskkill", "/T", "/F", "/PID", str(process.pid)], stdin=subprocess.DEVNULL,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=KILL_BOUND, check=False)
        else:
            process.kill()
        process.wait(KILL_BOUND)
        return True
    except subprocess.TimeoutExpired:
        return False


def poll(bound: float, what: str, attempt: Callable[[], Any], interval: float = POLL) -> Any:
    """Calls `attempt` until it gives something true and returns that. Past `bound`, an error."""
    end = time.monotonic() + bound
    while True:
        try:
            result = attempt()
            if result:
                return result
            last = repr(result)
        except GiveUp:
            raise
        except HarnessError as error:
            last = str(error)
        if time.monotonic() >= end:
            raise HarnessError(f"{what}: not within {bound} s; last: {last}")
        time.sleep(interval)


# ---- this PC's processes -----------------------------------------------------------------


class Proc(NamedTuple):
    pid: int
    name: str
    command: str


def watched_processes(wmi_class: str = "Win32_Process") -> list[Proc]:
    """Every tkfs, emulator and qemu process on this PC, by name. A query that fails raises,
    because what this answers is whether a process was left behind."""
    if os.name == "nt":
        # With no Stop, a failed Get-CimInstance only prints an error, and then [] with exit 0.
        script = ("$ErrorActionPreference='Stop';"
                  "[Console]::OutputEncoding=[Text.Encoding]::UTF8;"
                  f"$found=@(Get-CimInstance {wmi_class} -Filter \"Name='tkfs.exe' OR Name LIKE 'emulator%'"
                  " OR Name LIKE 'qemu%'\" | Select-Object ProcessId,Name,CommandLine);"
                  "ConvertTo-Json -InputObject $found -Compress")
        argv = ["powershell", "-NoProfile", "-NonInteractive", "-Command", script]
    else:
        argv = ["ps", "-eo", "pid=,comm=,args="]
    listing = run_bounded(argv, ADB_BOUND).require_success().out
    try:
        return _windows_processes(listing) if os.name == "nt" else _ps_processes(listing)
    except (ValueError, KeyError, TypeError) as error:
        raise HarnessError(f"an unreadable process list: {listing[:500]}") from error


def _windows_processes(listing: str) -> list[Proc]:
    return [Proc(row["ProcessId"], row["Name"], row["CommandLine"] or "") for row in json.loads(listing)]


def _ps_processes(listing: str) -> list[Proc]:
    found = []
    for line in listing.splitlines():
        pid, name, command = line.split(None, 2)
        if name.startswith(("tkfs", "emulator", "qemu")):
            found.append(Proc(int(pid), name, command))
    return found


def _normal(path) -> str:
    return str(path).replace("\\", "/").casefold()


def tkfs_processes(home: Path) -> list[Proc]:
    """The tkfs processes of the orchestrator home `home`: its daemon and workers."""
    return [proc for proc in watched_processes()
            if proc.name.startswith("tkfs") and _normal(home) in _normal(proc.command)]


def emulator_processes(avd: str, known=frozenset()) -> list[Proc]:
    """The AVD's emulator and qemu. An exiting emulator also leaves a watchdog for 20 s,
    `emulator -kill QEMU_PID -sleep 20`, which names no AVD: it counts if it names a `known` pid."""

    def ours(proc: Proc) -> bool:
        watchdog = re.search(r"-kill (\d+)", proc.command)
        return proc.name.startswith(("emulator", "qemu")) and (
            f"-avd {avd}" in proc.command or (watchdog is not None and int(watchdog[1]) in known))

    return [proc for proc in watched_processes() if ours(proc)]


def kill_pid(pid: int) -> None:
    if os.name == "nt":
        run_bounded(["taskkill", "/F", "/PID", pid], KILL_BOUND)
    else:
        run_bounded(["kill", "-9", pid], KILL_BOUND)


# ---- what adb prints ----------------------------------------------------------------------


def document_uri(doc: str) -> str:
    """A document's URI. The id is one path segment, so its `/`, `:`, `%` and all are encoded."""
    return f"content://{AUTHORITY}/document/{quote(doc, safe='')}"


def children_uri(doc: str) -> str:
    return document_uri(doc) + "/children"


def shell_line(words) -> str:
    """One command for the device's shell. adb hands its arguments to a remote `sh -c`."""
    return " ".join(shlex.quote(str(word)) for word in words)


def encode_payload(payload) -> str:
    """JSON as base64. `content call` refuses an extra with a colon in its value, so JSON
    cannot go as it is, and this also leaves nothing for a shell or a code page to mangle."""
    return base64.b64encode(json.dumps(payload).encode("utf-8")).decode("ascii")


def parse_call(output: str) -> str:
    """The value of `Result: Bundle[{result=VALUE}]`; `{error=MESSAGE}` raises ManageError."""
    match = re.search(r"Result: Bundle\[\{(result|error)=(.*)\}\]\s*$", output, re.DOTALL)
    if not match:
        raise HarnessError(f"content call printed: {output[:2000]}")
    key, value = match.groups()
    if key == "error":
        raise ManageError(value)
    return value


def parse_rows(output: str) -> list[dict[str, str]]:
    """`content query` prints `Row: N key=value, key=value`. A value may hold a comma, but
    not a comma, a space and a word and an equals sign, which would read as the next column."""
    rows = []
    for line in output.splitlines():
        match = re.match(r"Row: \d+ (.*)", line)
        if match:
            columns = re.split(r", (?=[A-Za-z_][A-Za-z0-9_]*=)", match.group(1))
            rows.append(dict(column.split("=", 1) for column in columns))
    return rows


def is_frozen(dump: str, package: str) -> bool:
    """Whether `dumpsys activity processes` shows `package`'s main process frozen. Its record
    starts `*APP* UID 10217 ProcessRecord{860d2a 5647:dev.tkfs/u0a217}`, and the first
    `isFrozen=` after that is its own."""
    record = rf"\*APP\* UID \d+ ProcessRecord\{{\S+ \d+:{re.escape(package)}/\S*\}}"
    match = re.search(record + r".*?isFrozen=(true|false)", dump, re.DOTALL)
    return bool(match) and match.group(1) == "true"


def split_qr(output: str) -> tuple[dict, list[str]]:
    """`manage reset-pairing-code --qr` prints the reply as JSON and then the QR's lines."""
    end = output.rindex("}") + 1
    return json.loads(output[:end]), [line for line in output[end:].splitlines() if line.strip()]


def bounds_center(bounds: str) -> tuple[int, int]:
    """The center of a uiautomator `bounds="[x1,y1][x2,y2]"`."""
    x1, y1, x2, y2 = map(int, re.findall(r"\d+", bounds))
    return (x1 + x2) // 2, (y1 + y2) // 2


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# ---- the phone ----------------------------------------------------------------------------


class Device:
    """One emulator through adb."""

    def __init__(self, adb: Path, serial: str):
        self.adb_path, self.serial = adb, serial

    def adb(self, *args, bound: float = ADB_BOUND) -> Completed:
        return run_bounded([self.adb_path, "-s", self.serial, *args], bound)

    def shell(self, *words, bound: float = ADB_BOUND) -> Completed:
        """Runs one command on the device, each word exactly as given."""
        return self.adb("shell", shell_line(words), bound=bound)

    def install(self, apk: Path) -> None:
        self.adb("install", "-r", apk, bound=INSTALL_BOUND).require_success()

    def reset_app(self) -> None:
        cleared = self.shell("pm", "clear", PACKAGE).require_success()
        if "Success" not in cleared.out:
            raise HarnessError(f"pm clear printed: {cleared.out} {cleared.err}")

    def force_stop(self) -> None:
        self.shell("am", "force-stop", PACKAGE).require_success()

    def app_pid(self) -> str | None:
        return self.shell("pidof", PACKAGE).out or None

    def shell_manage(self, action: str, payload=None, bound: float = ADB_BOUND):
        """One action on the orchestrator inside the app, like `tkfs manage` on the PC."""
        called = self._provider(shell_line(["content", "call", "--uri", CONTROL_URI, "--method", "tkfs.manage",
                                            "--arg", action, "--extra", "payload:s:" + encode_payload(payload or {})]),
                                bound=bound)
        return json.loads(parse_call(called.out or called.err))

    def call_method(self, method: str, arg: str | None = None) -> str:
        """What `content call` prints for a provider method, with its one argument if any."""
        words = ["content", "call", "--uri", CONTROL_URI, "--method", method]
        called = self._provider(shell_line(words + (["--arg", arg] if arg is not None else [])))
        return called.out or called.err

    def set_last_sync(self, ago_s: float) -> None:
        """Makes every folder's sync clock in the app read `ago_s` seconds ago (a debug build)."""
        wanted = int((time.time() - ago_s) * 1000)
        reply = self.call_method("tkfs.set_last_sync", str(wanted))
        if "result=" not in reply:
            raise HarnessError(f"tkfs.set_last_sync printed: {reply}")

    def _provider(self, line: str, bound: float = ADB_BOUND) -> Completed:
        """Runs a `content` command line, again if Android could not bring the provider up.
        Its low-memory killer can take the cached app between two calls, more so just after
        a boot, and a call that races with that reached nothing, so repeating it is safe. Any
        other failure, such as the app dying during a call, is for the caller to see."""
        end = time.monotonic() + PROVIDER_START_BOUND
        while True:
            done = self.adb("shell", line, bound=bound)
            if "Could not find provider" not in done.err + done.out or time.monotonic() >= end:
                return done
            print(f"  ({self.serial}: Android could not start the provider; trying again)", flush=True)
            time.sleep(PROVIDER_RETRY)

    def _content(self, *words, bound: float = PROVIDER_BOUND) -> Completed:
        """`content` exits 0 after a provider's failure and says so in its output."""
        done = self._provider(shell_line(["content", *words]), bound=bound)
        failed = done.code != 0 or done.err or done.stdout.startswith(b"Error while accessing provider")
        if failed:
            raise ContentError(f"content {words[0]} {words[2]}: {done.err or done.out}")
        return done

    def content_query(self, uri: str) -> list[dict[str, str]]:
        """The rows at `uri`. A provider's failure reads as no rows: `DocumentsProvider` turns
        it into a null cursor, which `content` prints as `No result found.`"""
        return parse_rows(self._content("query", "--uri", uri).out)

    def content_read(self, doc: str, bound: float = PROVIDER_BOUND) -> bytes:
        """The document's bytes, through the provider."""
        return self.content_read_timed(doc, bound)[0]

    def content_read_timed(self, doc: str, bound: float = PROVIDER_BOUND) -> tuple[bytes, float]:
        """The document's bytes, through the provider, and the seconds that read took on the
        device. They go through a file there: on Windows `adb shell` turns LF into CRLF in its
        output, and `adb exec-out`, which does not, mixes in any error."""
        remote = f"{REMOTE_TMP}/tkfs_e2e_read"
        started = time.monotonic()
        done = self._provider(f"content read --uri {shlex.quote(document_uri(doc))} > {remote}", bound=bound)
        took = time.monotonic() - started
        try:
            if done.code != 0 or done.err:
                raise ContentError(f"content read {doc}: {done.err or done.out}")
            with tempfile.TemporaryDirectory(prefix="tkfs-e2e-") as local:
                target = Path(local) / "read"
                self.adb("pull", remote, target, bound=bound).require_success()
                return target.read_bytes(), took
        finally:
            self.shell("rm", "-f", remote)

    def content_write(self, doc: str, data: bytes, bound: float = PROVIDER_BOUND) -> None:
        """Replaces the document's bytes with `data`, through the provider. `content write`
        copies its stdin, which is the device's, so the bytes go there first."""
        remote = f"{REMOTE_TMP}/tkfs_e2e_upload"
        with tempfile.TemporaryDirectory(prefix="tkfs-e2e-") as local:
            source = Path(local) / "upload"
            source.write_bytes(data)
            self.adb("push", source, remote, bound=bound).require_success()
        done = self._provider(f"content write --uri {shlex.quote(document_uri(doc))} < {remote}", bound=bound)
        self.shell("rm", "-f", remote)
        if done.code != 0 or done.err or done.out:
            raise ContentError(f"content write {doc}: {done.err or done.out}")

    def document(self, operation: str, **request) -> dict:
        """A document operation `content` has no way to ask for, done inside the app through
        Android's own API, as another app would (debug builds only). The provider's refusal
        raises ManageError, starting with its code."""
        line = shell_line(["content", "call", "--uri", CONTROL_URI, "--method", "tkfs.document", "--arg", operation,
                           "--extra", "payload:s:" + encode_payload(request)])
        called = self._provider(line, bound=PROVIDER_BOUND)
        return json.loads(parse_call(called.out or called.err))

    def create_document(self, parent: str, name: str, mime: str = "application/octet-stream") -> str:
        return self.document("create", parent=parent, name=name, mime=mime)["id"]

    def rename_document(self, doc: str, name: str) -> str:
        return self.document("rename", id=doc, name=name)["id"]

    def move_document(self, doc: str, source_parent: str, target_parent: str) -> str:
        return self.document("move", id=doc, **{"from": source_parent, "to": target_parent})["id"]

    def delete_document(self, doc: str) -> None:
        self.document("delete", id=doc)

    def write_document(self, doc: str, mode: str, data: bytes, offset: int = 0, fsync: bool = False,
                       close: bool = True) -> int:
        """One write through a descriptor opened in `mode`. With `close` false the descriptor
        stays open, for a kill to find or the document's next write to use. `data` goes on a
        command line, so keep it to a few KiB. How many bytes it wrote."""
        return self.document("write", id=doc, mode=mode, offset=offset, fsync=fsync, close=close,
                             data=base64.b64encode(data).decode("ascii"))["written"]

    def changes_heard(self, directory: str) -> int:
        """How many times Android told an observer of `directory`'s listing that it changed,
        since the first time this asked. An open Files window is such an observer."""
        return self.document("changes", id=directory)["count"]

    def provider_log(self) -> list[str]:
        """What the provider has logged since the last `clear_logcat`, a message per line."""
        logged = self.adb("logcat", "-d", "-v", "raw", "-s", "tkfs:I").require_success().out
        return logged.splitlines()

    def releases(self, doc: str) -> int:
        """How many descriptors of `doc` have been sealed and closed. A descriptor's close
        returns to its owner before its release, which runs later in the provider."""
        return self.provider_log().count(f"released {doc}")

    def wait_release(self, doc: str, before: int, bound: float = PROVIDER_BOUND) -> None:
        poll(bound, f"the release of {doc}", lambda: self.releases(doc) > before)

    def ui_dump(self, bound: float = UI_BOUND) -> ET.Element:
        """The current screen's view tree. uiautomator refuses a screen that is still moving."""
        remote = f"{REMOTE_TMP}/tkfs_e2e_ui.xml"

        def dump():
            dumped = self.shell("uiautomator", "dump", remote, bound=bound)
            if "dumped to" not in dumped.out:
                raise HarnessError(f"uiautomator: {dumped.out} {dumped.err}")
            return self.shell("cat", remote).require_success().stdout

        return ET.fromstring(poll(bound, "a UI dump", dump, interval=1))

    def tap_text(self, text: str, bound: float = UI_BOUND) -> tuple[int, int]:
        """Taps the view showing exactly `text` (or described by it), once one does."""

        def find():
            for node in self.ui_dump(bound).iter("node"):
                if text in (node.get("text"), node.get("content-desc")):
                    return bounds_center(node.get("bounds"))
            raise HarnessError("not on screen")

        x, y = poll(bound, f"a view showing {text!r}", find, interval=1)
        self.shell("input", "tap", x, y).require_success()
        return x, y

    def screen_texts(self) -> list[str]:
        """Every text and description on the screen, in tree order."""
        found = []
        for node in self.ui_dump().iter("node"):
            found += [value for value in (node.get("text"), node.get("content-desc")) if value]
        return found

    def wait_screen(self, what: str, wanted: Callable[[list[str]], bool], bound: float = UI_BOUND) -> list[str]:
        """The screen's texts once `wanted` accepts them. It names what it waited for when it gives up."""

        def look():
            texts = self.screen_texts()
            if not wanted(texts):
                raise HarnessError(f"on screen: {texts}")
            return texts

        return poll(bound, what, look, interval=1)

    def type_into(self, field: str, text: str) -> None:
        """Taps the field described by `field` and types `text` (no spaces: `input text` is plain)."""
        self.tap_text(field)
        self.shell("input", "text", text).require_success()

    def start_app(self) -> None:
        self.shell("am", "start", "-W", "-n", f"{PACKAGE}/.ui.MainActivity").require_success()

    def screenshot(self, path: Path) -> None:
        """The screen as a PNG, by `adb exec-out screencap -p`."""
        shot = self.adb("exec-out", "screencap", "-p").require_success().stdout
        if not shot.startswith(b"\x89PNG"):
            raise HarnessError(f"screencap gave {len(shot)} bytes that are not a PNG")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(shot)

    def wait_frozen(self, package: str, bound: float = FROZEN_BOUND) -> float:
        """Waits until Android has frozen `package`; the seconds that took."""
        started = time.monotonic()

        def frozen():
            return is_frozen(self.shell("dumpsys", "activity", "processes").require_success().out, package)

        poll(bound, f"{package} frozen", frozen, interval=1)
        return time.monotonic() - started

    def push_download(self, name: str, data: bytes | Path, bound: float = 900) -> str:
        """Puts a file in /sdcard/Download, has Android scan it and returns its media URI, which
        another app could share. `data` is bytes, or a file to push as it is."""
        remote = f"/sdcard/Download/{name}"
        with tempfile.TemporaryDirectory(prefix="tkfs-e2e-") as local:
            source = data if isinstance(data, Path) else Path(local) / name
            if not isinstance(data, Path):
                source.write_bytes(data)
            self.adb("push", source, remote, bound=bound).require_success()
        self.shell("am", "broadcast", "-a", "android.intent.action.MEDIA_SCANNER_SCAN_FILE", "-d", f"file://{remote}")

        def scanned() -> str:
            rows = self.shell("content", "query", "--uri", "content://media/external/file", "--projection", "_id",
                              "--where", f"_display_name='{name}'").out
            found = re.search(r"_id=(\d+)", rows)
            if not found:
                raise HarnessError(f"{name} is not scanned yet")
            return f"content://media/external/file/{found.group(1)}"

        return poll(ADB_BOUND, f"{name} scanned", scanned, interval=1)

    def start_share(self, uri: str, mime: str = "application/octet-stream") -> None:
        """Another app sharing one stream: ACTION_SEND to ShareActivity. `am` does not move
        EXTRA_STREAM into ClipData as an app's startActivity does, so the grant rides on the intent's
        data, which it does honor."""
        self.shell("am", "start", "-W", "-a", "android.intent.action.SEND", "-t", mime, "-n",
                   f"{PACKAGE}/.share.ShareActivity", "--eu", "android.intent.extra.STREAM", uri,
                   "-d", uri, "--grant-read-uri-permission").require_success()

    def battery_exempt(self, exempt: bool) -> None:
        """The battery-optimization exemption the owner gives in Settings, set from outside."""
        self.shell("dumpsys", "deviceidle", "whitelist", ("+" if exempt else "-") + PACKAGE).require_success()

    def home(self) -> None:
        self.shell("input", "keyevent", "KEYCODE_HOME").require_success()

    def transfer_log(self) -> list[str]:
        """The transfer job's lines since the last `clear_logcat`."""
        return [line for line in self.provider_log() if line.startswith(("transfer:", "share:"))]

    def pack_bytes(self, state: str) -> int:
        """How many bytes the folder at `state` holds in its packs: file contents on the phone."""
        found = self.shell("run-as", PACKAGE, "find", state, "-type", "f", "-path", "*/packs/*",
                           "-exec", "stat", "-c", "%s", "{}", "+").require_success()
        return sum(int(size) for size in found.out.split())

    def clear_logcat(self) -> None:
        self.adb("logcat", "-c")

    def logged_errors(self, tag: str) -> str:
        """What `tag` has logged at error level since the last `clear_logcat`."""
        return self.adb("logcat", "-d", "-s", f"{tag}:E").require_success().out

    def append_logcat(self, path: Path, title: str) -> None:
        dump = self.adb("logcat", "-d", "-v", "threadtime")
        with open(path, "ab") as log:
            log.write(f"\n===== {title} =====\n".encode())
            log.write(dump.stdout)


class Emulator:
    """The AVD a run drives: one already online, or one this run boots with no window."""

    def __init__(self, adb: Path, tool: Path, avd: str, run_dir: Path):
        self.adb, self.tool, self.avd = adb, tool, avd
        self.log = run_dir / "emulator.log"
        self.device: Device | None = None
        self.booted_here = False

    def _listed(self) -> dict[str, str]:
        """`adb devices`: serial to state. Other programs' ports can show up as offline emulators."""
        listing = run_bounded([self.adb, "devices"], ADB_BOUND).require_success()
        rows = (line.split() for line in listing.out.splitlines()[1:])
        return {row[0]: row[1] for row in rows if len(row) == 2}

    def _running(self) -> str | None:
        """This AVD's emulator, by serial. Never a phone: scenarios clear the app's data."""
        for serial, state in self._listed().items():
            if serial.startswith("emulator-") and state != "offline":
                named = run_bounded([self.adb, "-s", serial, "emu", "avd", "name"], ADB_BOUND)
                if named.code == 0 and named.out.splitlines()[:1] == [self.avd]:
                    return serial
        return None

    def _free_port(self) -> int:
        listed = self._listed()
        for port in range(FIRST_EMULATOR_PORT, LAST_EMULATOR_PORT + 1, 2):
            if f"emulator-{port}" not in listed and _port_free(port) and _port_free(port + 1):
                return port
        raise HarnessError("no free emulator port")

    def start(self) -> Device:
        serial = self._running() or self._boot()
        self.device = Device(self.adb, serial)
        self._wait_authorized(serial)

        def ready():
            booted = self.device.shell("getprop", "sys.boot_completed").out == "1"
            return booted and self.device.shell("pm", "path", "android").out.startswith("package:")

        poll(BOOT_BOUND, f"{serial} booted", ready, interval=2)
        return self.device

    def _boot(self) -> str:
        port = self._free_port()
        # Detached and on a file: it outlives this script with --keep-emulator, and must not
        # hold this script's output pipes. Cold, because loading a saved snapshot brings the
        # AVD's disk back to how it was then, and with it who adb may trust.
        argv = [self.tool, "-avd", self.avd, "-port", port, "-no-window", "-gpu", "swiftshader_indirect",
                "-no-audio", "-no-boot-anim", "-no-snapshot"]
        flags = subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
        with open(self.log, "wb") as sink:
            subprocess.Popen([str(word) for word in argv], stdin=subprocess.DEVNULL, stdout=sink,
                             stderr=subprocess.STDOUT, creationflags=flags, start_new_session=os.name != "nt")
        self.booted_here = True
        return f"emulator-{port}"

    def _wait_authorized(self, serial: str) -> None:
        """Until adb may use the emulator. A prompt with no window will never be answered, and
        this does not answer it: accepting a key on a device is for its owner."""
        first_refused = None

        def online():
            nonlocal first_refused
            if self.booted_here and not emulator_processes(self.avd):
                raise GiveUp(f"the emulator exited; its log ends:\n{self._log_tail()}")
            state = self._listed().get(serial)
            if state == "unauthorized":
                first_refused = first_refused or time.monotonic()
                if time.monotonic() - first_refused > AUTHORIZE_BOUND:
                    raise GiveUp(f"{serial} does not trust this PC's adb key and is asking 'Allow USB debugging?'. "
                                 f"Start {self.avd} once with a window, tick 'Always allow' and accept.")
            return state == "device"

        poll(BOOT_BOUND, f"{serial} online", online, interval=2)

    def _log_tail(self) -> str:
        return self.log.read_text("utf-8", "replace")[-2000:] if self.log.exists() else ""

    def stop(self) -> list[str]:
        """Stops the emulator, leaving nothing behind, whatever goes wrong on the way. What
        went wrong, as failures."""
        failures: list[str] = []
        known: set[int] = set()
        try:
            known = {proc.pid for proc in emulator_processes(self.avd)}
        except HarnessError as error:
            failures.append(f"could not list the emulator's processes: {error}")
        try:
            if self.device:
                run_bounded([self.adb, "-s", self.device.serial, "emu", "kill"], ADB_BOUND)
        except HarnessError as error:
            failures.append(f"emu kill: {error}")
        try:
            poll(EMULATOR_STOP_BOUND, "the emulator gone", lambda: not emulator_processes(self.avd, known), interval=1)
        except HarnessError:
            try:
                for proc in emulator_processes(self.avd, known):
                    kill_pid(proc.pid)
                    failures.append(f"STRANDED_PROCESS: {proc.name} pid {proc.pid}: {proc.command}")
            except HarnessError as error:
                failures.append(f"could not check what the emulator left: {error}")
        return failures


def _port_free(port: int) -> bool:
    with socket.socket() as probe:
        try:
            probe.bind(("127.0.0.1", port))
            return True
        except OSError:
            return False


def free_tcp_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


# ---- the PC -------------------------------------------------------------------------------


class PcHost:
    """A real `tkfs start` in its own home with no mounts, driven through `tkfs manage --json`.

    It listens on a free loopback port, which the emulator reaches at 10.0.2.2. The same port
    is used again by `start` after `stop`, so a test can take the PC away and bring it back.
    """

    def __init__(self, tkfs: Path, home: Path):
        self.tkfs, self.home = tkfs, home
        self.config = home / "orchestrator.toml"
        self.log = home / "daemon.log"
        self.port: int | None = None
        self.process: subprocess.Popen | None = None
        self.sources = 0

    @property
    def endpoint(self) -> str:
        """The address the phone dials."""
        return f"ws://{PC_FROM_EMULATOR}:{self.port}"

    def _env(self) -> dict[str, str]:
        env = dict(os.environ)
        env.pop("TKFS_FAKE_NOW_MS", None)
        env.pop("TKFS_UNSUPERVISED_EXIT_S", None)
        return env

    def start(self) -> None:
        self.home.mkdir(parents=True, exist_ok=True)
        if self.port is None:
            self.port = free_tcp_port()
            self.config.write_text(
                'format_version = 2\ndata_directory = "data"\ndefault_mount_directory = "mounts"\n\n'
                f'[network]\nlisten = "127.0.0.1:{self.port}"\nadvertise = "{self.endpoint}"\n',
                encoding="utf-8")
        with open(self.log, "ab") as sink:
            self.process = subprocess.Popen([str(self.tkfs), "-f", str(self.config), "start"],
                                            stdin=subprocess.DEVNULL, stdout=sink, stderr=subprocess.STDOUT,
                                            env=self._env())

        def started():
            if self.process.poll() is not None:
                raise GiveUp(f"tkfs start exited {self.process.returncode}: {self.log_tail()}")
            return (self.home / "data/orchestrator.json").exists()

        poll(PC_START_BOUND, "orchestrator.json", started, interval=0.1)

    def log_tail(self, size: int = 2000) -> str:
        return self.log.read_text("utf-8", "replace")[-size:]

    def _tkfs(self, *args, bound: float = PC_COMMAND_BOUND) -> Completed:
        return run_bounded([self.tkfs, *args], bound, env=self._env())

    def manage(self, *args, bound: float = PC_COMMAND_BOUND):
        """`tkfs manage --json ARGS`: its reply."""
        done = self._tkfs("-f", self.config, "manage", "--json", *args, bound=bound)
        if done.code != 0:
            raise ManageError(done.err or done.out)
        return json.loads(done.out)

    def folder(self, state: str, *args, bound: float = PC_COMMAND_BOUND) -> bytes:
        """`tkfs --state STATE ARGS`: its raw output."""
        return self._tkfs("--state", state, *args, bound=bound).require_success().stdout

    def pc_has(self, state: str, path: str) -> bool:
        """Whether the file or directory is there on the PC."""
        done = self._tkfs("--state", state, "rpc", "lookup", json.dumps({"path": path}))
        if done.code != 0 and "NOT_FOUND" not in done.err + done.out:
            done.require_success()
        return done.code == 0

    def pc_cat(self, state: str, path: str) -> bytes:
        """The file's bytes on the PC. `cat` takes files up to 3 MiB (CAT_MAX_BYTES)."""
        return self.folder(state, "cat", path)

    def state_dir(self, selector: str) -> str:
        return self.manage("inspect", selector)["record"]["state"]

    def wait_phase(self, selector: str, phase: str, bound: float = FOLDER_READY_BOUND) -> None:
        poll(bound, f"{selector} {phase}",
             lambda: self.manage("inspect", selector)["phase"]["phase"] == phase)

    def create_folder(self, label: str, files: dict[str, bytes]) -> tuple[str, str]:
        """A running headless folder holding `files`, by path with `/`: its repo and state."""
        repo = self.manage("create", label, "--headless")["repo"]
        self.wait_phase(label, "Running")
        state = self.state_dir(label)
        made = set()
        for path, content in files.items():
            parts = path.split("/")[:-1]
            for depth in range(1, len(parts) + 1):
                parent = "/".join(parts[:depth])
                if parent not in made:
                    self.folder(state, "mkdir", parent)
                    made.add(parent)
            self.write_file(state, path, content)
        self.folder(state, "flush")
        return repo, state

    def write_file(self, state: str, path: str, content: bytes) -> None:
        self.sources += 1
        source = self.home / f"source-{self.sources}"
        source.write_bytes(content)
        self.folder(state, "import", path, source)

    def pairing_qr(self) -> tuple[dict, list[str]]:
        """`reset-pairing-code --qr` as a person runs it: the reply and the QR's lines."""
        done = self._tkfs("-f", self.config, "manage", "reset-pairing-code", "--qr").require_success()
        return split_qr(done.out)

    def pairing_code(self) -> str:
        return self.manage("reset-pairing-code")["code"]

    def stop(self) -> tuple[list[str], str]:
        """`manage shutdown` and a bounded wait for its exit, then a kill if it comes to that,
        whatever went wrong before. What went wrong, as failures, and what ran, if it was seen."""
        if self.process is None:
            return [], "not started"
        daemon, self.process = self.process, None
        failures: list[str] = []
        ran = self._what_runs(daemon, failures)
        shut_down = True
        try:
            self.manage("shutdown", bound=PC_STOP_BOUND)
        except Exception as error:
            failures.append(f"manage shutdown: {error}")
            shut_down = False
        try:
            code = daemon.wait(PC_STOP_BOUND if shut_down else 0)
            if code != 0:
                failures.append(f"tkfs start exited {code}: {self.log_tail()}")
        except subprocess.TimeoutExpired:
            gone = kill_tree(daemon)
            failures.append(f"STRANDED_PROCESS: daemon pid {daemon.pid} did not stop (killed: {gone})")
        failures += self._end_what_is_left()
        return failures, f"{ran} process(es) ran, none left" if not failures else ""

    def _what_runs(self, daemon: subprocess.Popen, failures: list[str]) -> int:
        """How many processes run. A query that cannot see the daemon could not see what is
        left either, so that is a failure rather than a clean stop later."""
        try:
            running = tkfs_processes(self.home)
        except Exception as error:
            failures.append(f"could not list the PC's processes: {error}")
            return 0
        if daemon.poll() is None and daemon.pid not in {proc.pid for proc in running}:
            failures.append(f"the process query did not show daemon pid {daemon.pid}, which is running")
        return len(running)

    def _end_what_is_left(self) -> list[str]:
        try:
            try:
                poll(KILL_BOUND, "no tkfs process left", lambda: not tkfs_processes(self.home))
                return []
            except HarnessError:
                left = tkfs_processes(self.home)
                for proc in left:
                    kill_pid(proc.pid)
                return [f"STRANDED_PROCESS: {proc.name} pid {proc.pid}: {proc.command}" for proc in left]
        except Exception as error:
            return [f"could not check what the PC left: {error}"]


# ---- the report ---------------------------------------------------------------------------


class Report:
    """report.json, rewritten after every check, so a run that dies leaves its evidence."""

    def __init__(self, directory: Path, options: dict):
        self.path = directory / "report.json"
        self.data: dict[str, Any] = {"started": _utc(), "finished": None, "status": "running", "options": options,
                                     "device": None, "scenarios": [], "failures": []}
        self.scenario: dict[str, Any] | None = None
        self.save()

    def save(self) -> None:
        draft = self.path.with_suffix(".tmp")
        draft.write_text(json.dumps(self.data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        draft.replace(self.path)

    def begin(self, name: str) -> None:
        self.scenario = {"name": name, "status": "running", "started": _utc(), "checks": [], "error": None}
        self.data["scenarios"].append(self.scenario)
        self.started = time.monotonic()
        print(f"\n== {name}", flush=True)
        self.save()

    def record(self, name: str, passed: bool, detail: Any = "") -> None:
        detail = str(detail)[:2000]
        self.scenario["checks"].append({"name": name, "status": "pass" if passed else "fail", "detail": detail})
        print(f"  [{'pass' if passed else 'FAIL'}] {name}{': ' + detail if detail else ''}", flush=True)
        self.save()

    def check(self, name: str, passed: bool, detail: Any = "") -> None:
        self.record(name, passed, detail)
        if not passed:
            raise CheckFailed(f"{name}: {detail}")

    def end(self, error: str | None = None) -> None:
        checks = self.scenario["checks"]
        passed = error is None and bool(checks) and all(check["status"] == "pass" for check in checks)
        self.scenario.update(status="pass" if passed else "fail", error=error,
                             elapsed_s=round(time.monotonic() - self.started, 1))
        print(f"== {self.scenario['name']}: {self.scenario['status']}"
              f" ({self.scenario['elapsed_s']} s){' ' + error if error else ''}", flush=True)
        self.scenario = None
        self.save()

    def fail(self, message: str) -> None:
        """A failure of the run itself, such as a process left behind."""
        self.data["failures"].append(message)
        print(f"FAILURE: {message}", flush=True)
        self.save()

    def finish(self) -> bool:
        scenarios = self.data["scenarios"]
        passed = not self.data["failures"] and bool(scenarios) and all(s["status"] == "pass" for s in scenarios)
        self.data.update(status="pass" if passed else "fail", finished=_utc())
        self.save()
        print(f"\n{self.data['status'].upper()}: {self.path}", flush=True)
        return passed


def _utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---- what a scenario works with -----------------------------------------------------------


@dataclass
class Folder:
    """A folder on both sides."""
    id: str
    label: str
    repo: str
    state: str
    pc_state: str
    files: dict[str, bytes]

    def doc(self, path: str = "") -> str:
        """A document id, from a path with no leading `/`."""
        return f"{self.id}:/{path}"


class Session:
    def __init__(self, device: Device, pc: PcHost, report: Report):
        self.device, self.pc, self.report = device, pc, report

    def check(self, name: str, passed: bool, detail: Any = "") -> None:
        self.report.check(name, passed, detail)

    def check_pairing_qr(self) -> None:
        reply, lines = self.pc.pairing_qr()
        glyphs = set("".join(lines)) - {" "}
        self.check("the PC draws a pairing QR",
                   len(lines) >= 20 and glyphs <= set("█▀▄") and reply["endpoint"] == self.pc.endpoint
                   and reply["link"].startswith("tkfs://pair?endpoint=ws%3A%2F%2F" + PC_FROM_EMULATOR),
                   f"{len(lines)} lines {max(map(len, lines), default=0)} wide, endpoint {reply['endpoint']}")

    def folder_with(self, files: dict[str, bytes], label: str = "Shared") -> Folder:
        """A folder of `files` on the PC, paired with the phone and received, its listing
        across with `sync-now` and no file downloaded."""
        repo, pc_state = self.pc.create_folder(label, files)
        pair = self.device.shell_manage("pair", {"endpoint": self.pc.endpoint, "code": self.pc.pairing_code()})
        self.check("the phone pairs with the PC", bool(pair.get("installation")), pair)

        def published():
            listed = self.device.shell_manage("remote-list", {"endpoint": self.pc.endpoint})
            return any(entry["repo"] == repo for entry in listed)

        poll(FOLDER_READY_BOUND, "the PC's folder listed", published, interval=1)
        record = self.device.shell_manage("receive", {"endpoint": self.pc.endpoint, "repo": repo, "label": label})
        self.check("the phone receives the folder", record.get("repo") == repo, record.get("id"))

        def running():
            return self.device.shell_manage("inspect", {"state": record["id"]})["phase"]["phase"] == "Running"

        poll(FOLDER_READY_BOUND, "the phone's folder running", running)
        folder = Folder(record["id"], label, repo, record["state"], pc_state, files)
        self.sync_now(folder)
        return folder

    def sync_now(self, folder: Folder, metadata_only: bool = False) -> dict:
        reply = self.device.shell_manage("sync-now", {"state": folder.id, "metadata_only": metadata_only},
                                         bound=SYNC_BOUND)
        result = reply["folders"][0]
        self.check("sync-now finishes with no error", result["error"] is None and result["exchanged"] >= 1, result)
        return result

    def sync_with_the_pc_back(self, folder: Folder) -> dict:
        """A burst, once a PC that was just started or an app that was just killed can take one."""

        def exchanged():
            reply = self.device.shell_manage("sync-now", {"state": folder.id}, bound=SYNC_BOUND)["folders"][0]
            if reply["error"] is not None or reply["exchanged"] < 1:
                raise HarnessError(f"sync-now: {reply}")
            return reply

        return poll(SYNC_BOUND, "a burst that reaches the PC", exchanged, interval=2)

    def tree(self, folder: Folder) -> dict[str, dict[str, str]]:
        """Every document of the folder by path, by listing every directory."""
        found, pending = {}, [folder.doc()]
        while pending:
            for row in self.device.content_query(children_uri(pending.pop())):
                found[row["document_id"].split(":/", 1)[1]] = row
                if row["mime_type"] == DIRECTORY_MIME:
                    pending.append(row["document_id"])
        return found


def with_parents(paths) -> set[str]:
    """`paths` and every directory above them."""
    found = set()
    for path in paths:
        parts = path.split("/")
        found.update("/".join(parts[:depth]) for depth in range(1, len(parts) + 1))
    return found
