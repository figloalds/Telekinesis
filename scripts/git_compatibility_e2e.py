"""Default Git/Win32 acceptance on disposable local WinFsp mounts only.

No remote repositories, ambient Git configuration, hooks, or user state. Only
the daemon children created here are stopped; discovery credentials are removed.
"""
from __future__ import annotations
import argparse
import ctypes as C
from ctypes import wintypes as W
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
K = C.WinDLL("kernel32", use_last_error=True)
N = C.WinDLL("ntdll")
K.GetFileAttributesW.argtypes = [W.LPCWSTR]
K.GetFileAttributesW.restype = W.DWORD
K.SetFileAttributesW.argtypes = [W.LPCWSTR, W.DWORD]
K.SetFileAttributesW.restype = W.BOOL
K.CreateFileW.argtypes = [W.LPCWSTR, W.DWORD, W.DWORD, C.c_void_p, W.DWORD, W.DWORD, W.HANDLE]
K.CreateFileW.restype = W.HANDLE
K.CloseHandle.argtypes = [W.HANDLE]

class BASIC(C.Structure):
    _fields_ = [(name, C.c_int64) for name in ("creation", "access", "write", "change")] + [("attributes", W.DWORD)]

class IOSB(C.Structure):
    _fields_ = [("status", C.c_void_p), ("information", C.c_size_t)]

for name in ("NtSetInformationFile", "NtQueryInformationFile"):
    fn = getattr(N, name)
    fn.argtypes = [W.HANDLE, C.POINTER(IOSB), C.c_void_p, W.ULONG, W.ULONG]
    fn.restype = C.c_long

def basic(path, update=None):
    h = K.CreateFileW(str(path), 0x100 if update else 0x80, 7, None, 3, 0x02000000, None)
    assert h != C.c_void_p(-1).value, (path, C.get_last_error())
    try:
        data = BASIC(*update) if update else BASIC()
        io = IOSB()
        fn = N.NtSetInformationFile if update else N.NtQueryInformationFile
        status = fn(h, C.byref(io), C.byref(data), C.sizeof(data), 4)
        assert status == 0, (path, f"0x{status & 0xffffffff:08x}")
        return {name: getattr(data, name) for name, _ in BASIC._fields_}
    finally:
        K.CloseHandle(h)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--exe", type=Path, required=True)
    ap.add_argument("--report", type=Path, default=ROOT / "GIT-COMPATIBILITY-VALIDATION.json")
    args = ap.parse_args()
    exe = args.exe.resolve()
    run = ROOT / "test-runs" / ("git-" + uuid.uuid4().hex[:8])
    run.mkdir()
    env = os.environ.copy()
    for key in list(env):
        if key.startswith("GIT_"):
            del env[key]
    env.update(GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=str(run / "absent"),
        GIT_TERMINAL_PROMPT="0", GIT_TEMPLATE_DIR=str(run / "template"),
        XDG_CONFIG_HOME=str(run / "xdg"), GIT_AUTHOR_NAME="Local regression",
        GIT_AUTHOR_EMAIL="local@example.invalid", GIT_COMMITTER_NAME="Local regression",
        GIT_COMMITTER_EMAIL="local@example.invalid")
    (run / "template").mkdir()
    (run / "xdg").mkdir()
    checks, commands, cleanup = [], [], []
    report = {"passed": False, "host": os.environ.get("COMPUTERNAME"),
        "exe": str(exe), "exe_sha256": hashlib.sha256(exe.read_bytes()).hexdigest(),
        "run_directory": str(run), "checks": checks, "commands": commands, "cleanup": cleanup}
    def save():
        args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    def command(argv, cwd=run):
        p = subprocess.run(list(map(str, argv)), cwd=cwd, env=env, capture_output=True,
            text=True, timeout=60, creationflags=subprocess.CREATE_NO_WINDOW)
        commands.append({"argv": list(map(str, argv)), "cwd": str(cwd), "exit": p.returncode,
            "stdout": p.stdout, "stderr": p.stderr})
        save()
        assert p.returncode == 0, commands[-1]
        return p
    def passed(text):
        checks.append(text)
        print("PASS " + text, flush=True)
        save()
    def quiet(state):
        deadline = time.monotonic() + 25
        while True:
            status = json.loads(command([exe, "--runtime", state / "runtime.json", "status"]).stdout)
            if status["open_handles"] == 0:
                return
            assert time.monotonic() < deadline, status
            time.sleep(.2)
    def start(state, mount, label):
        out = open(run / (label + ".stdout.log"), "wb")
        err = open(run / (label + ".stderr.log"), "wb")
        p = subprocess.Popen([str(exe), "daemon", "--state", str(state), "--mount", str(mount)],
            cwd=run, env=env, stdout=out, stderr=err, creationflags=subprocess.CREATE_NO_WINDOW)
        try:
            deadline = time.monotonic() + 25
            while not mount.exists() or not (state / "runtime.json").exists():
                assert p.poll() is None, "test daemon exited"
                assert time.monotonic() < deadline, "mount timeout"
                time.sleep(.1)
            return p, out, err
        except BaseException:
            stop((p, out, err), state, mount)
            raise
    def stop(child, state, mount):
        p, out, err = child
        if p.poll() is None:
            p.terminate()
        p.wait(timeout=20)
        out.close()
        err.close()
        (state / "runtime.json").unlink(missing_ok=True)
        cleanup.append({"pid": p.pid, "exited": p.poll() is not None,
            "mount_removed": not mount.exists(), "credentials_removed": not (state / "runtime.json").exists()})
        save()
        assert not mount.exists()
    try:
        command(["git", "--version"])
        fixture = run / "fixture"
        command(["git", "init", "--initial-branch=main", fixture])
        (fixture / "hello.txt").write_bytes(b"local fixture\n")
        (fixture / "nested").mkdir()
        (fixture / "nested" / "second.txt").write_bytes(b"second\n")
        command(["git", "-C", fixture, "add", "."])
        command(["git", "-C", fixture, "commit", "-m", "fixture"])
        for transport in (False, True):
            for root in (True, False):
                label = ("transport" if transport else "local") + ("-root" if root else "-folder")
                state, mount = run / ("s-" + label), run / ("m-" + label)
                command([exe, "init", "--state", state])
                child = start(state, mount, label)
                try:
                    assert os.listdir(mount) == []
                    marker = mount / ".tkfs-runtime.json"
                    assert marker.is_file() and K.GetFileAttributesW(str(marker)) != 0xffffffff
                    # Read without serializing the credential-bearing body.
                    assert json.loads(marker.read_bytes())["format"]
                    command([exe, "-C", mount, "status"])
                    target = mount if root else mount / "repo"
                    command(["git", "clone", *(["--no-local"] if transport else []), fixture,
                        "." if root else "repo"], cwd=mount)
                    assert (target / "hello.txt").read_bytes() == b"local fixture\n"
                    assert (target / "nested" / "second.txt").read_bytes() == b"second\n"
                    assert K.GetFileAttributesW(str(target / ".git")) & 0x12 == 0x12
                    assert command(["git", "status", "--porcelain"], cwd=target).stdout == ""
                    command(["git", "fsck", "--full"], cwd=target)
                    command([exe, "status"], cwd=target / "nested")
                    command([exe, "-C", target / "nested", "status"])
                    command([exe, "--runtime", state / "runtime.json", "status"])
                    assert ".tkfs-runtime.json" not in os.listdir(mount)
                    passed(label + ": default clone, exact bytes, hidden .git, fsck, nested discovery")
                    command(["git", "config", "regression.value", "one"], cwd=target)
                    command(["git", "config", "regression.value", "two"], cwd=target)
                    assert command(["git", "config", "--get", "regression.value"], cwd=target).stdout.strip() == "two"
                    (target / "hello.txt").write_bytes(b"edited\n")
                    command(["git", "add", "hello.txt"], cwd=target)
                    command(["git", "commit", "-m", "mounted edit"], cwd=target)
                    command(["git", "reset", "--hard", "HEAD~1"], cwd=target)
                    assert (target / "hello.txt").read_bytes() == b"local fixture\n"
                    # An existing lock must refuse a competing CREATE_NEW.
                    lock = target / ".git" / "index.lock"
                    with lock.open("xb"):
                        try:
                            lock.open("xb")
                        except FileExistsError:
                            pass
                        else:
                            raise AssertionError("lock collision accepted")
                    lock.unlink()
                    assert not list((target / ".git").glob("*.lock"))
                    passed(label + ": config/index/ref atomic locks, add/commit/reset")
                    test = target / "metadata-probe"
                    test.write_bytes(b"metadata")
                    times = [132000000000000001, 132000000000000002, 132000000000000003, 132000000000000004]
                    basic(test, (*times, 3))
                    got = basic(test)
                    assert [got[k] for k in ("creation", "access", "write", "change")] == times, got
                    assert got["attributes"] == 3, got
                    try:
                        test.write_bytes(b"denied")
                    except PermissionError:
                        pass
                    else:
                        raise AssertionError("readonly write accepted")
                    try:
                        test.unlink()
                    except PermissionError:
                        pass
                    else:
                        raise AssertionError("readonly delete accepted")
                    basic(test, (0, 0, 0, 0, 2))
                    assert basic(test)["write"] == times[2]
                    passed(label + ": four independent times, hidden/readonly enforcement and unchanged times")
                    if not transport and root:
                        retained = basic(test)
                        stop(child, state, mount)
                        child = start(state, mount, label + "-restart")
                        assert basic(test) == retained
                        assert ".tkfs-runtime.json" not in os.listdir(mount)
                        command(["git", "fsck", "--full"], cwd=target)
                        command([exe, "-C", target / "nested", "status"])
                        passed("restart retains metadata, Git objects and direct-only discovery")
                finally:
                    stop(child, state, mount)
        report["passed"] = True
    except BaseException as exc:
        report["error"] = repr(exc)
        raise
    finally:
        save()
        print("Evidence: " + str(args.report), flush=True)

if __name__ == "__main__":
    main()
