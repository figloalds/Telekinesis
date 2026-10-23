"""Real WinFsp smoke and two-peer cached-update test using disposable stores."""
import json
import hashlib
import argparse
import os
from pathlib import Path
import subprocess
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parent))
from mount_lifecycle import stop_all  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
EXE = ROOT / "target/debug/tkfs.exe"
HOME = ROOT / "test-runs/replacement" / str(time.time_ns())


def cli(state, *args):
    result = subprocess.run([str(EXE), "--state", str(state), *args],
                            capture_output=True, text=True, timeout=60, check=True)
    return json.loads(result.stdout) if result.stdout.strip() else None


def daemon(state, letter, repo):
    state.mkdir(parents=True)
    cli(state, "init", "--repo", repo)
    log = (state / "daemon.log").open("w")
    proc = subprocess.Popen([str(EXE), "--state", str(state), "daemon", "--mount", letter,
                             "--listen", "127.0.0.1:0"], stdout=log, stderr=log,
                            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError((state / "daemon.log").read_text())
        if (state / "control.json").exists():
            return proc, log
        time.sleep(.05)
    raise TimeoutError((state / "daemon.log").read_text())


def wait_for(predicate, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if predicate():
                return
        except OSError:
            pass
        time.sleep(.01)
    raise TimeoutError("condition not reached")


def main():
    global EXE,HOME
    parser=argparse.ArgumentParser()
    parser.add_argument("--binary",type=Path,default=EXE)
    parser.add_argument("--home",type=Path,default=HOME)
    parser.add_argument("--forced-kill",action="store_true",
        help="W6-P0-MOUNT-LIFECYCLE: after the body passes, kill peer A while a cached "
             "write and a retained reader are open; the daemon must exit within 10 s and "
             "the host must stay responsive. Run only in an isolated Windows VM.")
    args=parser.parse_args()
    EXE=args.binary.resolve();HOME=args.home
    HOME.mkdir(parents=True)
    processes = []  # (state, proc, log, mount)
    try:
        a = HOME / "a"
        mount=Path("X:/") if os.name=="nt" else HOME/"mount-a"
        other=Path("Y:/") if os.name=="nt" else HOME/"mount-b"
        mount_a = "X:" if os.name=="nt" else str(mount)
        processes.append((a, *daemon(a, mount_a, "replacement-smoke"), mount_a))
        (mount / "folder").mkdir()
        empty=mount / "folder/empty"
        empty.mkdir()
        assert list(empty.iterdir())==[]
        empty.rmdir()
        p = mount / "folder/file.txt"
        p.write_bytes(b"original")
        assert p.read_bytes() == b"original"
        with p.open("r+b") as f:
            f.seek(3)
            f.write(b"XY")
            f.flush()
            os.fsync(f.fileno())
        assert p.read_bytes() == b"oriXYnal"
        p.rename(mount / "folder/renamed.txt")
        assert (mount / "folder/renamed.txt").read_bytes() == b"oriXYnal"
        cli(a, "flush")
        sealed = bytes(cli(a, "rpc", "cat", json.dumps({"path":"folder/renamed.txt"}))["bytes"])
        assert sealed == b"oriXYnal", f"sealed bytes differ from the kernel cache: {sealed!r}"
        print("single-mount create/read/write/fsync/rename passed", flush=True)
        b = HOME / "b"
        mount_b = "Y:" if os.name=="nt" else str(other)
        processes.append((b, *daemon(b, mount_b, "replacement-smoke"), mount_b))
        peer = json.loads((a / "daemon.log").read_text().splitlines()[0])["peer"]
        code = cli(a, "pairing-code")["code"]
        cli(b, "pair", peer, code)
        remote = other / "folder/renamed.txt"
        wait_for(lambda: remote.read_bytes() == b"oriXYnal")
        automatic = b"a" * 65536 + b"automatic close"
        (mount / "folder/automatic.txt").write_bytes(automatic)
        os.chmod(mount / "folder/automatic.txt", 0o444)
        wait_for(lambda: (other / "folder/automatic.txt").read_bytes() == automatic, timeout=3)
        with (mount / "folder/open-writer.txt").open("wb") as writer:
            writer.write(b"open writer data")
            writer.flush()
            cli(a, "flush")
            sealed = bytes(cli(a, "rpc", "cat", json.dumps({"path":"folder/open-writer.txt"}))["bytes"])
            assert sealed == b"open writer data", sealed
            head = cli(a, "rpc", "lookup", json.dumps({"path":"folder/open-writer.txt"}))
            assert head["entry"]["content"]["digest"] == hashlib.sha256(b"open writer data").hexdigest(), head
        print("automatic close seal and explicit flush of an open writer passed", flush=True)
        # Prime both data and attribute caches before a remote update.
        assert remote.stat().st_size == 8
        assert remote.read_bytes() == b"oriXYnal"
        old_reader=remote.open("rb", buffering=0)
        assert old_reader.read()==b"oriXYnal"
        (mount / "folder/renamed.txt").write_bytes(b"remote replacement")
        cli(a, "flush")
        sealed = bytes(cli(a, "rpc", "cat", json.dumps({"path":"folder/renamed.txt"}))["bytes"])
        assert sealed == b"remote replacement", f"explicit flush missed cached bytes: {sealed!r}"
        head = cli(a, "rpc", "lookup", json.dumps({"path":"folder/renamed.txt"}))
        assert head["entry"]["content"]["digest"] == hashlib.sha256(b"remote replacement").hexdigest(), head
        started = time.monotonic()
        wait_for(lambda: remote.read_bytes() == b"remote replacement")
        elapsed = (time.monotonic() - started) * 1000
        old_reader.seek(0)
        retained=old_reader.read()
        old_reader.close()
        expected=b"remote replacement" if os.name=="nt" else b"oriXYnal"
        assert retained==expected, f"unexpected existing-handle bytes: {retained!r}"
        print(f"paired cached update visible in {elapsed:.1f} ms", flush=True)
        held = remote.open("rb", buffering=0)
        assert held.read() == b"remote replacement"
        source = mount / "folder/renamed.txt"
        source.unlink()
        source.write_bytes(b"recreated remote entity")
        cli(a, "flush")
        wait_for(lambda: remote.read_bytes() == b"recreated remote entity")
        with remote.open("wb") as writer:
            writer.write(b"edited after recreation")
            writer.flush()
            os.fsync(writer.fileno())
        head = cli(b, "rpc", "lookup", json.dumps({"path":"folder/renamed.txt"}))
        assert head["entry"]["content"]["digest"] == hashlib.sha256(b"edited after recreation").hexdigest(), head
        wait_for(lambda: source.read_bytes() == b"edited after recreation")
        held.seek(0)
        expected = b"edited after recreation" if os.name=="nt" else b"remote replacement"
        assert held.read() == expected
        held.close()
        print("cached remote delete/recreate and subsequent local write passed", flush=True)
        print(json.dumps({"home": str(HOME), "visibility_ms": elapsed}), flush=True)
        if args.forced_kill:
            forced_kill_check(processes, mount)
    finally:
        # Bounded graceful stop, bounded kill, stranded daemons fail the run
        # (W6-P0-MOUNT-LIFECYCLE). Never wait forever on a deleting process.
        try:
            stop_all([(lambda s=state: cli(s, "stop"), proc, state / "daemon.log", mnt)
                      for state, proc, log, mnt in processes])
        finally:
            for _, _, log, _ in processes:
                log.close()
        # Paging writes on a reader-first open must not be refused (blocker).
        refused = [line for state, *_ in processes
                   for line in (state / "daemon.log").read_text(errors="replace").splitlines()
                   if "mount I/O operation" in line or "refused self-open" in line]
        if refused:
            raise RuntimeError("mount I/O errors logged: " + "; ".join(refused))


def forced_kill_check(processes, mount):
    """Kill peer A mid-write with a retained reader. With no self-handle the
    process must finish exiting within 10 s, the mount must disappear and a
    fresh PowerShell must still enumerate drives (W6-P0-MOUNT-LIFECYCLE)."""
    state, proc, log, letter = processes[0]
    target = mount / "folder/forced-kill.txt"
    target.write_bytes(b"before kill")
    reader = target.open("rb", buffering=0)
    writer = target.open("r+b", buffering=0)
    writer.write(b"dirty cached bytes " * 4096)  # stays in the Windows cache
    started = time.monotonic()
    proc.kill()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        raise RuntimeError(f"forced kill: daemon pid {proc.pid} still deleting after 10 s")
    exited = time.monotonic() - started
    for handle in (writer, reader):
        try:
            handle.close()
        except OSError:
            pass
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline and mount.exists():
        time.sleep(.1)
    assert not mount.exists(), f"stale usable mount {mount} after forced kill"
    if os.name == "nt":
        shell = subprocess.run(["powershell", "-NoProfile", "-Command", "(Get-PSDrive -PSProvider FileSystem).Count"],
                               capture_output=True, text=True, timeout=10)
        assert shell.returncode == 0, shell.stderr
    processes.pop(0)
    print(json.dumps({"forced_kill_exit_s": round(exited, 3), "mount_gone": True}), flush=True)


if __name__ == "__main__":
    main()
