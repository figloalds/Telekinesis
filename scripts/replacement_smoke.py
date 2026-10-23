"""Real WinFsp smoke and two-peer cached-update test using disposable stores."""
import json
import hashlib
import argparse
import os
from pathlib import Path
import subprocess
import time

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
    args=parser.parse_args()
    EXE=args.binary.resolve();HOME=args.home
    HOME.mkdir(parents=True)
    processes = []
    try:
        a = HOME / "a"
        mount=Path("X:/") if os.name=="nt" else HOME/"mount-a"
        other=Path("Y:/") if os.name=="nt" else HOME/"mount-b"
        processes.append((a, *daemon(a, "X:" if os.name=="nt" else str(mount), "replacement-smoke")))
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
        processes.append((b, *daemon(b, "Y:" if os.name=="nt" else str(other), "replacement-smoke")))
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
        print(json.dumps({"home": str(HOME), "visibility_ms": elapsed}), flush=True)
    finally:
        for state, proc, log in reversed(processes):
            try:
                cli(state, "stop")
                proc.wait(timeout=20)
            except Exception:
                proc.kill()
                proc.wait()
            log.close()


if __name__ == "__main__":
    main()
