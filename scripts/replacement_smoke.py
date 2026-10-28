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
from mount_lifecycle import kill_daemon, run_bounded, stop_all  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
EXE = ROOT / "target/debug/tkfs.exe"
HOME = ROOT / "test-runs/replacement" / str(time.time_ns())


def cli(state, *args, timeout=60):
    result = run_bounded([str(EXE), "--state", str(state), *args],
                         text=True, timeout=timeout)
    result.check_returncode()
    return json.loads(result.stdout) if result.stdout.strip() else None


def daemon(state, letter, repo, processes):
    state.mkdir(parents=True, exist_ok=True)
    cli(state, "init", "--repo", repo)
    log = (state / "daemon.log").open("w")
    proc = subprocess.Popen([str(EXE), "--state", str(state), "daemon", "--mount", letter,
                             "--listen", "127.0.0.1:0"], stdout=log, stderr=log,
                            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    # Register before waiting: failed startup still needs bounded cleanup.
    processes.append((state, proc, log, letter))
    (state / "daemon-process.json").write_text(json.dumps({
        "pid": proc.pid, "mount": letter, "log": str(state / "daemon.log")}), encoding="utf-8")
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
    parser.add_argument("--mount-a", default="X:" if os.name == "nt" else None,
                        help="free drive letter or nonexistent directory (VM only on Windows)")
    parser.add_argument("--mount-b", default="Y:" if os.name == "nt" else None)
    parser.add_argument("--hold-cached", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--ready-file", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--forced-kill",action="store_true",
        help="W6-P0-MOUNT-LIFECYCLE: after the body passes, kill peer A while a cached "
             "write and a retained reader are open; the daemon must exit within 10 s and "
             "the host must stay responsive. Run only in an isolated Windows VM.")
    args=parser.parse_args()
    if args.hold_cached:
        hold_cached(args.hold_cached, args.ready_file)
        return
    EXE=args.binary.resolve();HOME=args.home
    HOME.mkdir(parents=True)
    processes = []  # (state, proc, log, mount)
    try:
        a = HOME / "a"
        mount_a = args.mount_a or str(HOME / "mount-a")
        mount_b = args.mount_b or str(HOME / "mount-b")
        mount = Path(mount_a + "/")
        other = Path(mount_b + "/")
        daemon(a, mount_a, "replacement-smoke", processes)
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
        daemon(b, mount_b, "replacement-smoke", processes)
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
            # RPC flush cannot drain another process's Windows cache. File
            # fsync is the durability point (plan section 3a).
            os.fsync(writer.fileno())
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
        conflict_resolution_check(a, b, mount, other, mount_a, mount_b, processes)
        print(json.dumps({"home": str(HOME), "visibility_ms": elapsed}), flush=True)
        if args.forced_kill:
            forced_kill_check(processes, mount)
    finally:
        # Bounded graceful stop, bounded kill, stranded daemons fail the run
        # (W6-P0-MOUNT-LIFECYCLE). Never wait forever on a deleting process.
        try:
            stop_all([(lambda timeout, s=state: cli(s, "stop", timeout=timeout), proc, state / "daemon.log", mnt)
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


def conflict_resolution_check(a, b, mount, other, mount_a, mount_b, processes):
    """Make actual offline edits, then resolve through cached mounted names."""
    import shutil
    relative = "folder/conflict.txt"
    renamed = "folder/conflict-renamed.txt"
    (mount / relative).write_bytes(b"shared conflict base")
    cli(a, "flush")
    wait_for(lambda: (other / relative).read_bytes() == b"shared conflict base")
    entity = cli(a, "rpc", "lookup", json.dumps({"path":relative}))["entry"]["id"]
    previous = processes[:]
    processes.clear()  # no retry or new mounts after a failed bounded stop
    try:
        stop_all([(lambda timeout, s=state: cli(s, "stop", timeout=timeout),
                   proc, state / "daemon.log", letter)
                  for state, proc, _, letter in previous])
    finally:
        for state, _, log, _ in previous:
            log.close()
            shutil.copy2(state / "daemon.log", state / "before-conflict-daemon.log")
            shutil.copy2(state / "daemon-exit-state.json", state / "before-conflict-exit-state.json")
    for state, *_ in previous:
        errors = [line for line in (state / "before-conflict-daemon.log").read_text(errors="replace").splitlines()
                  if "mount I/O operation" in line or "refused self-open" in line]
        if errors:
            raise RuntimeError("pre-conflict mount I/O errors: " + "; ".join(errors))
    for state in (a, b):
        for peer in cli(state, "peers"):
            cli(state, "unpair", peer["installation"])
    source = HOME / "offline-import.bin"  # native backing disk, outside both mounts
    source.write_bytes(b"local conflict edit")
    cli(a, "import", relative, str(source))
    cli(b, "rename", relative, renamed)
    source.write_bytes(b"remote renamed edit")
    cli(b, "import", renamed, str(source))
    selected = [event for _, event in cli(b, "events") if event["entity"] == entity][-1]["id"]
    daemon(a, mount_a, "replacement-smoke", processes)
    daemon(b, mount_b, "replacement-smoke", processes)
    assert (mount / relative).read_bytes() == b"local conflict edit"
    endpoint = json.loads((a / "daemon.log").read_text().splitlines()[0])["peer"]
    cli(b, "pair", endpoint, cli(a, "pairing-code")["code"])
    wait_for(lambda: any(c["entity"] == entity for c in cli(a, "conflicts")))
    assert (mount / relative).read_bytes() == b"local conflict edit"
    # Cleanup delivers these bytes; resolution must seal them before choosing
    # the remote head, so its later delayed seal cannot reopen the conflict.
    pending = b"pending local alternative"
    (mount / relative).write_bytes(pending)
    cli(a, "resolve", entity, selected)
    wait_for(lambda: not (mount / relative).exists()
             and (mount / renamed).read_bytes() == b"remote renamed edit")
    wait_for(lambda: not cli(b, "conflicts"))
    assert not cli(a, "conflicts"), "resolution reopened a local conflict"
    history = cli(a, "conflicts", "--history")
    digest = hashlib.sha256(pending).hexdigest()
    assert any((event["state"].get("content") or {}).get("digest") == digest
               for conflict in history for event in conflict["heads"]), "pending alternative was lost"
    print("offline conflict, pending edit retention and cached rename resolution passed", flush=True)


def hold_cached(target, ready):
    """All risky namespace handles stay in a disposable, bounded child."""
    target.write_bytes(b"before kill")
    with target.open("rb", buffering=0) as reader, target.open("r+b", buffering=0) as writer:
        assert reader.read() == b"before kill"
        writer.write(b"dirty cached bytes " * 4096)
        ready.write_text("ready")
        time.sleep(600)


def forced_kill_check(processes, mount):
    state, proc, log, letter = processes[0]
    ready = state / "cached-worker-ready"
    worker_log = (state / "cached-worker.log").open("w")
    worker = subprocess.Popen([sys.executable, __file__, "--hold-cached",
                               str(mount / "folder/forced-kill.txt"), "--ready-file", str(ready)],
                              stdout=worker_log, stderr=worker_log)
    try:
        wait_for(ready.exists, timeout=20)  # native disk only
        processes.pop(0)  # do not retry a stranded daemon in final cleanup
        record = kill_daemon(proc, state / "daemon.log", letter)
        if record["stranded"] or not record["killed"]:
            raise RuntimeError("forced kill failed: " + json.dumps(record))
        errors = [line for line in (state / "daemon.log").read_text(errors="replace").splitlines()
                  if "mount I/O operation" in line or "refused self-open" in line]
        if errors:
            raise RuntimeError("forced peer logged I/O errors: " + "; ".join(errors))
        # Even stat/exists can block on a hung mount; probe in a bounded child.
        probe = run_bounded([sys.executable, "-c",
            "import os,sys; sys.exit(int(os.path.exists(sys.argv[1])))", str(mount)],
            timeout=10, text=True)
        assert probe.returncode == 0, f"stale usable mount {mount}"
        if os.name == "nt":
            shell = run_bounded(["powershell", "-NoProfile", "-Command",
                "(Get-PSDrive -PSProvider FileSystem).Count; "
                "$p=Join-Path $env:TEMP 'tkfs-native-probe'; "
                "[IO.File]::WriteAllText($p,'ok'); [IO.File]::ReadAllText($p); "
                "Remove-Item -LiteralPath $p"], timeout=10, text=True)
            shell.check_returncode()
        print(json.dumps({"forced_kill_exit_s": record["kill_seconds"], "mount_gone": True}), flush=True)
    finally:
        worker_record = kill_daemon(worker, state / "cached-worker.log", letter)
        worker_log.close()
        # Peer A is no longer in the normal teardown list.
        if not any(p is proc for _, p, _, _ in processes):
            log.close()
        if worker_record["stranded"]:
            raise RuntimeError("cached worker stranded: " + json.dumps(worker_record))


if __name__ == "__main__":
    main()
