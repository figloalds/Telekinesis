"""W6-P0-MOUNT-LIFECYCLE acceptance. Run ONLY inside an isolated Windows VM.

The controller opens no paths on the served volume. All namespace probes and
workloads run in bounded children. A failed scenario stops the matrix; stores,
logs, PID/exit records and the incremental report are retained under --home.
This checks lifecycle correctness; scripts/bench.py remains the performance gate.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
import sqlite3
import subprocess
import sys
import threading
import time

sys.path.insert(0, str(Path(__file__).resolve().parent))
from mount_lifecycle import kill_daemon, run_bounded, stop_daemon

PAYLOAD = b"recoverable lifecycle bytes" * 4096


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def wait_ready(ready, proc, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        require(proc.poll() is None, f"worker exited early: {proc.returncode}")
        if ready.exists():  # ready is on the guest's native filesystem
            return
        time.sleep(.01)
    raise TimeoutError(f"worker {proc.pid} never reached {ready}")


def worker(mode, root, ready, peer=None, binary=None, state=None):
    """Only this disposable child holds namespace handles."""
    if mode == "timestamps":
        timestamp_checks(root, ready, binary, state)
        return
    if mode == "notify-rename":
        # Independent pathname sets avoid application races while exercising
        # the driver's global rename resource against local cache invalidation.
        # This is the workload class behind WinFsp notification deadlock #669.
        counts = [0] * 4
        failures = []
        payload = b"notification rename lifecycle" * 1024
        def exercise(index):
            try:
                first, second = root / f"storm-{index}.a", root / f"storm-{index}.b"
                first.write_bytes(payload)
                if index < 2:
                    while True:
                        first.replace(second)
                        second.replace(first)
                        counts[index] += 2
                else:
                    with first.open("r+b", buffering=0) as file:
                        while True:
                            file.seek(0)
                            file.write(payload)
                            os.fsync(file.fileno())
                            counts[index] += 1
            except BaseException as error:
                failures.append(repr(error))
        for index in range(4):
            threading.Thread(target=exercise, args=(index,), daemon=True).start()
        started = time.monotonic()
        while True:
            require(not failures, f"notification/rename worker failed: {failures}")
            if not ready.exists() and min(counts) >= 24 and time.monotonic() - started >= 8:
                ready.write_text(json.dumps({"counts": counts, "seconds": time.monotonic() - started}))
            time.sleep(.01)
    target = root / "lifecycle.bin"
    if mode == "receipt":
        with target.open("w+b", buffering=0) as writer:
            writer.write(b"old")
            os.fsync(writer.fileno())
            ready.write_text("old-flushed")
            deadline = time.monotonic() + 20
            while not ready.with_suffix(".continue").exists():
                require(time.monotonic() < deadline, "receipt worker continuation timed out")
                time.sleep(.005)
            writer.seek(0)
            writer.write(b"new bytes")
            try:
                os.fsync(writer.fileno())
            except OSError:
                ready.write_text("new-flush-failed")
                time.sleep(600)  # controller bounds shutdown and worker kill
                return
            raise RuntimeError("receipt replay flush fault did not fire")
    target.write_bytes(PAYLOAD)
    if mode == "paired":
        remote = peer / target.name
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            try:
                if remote.read_bytes() == PAYLOAD:
                    break
            except OSError:
                pass
            time.sleep(.01)
        else:
            raise TimeoutError("paired seed never arrived")
        with remote.open("rb", buffering=0) as retained:
            require(retained.read() == PAYLOAD, "paired seed mismatch")
            for index in range(100000):
                target.unlink()
                current = str(index).encode() + PAYLOAD
                target.write_bytes(current)
                while True:
                    try:
                        if remote.read_bytes() == current:
                            break
                    except OSError:
                        pass
                    time.sleep(.01)
                with remote.open("r+b", buffering=0) as writer:
                    writer.write(current)
                    os.fsync(writer.fileno())
                # Signal after a complete reader-first write/fsync iteration;
                # keep delete/recreate running until the controller stops B.
                ready.write_text(str(index))
        return
    with target.open("rb", buffering=0) as reader, target.open("r+b", buffering=0) as writer:
        require(reader.read() == PAYLOAD, "reader-first seed mismatch")
        for index in range(100000):
            writer.seek(0)
            writer.write(PAYLOAD)
            if mode == "fault":
                try:
                    os.fsync(writer.fileno())
                except OSError as error:
                    ready.write_text(str(error))
                    time.sleep(600)  # keep scratch recoverable until shutdown
                    return
                raise RuntimeError("injected native flush unexpectedly succeeded")
            if mode == "flushing":
                os.fsync(writer.fileno())
            ready.write_text(str(index))


def timestamp_checks(root, ready, binary, state):
    import ctypes
    from ctypes import wintypes
    import msvcrt
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.SetFileTime.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.FILETIME),
                                  ctypes.POINTER(wintypes.FILETIME), ctypes.POINTER(wintypes.FILETIME)]
    kernel.SetFileTime.restype = wintypes.BOOL
    ticks = 116_443_872_000_000_000  # 1969-12-31, including before-Unix conversion.
    fixed_ns = (ticks - 116_444_736_000_000_000) * 100
    stamp = wintypes.FILETIME(ticks & 0xffffffff, ticks >> 32)
    payload = b"cached timestamp bytes" * 4096
    observations = []

    def logical_time(path):
        result = run_bounded([str(binary), "--state", str(state), "rpc", "lookup",
                              json.dumps({"path": path.name})], timeout=5, text=True)
        require(result.returncode == 0, result.stderr)
        ticks = json.loads(result.stdout)["entry"]["times"]["modified"]
        return (ticks - 116_444_736_000_000_000) * 100

    def stable(path, expected=None):
        started = time.monotonic()
        logical = logical_time(path)
        observed = before = path.stat().st_mtime_ns
        # Invalidate the kernel's old FileInfo before the 1 s write-back
        # window. Nested paging replies do not refresh that metadata cache.
        # Record the stale observation instead of mistaking it for seal time.
        while before != logical and time.monotonic() - started < .5:
            time.sleep(.005)
            before = path.stat().st_mtime_ns
        require(before == logical and time.monotonic() - started < .5,
                f"cached mtime did not converge before seal: {before} != {logical}")
        coherence_ms = (time.monotonic() - started) * 1000
        require(path.read_bytes() == payload, f"timestamp bytes differ: {path}")
        time.sleep(1.5)  # cross the delayed-seal boundary outside any timed gate
        after = path.stat().st_mtime_ns
        require(before == after, f"seal changed mtime: {before} -> {after}")
        require(logical_time(path) == logical, "seal changed the engine's write time")
        if expected is not None:
            require(after == expected, f"explicit mtime lost: {after} != {expected}")
        observations.append({"file": path.name, "observed_ns": observed,
                             "logical_ns": logical, "coherence_ms": coherence_ms,
                             "before_ns": before, "after_ns": after})

    explicit = root / "explicit-time.bin"
    explicit.write_bytes(payload)
    with explicit.open("rb", buffering=0) as reader:
        require(reader.read() == payload, "timestamp reader-first seed differs")
        with explicit.open("r+b", buffering=0) as writer:
            writer.write(payload)
            require(kernel.SetFileTime(msvcrt.get_osfhandle(writer.fileno()), None, None,
                                       ctypes.byref(stamp)), f"SetFileTime: {ctypes.get_last_error()}")
        stable(explicit, fixed_ns)
    automatic = root / "automatic-time.bin"
    automatic.write_bytes(payload)
    stable(automatic)
    require(automatic.stat().st_mtime_ns > fixed_ns, "automatic write time did not advance")
    temporary = root / "timestamp-temp.bin"
    with temporary.open("wb", buffering=0) as writer:
        writer.write(payload)
        require(kernel.SetFileTime(msvcrt.get_osfhandle(writer.fileno()), None, None,
                                   ctypes.byref(stamp)), f"SetFileTime: {ctypes.get_last_error()}")
    os.replace(temporary, automatic)
    stable(automatic, fixed_ns)
    ready.write_text(json.dumps(observations))


class Acceptance:
    def __init__(self, args):
        self.args = args
        self.exe = args.binary.resolve()
        self.home = args.home.resolve()
        self.home.mkdir(parents=True, exist_ok=False)
        self.report = {"binary": str(self.exe), "sha256": hashlib.sha256(self.exe.read_bytes()).hexdigest(),
                       "platform": sys.getwindowsversion().build, "results": [], "passed": False}
        self.save()

    def save(self):
        report = json.dumps(self.report, indent=2)
        (self.home / "acceptance.json").write_text(report, encoding="utf-8")
        if self.args.report:
            self.args.report.write_text(report, encoding="utf-8")

    def cli(self, state, *command, timeout=20, check=True):
        result = run_bounded([str(self.exe), "--state", str(state), *command], timeout=timeout, text=True)
        if check:
            require(result.returncode == 0, result.stderr)
        return result

    def probe(self, mount):
        result = run_bounded([sys.executable, "-c",
            "import os,sys; sys.exit(int(os.path.exists(sys.argv[1])))", str(mount)], timeout=10, text=True)
        require(result.returncode == 0, f"stale mount remains: {mount}")
        result = run_bounded(["powershell", "-NoProfile", "-Command",
            "$ErrorActionPreference='Stop'; (Get-PSDrive -PSProvider FileSystem).Count; "
            "$p=Join-Path $env:TEMP 'tkfs-native-responsiveness'; "
            "[IO.File]::WriteAllText($p,'ok'); [IO.File]::ReadAllText($p); "
            "Remove-Item -LiteralPath $p"], timeout=10, text=True)
        require(result.returncode == 0, result.stderr)

    def backing_path_refusal(self, alias=False):
        name = "backing-alias-refusal" if alias else "backing-scratch-refusal"
        home = self.home / name
        home.mkdir()
        state = home / "state"
        self.cli(state, "init", "--repo", name)
        scratch = state / "scratch"
        mount = scratch
        if alias:
            mount = home / "scratch-alias"
            quote = lambda path: "'" + str(path).replace("'", "''") + "'"
            result = run_bounded(["powershell", "-NoProfile", "-Command",
                f"New-Item -ItemType Junction -Path {quote(mount)} -Target {quote(scratch)} -ErrorAction Stop | Out-Null"],
                timeout=10, text=True)
            require(result.returncode == 0, result.stderr)
        log_path = home / "daemon.log"
        env = {**os.environ, "TKFS_TEST_MOUNT_FAIL": "after-create"}
        # If preflight regresses, the fault seam still prevents mount-point
        # attachment. Success requires rejection before volume creation.
        with log_path.open("w") as log:
            proc = subprocess.Popen([str(self.exe), "--state", str(state), "daemon",
                "--mount", str(mount), "--listen", "127.0.0.1:0"], stdout=log, stderr=log, env=env)
        record = {"name": name, "pid": proc.pid, "mount": str(mount), "passed": False}
        self.report["results"].append(record)
        self.save()
        try:
            proc.wait(timeout=20)
            require(proc.returncode == 1, "backing overlap must be refused")
            text = log_path.read_text()
            require("MOUNT_OVERLAPS_BACKING_STORAGE" in text, text)
            require("injected mount failure" not in text, "backing check ran after volume creation")
            require(not (state / "control.json").exists(), "overlapping mount became ready")
            self.probe(home / "unused-native-probe")
            record["passed"] = True
        finally:
            record["exit"] = stop_daemon(lambda timeout: None, proc, log_path, mount)
            if record["exit"]["stranded"] or record["exit"]["killed"]:
                record["passed"] = False
            self.save()
        require(record["passed"], f"backing refusal cleanup failed: {record}")

    def scenario(self, kind, mode, forced=False, equal_timestamp=False):
        require(not equal_timestamp or mode == "fault", "timestamp injection requires fault mode")
        tag = mode + ("-equal-timestamp" if equal_timestamp else "")
        name = f"{kind}-{tag}-{'forced' if forced else 'graceful'}"
        home = self.home / name
        home.mkdir()
        mount_arg = self.args.letter if kind == "letter" else str(home / "mount")
        mount = Path(mount_arg + "/")
        state = home / "state"
        self.cli(state, "init", "--repo", name)
        env = os.environ.copy()
        env.pop("TKFS_TEST_MOUNT_SELF_OPEN", None)
        if mode == "self-open":
            env["TKFS_TEST_MOUNT_SELF_OPEN"] = "1"
        if mode.startswith("after-"):
            env["TKFS_TEST_MOUNT_FAIL"] = mode
        else:
            env.pop("TKFS_TEST_MOUNT_FAIL", None)
        conflict = socket.socket()
        listen = "127.0.0.1:0"
        if mode == "startup-error":
            conflict.bind(("127.0.0.1", 0))
            conflict.listen()
            listen = f"127.0.0.1:{conflict.getsockname()[1]}"
        log_path = home / "daemon.log"
        log = log_path.open("w")
        proc = subprocess.Popen([str(self.exe), "--state", str(state), "daemon", "--mount", mount_arg,
                                 "--listen", listen], stdout=log, stderr=log, env=env)
        record = {"name": name, "pid": proc.pid, "mount": mount_arg, "passed": False}
        self.report["results"].append(record)
        self.save()  # evidence exists before any wait on the new daemon
        workload = None
        peer = None
        peer_log = None
        stopped = False
        try:
            if mode.startswith("after-") or mode == "startup-error":
                proc.wait(timeout=20)
                record["exit"] = stop_daemon(lambda timeout: None, proc, log_path, mount_arg)
                stopped = True
                require(proc.returncode != 0, "injected startup failure returned success")
                expected = "MOUNT_FAILED" if mode.startswith("after-") else "SHUTDOWN_INCOMPLETE"
                require(expected in log_path.read_text(), f"missing {expected}")
                if mode.startswith("after-"):
                    require(f"injected mount failure: {mode}" in log_path.read_text(),
                            "mount failed before reaching the injected stage")
            else:
                wait_ready(state / "control.json", proc)
                if mode == "self-open":
                    stats = json.loads(self.cli(state, "rpc", "mount-stats", "{}").stdout)
                    count = next(row["calls"] for row in stats if row.get("operation") == "self_open_refusals")
                    require(count >= 2, "self-open guard was not reached twice")
                    record["self_open_refusals"] = count
                    require(log_path.read_text().count("refused self-open") == 1,
                            "self-open refusal must be logged once")
                if mode == "fault":
                    self.cli(state, "rpc", "inject-fault", json.dumps({"name": "flush-persistent"}))
                if mode == "timestamps":
                    ready = home / "timestamp-observations.json"
                    result = run_bounded([sys.executable, __file__, "--worker", mode,
                        "--root", str(mount), "--ready", str(ready),
                        "--binary", str(self.exe), "--state", str(state)], timeout=60, text=True)
                    (home / "worker.log").write_text(result.stdout + result.stderr)
                    require(result.returncode == 0, result.stderr)
                    record["timestamps"] = json.loads(ready.read_text())
                peer_root = None
                if mode == "paired":
                    peer_state = home / "peer"
                    self.cli(peer_state, "init", "--repo", name)
                    peer_arg = self.args.peer_letter if kind == "letter" else str(home / "peer-mount")
                    peer_root = Path(peer_arg + "/")
                    peer_log = (home / "peer.log").open("w")
                    peer = subprocess.Popen([str(self.exe), "--state", str(peer_state), "daemon",
                        "--mount", peer_arg, "--listen", "127.0.0.1:0"], stdout=peer_log, stderr=peer_log)
                    record["peer_pid"] = peer.pid
                    self.save()
                    wait_ready(peer_state / "control.json", peer)
                    endpoint = json.loads(log_path.read_text().splitlines()[0])["peer"]
                    code = json.loads(self.cli(state, "pairing-code").stdout)["code"]
                    self.cli(peer_state, "pair", endpoint, code)
                if mode in ("cached", "flushing", "fault", "paired", "receipt", "notify-rename"):
                    ready = home / "worker-ready"
                    command = [sys.executable, __file__, "--worker", mode, "--root", str(mount),
                               "--ready", str(ready)]
                    if peer_root:
                        command += ["--peer", str(peer_root)]
                    with (home / "worker.log").open("w") as worker_log:
                        workload = subprocess.Popen(command, stdout=worker_log, stderr=worker_log)
                    record["worker_pid"] = workload.pid
                    self.save()
                    wait_ready(ready, workload)
                    if mode == "receipt":
                        require(ready.read_text() == "old-flushed", "old receipt was not committed")
                        self.cli(state, "rpc", "inject-fault", json.dumps({"name": "flush-persistent"}))
                        ready.with_suffix(".continue").write_text("continue")
                        deadline = time.monotonic() + 20
                        while ready.read_text() != "new-flush-failed":
                            require(workload.poll() is None and time.monotonic() < deadline,
                                    "receipt worker did not preserve the successor bytes")
                            time.sleep(.005)
                    record["worker_progress"] = ready.read_text()
                # In the paired case stop the reader-first receiving peer B.
                target, target_state, target_log, target_mount = (
                    (peer, peer_state, home / "peer.log", peer_arg) if peer else
                    (proc, state, log_path, mount_arg))
                require(target.poll() is None, "daemon exited before the requested stop")
                record["mount_stats"] = json.loads(self.cli(target_state, "rpc", "mount-stats", "{}").stdout)
                if peer:
                    record["source_mount_stats"] = json.loads(self.cli(state, "rpc", "mount-stats", "{}").stdout)
                self.save()
                if forced:
                    record["exit"] = kill_daemon(target, target_log, target_mount)
                else:
                    record["exit"] = stop_daemon(
                        lambda timeout: self.cli(target_state, "stop", timeout=timeout),
                        target, target_log, target_mount)
                if peer:
                    peer = None  # already stopped; never retry after a timeout
                    peer_log.close()
                else:
                    stopped = True
                exit_state = record["exit"]
                require(not exit_state["stranded"], "daemon remained deleting")
                if forced:
                    require(exit_state["killed"], "forced kill was not issued")
                elif mode in ("fault", "receipt"):
                    require(exit_state["returncode"] == 1 and not exit_state["killed"],
                            "failed durability must exit 1 without requiring a kill")
                    require("SHUTDOWN_INCOMPLETE" in log_path.read_text(), "lost durability failure")
                    require(list((state / "scratch").glob("*.scratch")), "recoverable scratch missing")
                else:
                    require(exit_state["graceful"] and exit_state["returncode"] == 0
                            and not exit_state["killed"], "unclean graceful exit")
                self.probe(Path(target_mount + "/"))
                if mode == "paired":
                    record["worker_exit"] = kill_daemon(workload, home / "worker.log", mount_arg)
                    workload = None
                    require(not record["worker_exit"]["stranded"], "paired worker stranded")
                    record["source_exit"] = stop_daemon(
                        lambda timeout: self.cli(state, "stop", timeout=timeout), proc, log_path, mount_arg)
                    stopped = True
                    require(record["source_exit"]["graceful"]
                            and record["source_exit"]["returncode"] == 0
                            and not record["source_exit"]["killed"], "paired source stop failed")
            self.probe(mount)
            if mode in ("fault", "receipt"):
                # Opening the offline store re-seals scratch without a mount.
                # Preserve the original records/bytes before recovery can
                # consume them, especially when the byte comparison fails.
                snapshot = home / "pre-recovery"
                shutil.copytree(state, snapshot)
                record["recovery_snapshot"] = str(snapshot)
                expected_recovery = PAYLOAD
                if mode == "receipt":
                    sidecars = list((state / "scratch").glob("*.json"))
                    require(len(sidecars) == 1, "receipt replay expected one shared scratch")
                    sidecar = sidecars[0]
                    current = json.loads(sidecar.read_text())
                    with sqlite3.connect(state / "metadata.sqlite") as db:
                        row = db.execute("SELECT operation,event FROM seal_receipts WHERE event=?",
                                         (current["basis"]["version"],)).fetchone()
                        require(row is not None, "old content receipt missing")
                        event = json.loads(db.execute("SELECT event FROM events WHERE id=?", (row[1],)).fetchone()[0])
                        parent = event["parents"][0]
                        base = json.loads(db.execute("SELECT event FROM events WHERE id=?", (parent,)).fetchone()[0])
                    require(base["state"]["content"]["total_len"] == 0, "receipt replay base was not empty")
                    replay = json.loads(json.dumps(current))
                    replay["operation"] = row[0]
                    replay["basis"].update(version=parent, anchor=parent, content=base["state"]["content"])
                    replay["hydrated"] = []
                    replay.pop("base_visible_len", None)  # exercise legacy sidecars too
                    record["receipt_replay"] = {"original_record":current, "replayed_record":replay,
                                                "committed_event":row[1], "operation":row[0]}
                    sidecar.write_text(json.dumps(replay))
                    expected_recovery = b"new bytes"
                if equal_timestamp:
                    injections = []
                    for sidecar in (state / "scratch").glob("*.json"):
                        scratch = sidecar.with_suffix(".scratch")
                        marker = json.loads(sidecar.read_text())["modified_ns"]
                        before = scratch.stat()
                        os.utime(scratch, ns=(before.st_atime_ns, marker))
                        require(scratch.stat().st_mtime_ns == marker, "timestamp injection failed")
                        injections.append({"scratch": str(scratch), "bytes": before.st_size,
                                           "before_ns": before.st_mtime_ns, "injected_ns": marker})
                    require(injections, "no scratch to exercise timestamp equality")
                    record["recovery_timestamp_injections"] = injections
                recovered = self.cli(state, "cat", "lifecycle.bin")
                recovered_bytes = recovered.stdout.encode()
                record["recovered_bytes"] = len(recovered_bytes)
                record["recovered_sha256"] = hashlib.sha256(recovered_bytes).hexdigest()
                require(recovered_bytes == expected_recovery, "startup recovery bytes differ")
            record["passed"] = True
        except Exception as error:
            record["error"] = f"{type(error).__name__}: {error}"
            raise
        finally:
            if workload:
                record["worker_exit"] = kill_daemon(workload, home / "worker.log", mount_arg)
            if peer:
                record["peer_cleanup"] = stop_daemon(
                    lambda timeout: self.cli(peer_state, "stop", timeout=timeout),
                    peer, home / "peer.log", peer_arg)
            if not stopped:
                record["cleanup"] = stop_daemon(
                    lambda timeout: self.cli(state, "stop", timeout=timeout), proc, log_path, mount_arg)
            conflict.close()
            log.close()
            if peer_log:
                peer_log.close()
            for key in ("worker_exit", "peer_cleanup", "cleanup"):
                if key not in record:
                    continue
                cleanup = record[key]
                if cleanup.get("stranded") or (key != "worker_exit" and (
                        cleanup.get("killed") or cleanup.get("returncode") != 0)):
                    record["passed"] = False
                    record["error"] = f"unclean {key}: {cleanup}"
            # Fault mode intentionally logs the refused flush. All other I/O
            # errors, especially reader-first ACCESS_DENIED, fail acceptance.
            for path in (home / "daemon.log", home / "peer.log"):
                if not path.exists():
                    continue
                for line in path.read_text(errors="replace").splitlines():
                    if "mount I/O operation" in line and not (
                            mode in ("fault", "receipt") and "operation 7: FAULT_INJECTED: flush" in line):
                        record["passed"] = False
                        record["error"] = f"mount I/O error in {path}: {line}"
            self.save()
        require(record["passed"], record.get("error", name))
        print(json.dumps(record), flush=True)
        return state, mount_arg

    def restart_check(self, state, mount_arg, kind):
        home = state.parent
        log_path = home / "restart.log"
        record = {"name": f"{kind}-save-restart", "passed": False}
        self.report["results"].append(record)
        with log_path.open("w") as log:
            proc = subprocess.Popen([str(self.exe), "--state", str(state), "daemon",
                "--mount", mount_arg, "--listen", "127.0.0.1:0"], stdout=log, stderr=log)
        record["pid"] = proc.pid
        self.save()
        try:
            wait_ready(state / "control.json", proc)
            result = run_bounded([sys.executable, "-c",
                "import hashlib,pathlib,sys; assert hashlib.sha256(pathlib.Path(sys.argv[1]).read_bytes()).hexdigest()==sys.argv[2]",
                str(Path(mount_arg + "/") / "lifecycle.bin"), hashlib.sha256(PAYLOAD).hexdigest()], timeout=20, text=True)
            require(result.returncode == 0, result.stderr)
            record["mount_stats"] = json.loads(self.cli(state, "rpc", "mount-stats", "{}").stdout)
            record["passed"] = True
        finally:
            record["exit"] = stop_daemon(
                lambda timeout: self.cli(state, "stop", timeout=timeout), proc, log_path, mount_arg)
            if not record["exit"]["graceful"] or record["exit"]["returncode"] != 0:
                record["passed"] = False
            self.save()
        require(record["passed"], f"{kind} restart failed")
        self.probe(Path(mount_arg + "/"))

    def smoke(self, kind):
        smoke_home = self.home / f"{kind}-smoke"
        command = [sys.executable, str(Path(__file__).with_name("replacement_smoke.py")),
                   "--binary", str(self.exe), "--home", str(smoke_home), "--forced-kill"]
        if kind == "letter":
            command += ["--mount-a", self.args.letter, "--mount-b", self.args.peer_letter]
        else:
            command += ["--mount-a", str(self.home / "smoke-mount-a"),
                        "--mount-b", str(self.home / "smoke-mount-b")]
        smoke = {"name": f"{kind}-cached-smoke", "passed": False}
        self.report["results"].append(smoke)
        self.save()
        try:
            result = run_bounded(command, timeout=300, text=True)
            (self.home / f"{kind}-smoke.log").write_text(result.stdout + result.stderr, encoding="utf-8")
            require(result.returncode == 0, f"{kind} cached smoke failed")
            smoke["passed"] = True
        except Exception as error:
            smoke["error"] = f"{type(error).__name__}: {error}"
            # A timed-out smoke controller must not leave untracked
            # daemons. Its PID manifests live on native disk. Emergency
            # cleanup verifies the executable path before terminating.
            cleanup = []
            for manifest in smoke_home.glob("*/daemon-process.json"):
                process = json.loads(manifest.read_text())
                executable = str(self.exe).replace("'", "''")
                script = (
                    "$ErrorActionPreference='Stop'; "
                    f"$p=Get-Process -Id {int(process['pid'])} -ErrorAction SilentlyContinue; "
                    "if($p){"
                    f"if($p.Path -ne '{executable}'){{throw 'daemon PID was reused'}}; "
                    "$p.Kill(); if(-not $p.WaitForExit(10000)){throw 'STRANDED daemon'} "
                    "}"
                )
                result = run_bounded(["powershell", "-NoProfile", "-Command",
                    script], timeout=20, text=True)
                cleanup.append({**process, "cleanup_returncode": result.returncode,
                                "cleanup_error": result.stderr})
            smoke["cleanup"] = cleanup
            raise
        finally:
            self.save()

    def run(self):
        self.backing_path_refusal()
        self.backing_path_refusal(alias=True)
        for kind in ("letter", "directory"):
            self.scenario(kind, "self-open")
            self.scenario(kind, "timestamps")
            self.scenario(kind, "notify-rename")
            self.scenario(kind, "notify-rename", forced=True)
            self.smoke(kind)
            # Prove a working mount first; an unavailable driver must never be
            # mistaken for successful partial-attach failure injection.
            self.scenario(kind, "idle")
            self.scenario(kind, "idle", forced=True)
            for mode in ("after-create", "after-mount", "after-dispatcher", "startup-error"):
                self.scenario(kind, mode)
            for mode in ("cached", "flushing", "paired"):
                for forced in (False, True):
                    state, mount_arg = self.scenario(kind, mode, forced)
                    if mode == "flushing" and not forced:
                        self.restart_check(state, mount_arg, kind)
            self.scenario(kind, "fault")
            self.scenario(kind, "fault", equal_timestamp=True)
            self.scenario(kind, "receipt")
        self.report["passed"] = True
        self.save()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=Path("target/release/tkfs.exe"))
    parser.add_argument("--home", type=Path, default=Path("test-runs/mount-acceptance") / str(time.time_ns()))
    parser.add_argument("--isolated-vm", action="store_true", help="confirm execution inside the disposable VM")
    parser.add_argument("--letter", default="X:")
    parser.add_argument("--peer-letter", default="Y:")
    parser.add_argument("--report", type=Path, help="optional second report destination, e.g. Sandbox evidence export")
    parser.add_argument("--worker", choices=("cached", "flushing", "fault", "paired", "timestamps", "receipt", "notify-rename"), help=argparse.SUPPRESS)
    parser.add_argument("--root", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--peer", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--ready", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--state", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.worker:
        worker(args.worker, args.root, args.ready, args.peer, args.binary, args.state)
        return
    if os.name != "nt" or not args.isolated_vm:
        parser.error("requires --isolated-vm inside an isolated Windows guest; never run on the development host")
    Acceptance(args).run()


if __name__ == "__main__":
    main()
