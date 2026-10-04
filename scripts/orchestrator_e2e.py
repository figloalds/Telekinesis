"""Isolated LOCAL O1 acceptance. No existing daemon, driver or system PATH changes.

Runs the new binary without WinFsp environment setup, suppresses native loader
dialogs in test children. --keep-artifacts retains fixtures for inspection.
"""
from __future__ import annotations
import argparse
import ctypes
from ctypes import wintypes
from datetime import datetime, timezone
import hashlib
import json
import mmap
import os
from pathlib import Path
import socket
import struct
import subprocess
import time
import uuid
from test_artifacts import add_artifact_options, prepare_artifacts, finish_artifacts

ROOT = Path(__file__).resolve().parents[1]
ENV = os.environ.copy()
ENV.pop("WINFSP_DIR", None)
ENV.pop("TKFS_PEER_KEY", None)
ENV.pop("TKFS_FAULT", None)
ENV["PATH"] = os.pathsep.join(p for p in ENV.get("PATH", "").split(os.pathsep) if "winfsp" not in p.lower())
ctypes.windll.kernel32.SetErrorMode(0x8003)

def wait(check, message, timeout=25):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            result = check()
            if result:
                return result
        except (OSError, AssertionError, subprocess.TimeoutExpired, json.JSONDecodeError) as error:
            last = error
        time.sleep(0.1)
    raise AssertionError((message, str(last)))

def pipe_call(name, request):
    path = rf"\\.\pipe\{name}"
    assert ctypes.windll.kernel32.WaitNamedPipeW(path, 5000), ctypes.get_last_error()
    with open(path, "r+b", buffering=0) as pipe:
        data = json.dumps(request).encode()
        pipe.write(struct.pack("<I", len(data)) + data)
        size = struct.unpack("<I", pipe.read(4))[0]
        data = b""
        while len(data) < size:
            part = pipe.read(size - len(data))
            assert part, "closed pipe"
            data += part
        pipe.write(b"\x01")
        return json.loads(data)

class Fixture:
    def __init__(self, exe, run, name):
        self.exe = exe
        self.run = run / name
        self.run.mkdir()
        self.data = self.run / "data"
        self.pipe = f"tkfs-o1-test-{uuid.uuid4()}"
        self.config = self.run / "defaults.toml"
        self.config.write_text(f"format_version=1\ndata_directory='data'\n[control]\ntransport='named-pipe'\nname='{self.pipe}'\n[network]\nenabled=false\n[workers]\nrestart_policy='on-failure'\nmaximum_running=8\n")
        self.process = None
        self.sequence = 0

    def cli(self, *args, good=True, env=None):
        result = subprocess.run([str(self.exe), *map(str, args)], env=env or ENV, capture_output=True, text=True, timeout=35)
        if good:
            assert result.returncode == 0, (args, result.returncode, result.stderr, result.stdout)
            return json.loads(result.stdout)
        assert result.returncode != 0, (args, result.stdout)
        return result.stderr

    def manage(self, action, *args, generation=None, operation=None, good=True):
        prefix = []
        if generation is not None:
            prefix += ["--generation", str(generation)]
        if operation is not None:
            prefix += ["--request-id", operation]
        return self.cli(*prefix, "manage", "--defaults-file", self.config, action, *args, good=good)

    def start(self, fault=None, env_override=None):
        assert self.process is None
        self.sequence += 1
        env = ENV.copy()
        env.update(env_override or {})
        if fault:
            env["TKFS_FAULT"] = fault
        with (self.run / f"supervisor-{self.sequence}.out.log").open("wb") as out, (self.run / f"supervisor-{self.sequence}.err.log").open("wb") as err:
            self.process = subprocess.Popen([str(self.exe), "orchestrator", "--defaults-file", str(self.config)], env=env, stdout=out, stderr=err, creationflags=subprocess.CREATE_NO_WINDOW)
        def ready():
            assert self.process.poll() is None, (self.run / f"supervisor-{self.sequence}.err.log").read_text()
            return self.manage("list")
        wait(ready, "supervisor readiness")

    def kill_supervisor(self):
        self.process.kill()
        self.process.wait(timeout=10)
        self.process = None

    def root(self, state):
        return self.data / "states" / state["state_id"]

    def record(self, state):
        return json.loads((self.root(state) / "worker.json").read_text())

    def worker(self, state, op="health", **changes):
        record = self.record(state)
        request = {"version": 1, "instance": record["instance"], "token": record["token"], "op": op}
        request.update(changes)
        return pipe_call(record["pipe"], request)

    def shutdown(self):
        generation = self.manage("list")["catalog_generation"]
        self.manage("shutdown", generation=generation, operation=str(uuid.uuid4()))
        self.process.wait(timeout=10)
        assert self.process.returncode == 0
        self.process = None

    def cleanup(self):
        # Only recorded workers in this UUID fixture. Never enumerate/kill all tkfs.
        if self.process:
            try:
                self.shutdown()
            except Exception:
                self.kill_supervisor()
        if self.data.exists():
            for record_path in self.data.glob("states/*/worker.json"):
                record = json.loads(record_path.read_text())
                try:
                    reply = pipe_call(record["pipe"], {"version": 1, "instance": record["instance"], "token": record["token"], "op": "stop"})
                    assert reply["ok"], reply
                except (OSError, AssertionError):
                    pass  # A stopped worker has no pipe; verify that below.
                def stopped():
                    available = ctypes.windll.kernel32.WaitNamedPipeW(rf"\\.\pipe\{record['pipe']}", 1)
                    absent = not available and ctypes.windll.kernel32.GetLastError() == 2
                    return absent and (not record.get("mount") or not Path(record["mount"]).exists())
                wait(stopped, "owned worker exit and mount removal")

def anonymous_rejected(name):
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    advapi = ctypes.WinDLL("advapi32", use_last_error=True)
    kernel.GetCurrentThread.restype = wintypes.HANDLE
    kernel.CreateFileW.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD, ctypes.c_void_p, wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE]
    kernel.CreateFileW.restype = wintypes.HANDLE
    advapi.ImpersonateAnonymousToken.argtypes = [wintypes.HANDLE]
    assert kernel.WaitNamedPipeW(rf"\\.\pipe\{name}", 5000)
    assert advapi.ImpersonateAnonymousToken(kernel.GetCurrentThread())
    try:
        handle = kernel.CreateFileW(rf"\\.\pipe\{name}", 0xC0000000, 0, None, 3, 0, None)
        error = ctypes.get_last_error()
        assert handle == ctypes.c_void_p(-1).value and error == 5, (handle, error)
    finally:
        assert advapi.RevertToSelf()

def hold_directory_without_delete_sharing(path):
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.CreateFileW.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD, ctypes.c_void_p, wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE]
    kernel.CreateFileW.restype = wintypes.HANDLE
    handle = kernel.CreateFileW(str(path), 0x80000000, 3, None, 3, 0x02000000, None)
    assert handle != ctypes.c_void_p(-1).value, ctypes.get_last_error()
    return handle

def close_native_handle(handle):
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    assert kernel.CloseHandle(handle), ctypes.get_last_error()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--exe", type=Path, default=ROOT / "target/o1/debug/tkfs.exe")
    parser.add_argument("--report", type=Path, default=ROOT / "test-evidence" / "ORCHESTRATOR-VALIDATION.json")
    add_artifact_options(parser)
    options = parser.parse_args()
    prepare_artifacts(options)
    run = ROOT / "test-runs" / f"orchestrator-{uuid.uuid4()}"
    run.mkdir()
    fixture = Fixture(options.exe.resolve(), run, "main")
    intent = Fixture(options.exe.resolve(), run, "intent")
    startup = Fixture(options.exe.resolve(), run, "startup")
    limited = Fixture(options.exe.resolve(), run, "limited")
    fenced = Fixture(options.exe.resolve(), run, "shutdown-fence")
    sharing = Fixture(options.exe.resolve(), run, "staging-sharing")
    limited.config.write_text(limited.config.read_text().replace("maximum_running=8", "maximum_running=1"))
    crash_fixtures = []
    checks = []
    def passed(message):
        checks.append(message)
        print("PASS", message, flush=True)
    report = {"scope": "one Windows computer, isolated real WinFsp mounts, no peer network", "run": str(run), "exe": str(options.exe.resolve()), "exe_sha256": hashlib.sha256(options.exe.read_bytes()).hexdigest(), "started_at": datetime.now(timezone.utc).isoformat(), "checks": checks, "manual_winfsp_setup": False}
    try:
        version = subprocess.run([str(fixture.exe), "--version"], env=ENV, capture_output=True, text=True, timeout=10)
        assert version.returncode == 0 and version.stdout.startswith("tkfs "), (version.returncode, version.stderr)
        passed("new executable launches without WINFSP_DIR or WinFsp PATH")
        fixture.start()
        assert "ALREADY_OWNED" in fixture.cli("orchestrator", "--defaults-file", fixture.config, good=False)
        assert "UNSUPPORTED_API_VERSION" in str(pipe_call(fixture.pipe, {"version": 2, "operation_id": None, "expected_generation": None, "action": {"op": "list"}}))
        anonymous_rejected(fixture.pipe)
        passed("duplicate supervisor refused; API version and anonymous caller rejected")

        mount_a, mount_b = fixture.run / "mount-a", fixture.run / "mount-b"
        create_a = str(uuid.uuid4())
        a = fixture.manage("create", "A", "--mount", mount_a, generation=0, operation=create_a)
        replay = fixture.manage("create", "A", "--mount", mount_a, generation=0, operation=create_a)
        assert replay == a
        assert "OPERATION_PAYLOAD_MISMATCH" in fixture.manage("create", "different", generation=0, operation=create_a, good=False)
        assert "STALE_MANAGEMENT_GENERATION" in fixture.manage("create", "stale", generation=0, operation=str(uuid.uuid4()), good=False)
        b = fixture.manage("create", "B", "--mount", mount_b, generation=1, operation=str(uuid.uuid4()))
        assert len(fixture.manage("list")["states"]) == 2 and a["device_id"] != b["device_id"]
        assert "ALREADY_OWNED" in fixture.cli("daemon", "--state", fixture.root(a), good=False)
        passed("two independent real mounts; duplicate store owner refused; exact replay before stale check; no duplicate creation")

        for path, data in [(mount_a / "saved.txt", b"durable A\n"), (mount_b / "saved.txt", b"independent B\n")]:
            with path.open("wb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
        assert (mount_a / "saved.txt").read_bytes() != (mount_b / "saved.txt").read_bytes()
        wait(lambda: fixture.worker(a)["result"]["open_handles"] == 0, "handle cleanup")
        assert not fixture.worker(a, token="wrong")["ok"]
        assert not fixture.worker(a, instance=str(uuid.uuid4()))["ok"]
        # A mounted bearer authorizes existing store operations, never lifecycle.
        runtime = json.loads((fixture.root(a) / "runtime.json").read_text())
        with socket.create_connection(tuple([runtime["address"].split(":")[0], int(runtime["address"].split(":")[1])])) as connection:
            request = json.dumps({"token": runtime["token"], "request": str(uuid.uuid4()), "payload": {"op": "stop"}}).encode()
            connection.sendall(struct.pack(">I", len(request)) + request)
            size = struct.unpack(">I", connection.recv(4))[0]
            response = json.loads(connection.recv(size))
            assert not response["ok"] and "UNKNOWN_OPERATION" in response["error"]
        passed("durable isolated content; worker secret/instance rejected; mounted bearer cannot stop worker")

        before = [fixture.worker(s)["result"]["instance"] for s in (a, b)]
        fixture.kill_supervisor()
        assert mount_a.exists() and mount_b.exists()
        fixture.start()
        after = [fixture.worker(s)["result"]["instance"] for s in (a, b)]
        assert before == after
        assert (mount_a / "saved.txt").read_bytes() == b"durable A\n"
        passed("supervisor crash leaves workers available; authenticated reattachment preserves instances and durable data")

        # A reachable process with a mismatched secret is never stopped/replaced.
        original_record = fixture.record(a)
        record_path = fixture.root(a) / "worker.json"
        fixture.kill_supervisor()
        tampered = dict(original_record, token="wrong")
        record_path.write_text(json.dumps(tampered))
        fixture.start()
        status = fixture.manage("inspect", a["state_id"])
        assert status["observation"]["status"] == "unavailable" and mount_a.exists() and mount_b.exists()
        healthy = pipe_call(original_record["pipe"], {"version": 1, "instance": original_record["instance"], "token": original_record["token"], "op": "health"})
        assert healthy["ok"] and healthy["result"]["instance"] == before[0]
        record_path.write_text(json.dumps(original_record))
        wait(lambda: fixture.manage("inspect", a["state_id"])["observation"]["status"] == "running", "verified reattachment after record restoration")
        passed("unverified live owner is reported unavailable and never killed/replaced; other mounted state stays available")

        stop_a = str(uuid.uuid4())
        with (mount_a / "saved.txt").open("rb") as held:
            held.read(1)
            assert "BUSY_VIEW" in fixture.manage("stop", a["state_id"], generation=0, operation=stop_a, good=False)
            assert mount_a.exists() and mount_b.exists()
        wait(lambda: not mount_a.exists(), "automatic pending stop completion")
        stopped = fixture.manage("stop", a["state_id"], generation=0, operation=stop_a)
        assert stopped["observation"]["status"] == "stopped"
        fixture.manage("start", a["state_id"], generation=1, operation=str(uuid.uuid4()))
        assert (mount_a / "saved.txt").read_bytes() == b"durable A\n"
        passed("busy handles refuse stop; journal resumes stop after cleanup; start strictly reopens preserved data")

        with (mount_a / "saved.txt").open("rb") as mapped_file:
            mapping = mmap.mmap(mapped_file.fileno(), 0, access=mmap.ACCESS_READ)
        mapped_stop = str(uuid.uuid4())
        try:
            assert "BUSY_VIEW" in fixture.manage("stop", a["state_id"], generation=2, operation=mapped_stop, good=False)
            assert mount_a.exists() and mount_b.exists()
        finally:
            mapping.close()
        wait(lambda: not mount_a.exists(), "mapping release stop completion")
        fixture.manage("stop", a["state_id"], generation=2, operation=mapped_stop)
        fixture.manage("start", a["state_id"], generation=3, operation=str(uuid.uuid4()))
        passed("retained Windows mapping refuses cooperative stop until mapping is released")

        # Existing ordinary files are protected even in partially failing starts.
        obstacle = fixture.run / "occupied"
        obstacle.write_text("ordinary user data")
        assert "MOUNT_PATH_OCCUPIED" in fixture.manage("create", "occupied", "--mount", obstacle, generation=2, operation=str(uuid.uuid4()), good=False)
        assert obstacle.read_text() == "ordinary user data"
        assert "MOUNT_DATA_OVERLAP" in fixture.manage("create", "inside", "--mount", fixture.data / "bad-mount", generation=2, operation=str(uuid.uuid4()), good=False)
        passed("occupied user file and mount/data overlap refused without replacement")

        shutdown_id = str(uuid.uuid4())
        with (mount_a / "saved.txt").open("rb") as held:
            held.read(1)
            assert "BUSY_VIEW" in fixture.manage("shutdown", generation=2, operation=shutdown_id, good=False)
            assert mount_a.exists() and mount_b.exists()
        fixture.manage("shutdown", generation=2, operation=shutdown_id)
        fixture.process.wait(timeout=10)
        assert fixture.process.returncode == 0
        fixture.process = None
        wait(lambda: not mount_a.exists() and not mount_b.exists(), "orderly unmount")
        fixture.start()
        assert (mount_a / "saved.txt").read_bytes() == b"durable A\n"
        assert (mount_b / "saved.txt").read_bytes() == b"independent B\n"
        fixture.manage("shutdown", generation=2, operation=shutdown_id)
        assert fixture.process.poll() is None and mount_a.exists() and mount_b.exists()
        passed("busy shutdown preflight leaves both mounts available; exact retry verifies worker exits; restart restores desired mounts")
        passed("replayed completed shutdown returns old receipt without stopping a new supervisor instance")

        fixture.manage("stop", b["state_id"], generation=0, operation=str(uuid.uuid4()))
        db = fixture.root(b) / "metadata.sqlite"
        preserved = fixture.root(b) / "metadata.preserved"
        db.rename(preserved)
        missing_op = str(uuid.uuid4())
        missing = fixture.manage("start", b["state_id"], generation=1, operation=missing_op, good=False)
        assert "STATE_UNAVAILABLE" in missing and not db.exists()
        assert mount_a.exists() and not mount_b.exists()
        preserved.rename(db)
        wait(lambda: mount_b.exists(), "missing store recovery")
        fixture.manage("start", b["state_id"], generation=1, operation=missing_op)
        assert (mount_b / "saved.txt").read_bytes() == b"independent B\n"
        passed("missing registered metadata reports unavailable and is never recreated; restored store recovers without affecting other mount")

        fixture.shutdown()
        passed("final orderly shutdown removes only fixture mounts and workers")

        # Crash just after durable intent: identities and operation survive before
        # any native store exists. Recovery does not allocate a second state.
        intent.start(fault="orchestrator_intent_committed")
        operation = str(uuid.uuid4())
        intent.manage("create", "interrupted", generation=0, operation=operation, good=False)
        intent.process.wait(timeout=10)
        assert intent.process.returncode == 86
        intent.process = None
        intent.start()
        recovered = intent.manage("create", "interrupted", generation=0, operation=operation)
        states = intent.manage("list")["states"]
        assert len(states) == 1 and states[0]["state_id"] == recovered["state_id"]
        assert intent.manage("operation", operation)["status"] == "completed"
        intent.shutdown()
        passed("crash after registry intent resumes same identities, completes journal and exact retry creates no extra state")

        for fault in ["orchestrator_staging_created", "orchestrator_store_initialized", "orchestrator_store_installed", "orchestrator_worker_spawned"]:
            interrupted = Fixture(options.exe.resolve(), run, fault)
            crash_fixtures.append(interrupted)
            interrupted.start(fault=fault)
            operation = str(uuid.uuid4())
            interrupted.manage("create", "interrupted", generation=0, operation=operation, good=False)
            interrupted.process.wait(timeout=10)
            assert interrupted.process.returncode == 86
            interrupted.process = None
            interrupted.start()
            recovered = interrupted.manage("create", "interrupted", generation=0, operation=operation)
            assert len(interrupted.manage("list")["states"]) == 1
            assert interrupted.manage("operation", operation)["status"] == "completed"
            assert interrupted.worker(recovered)["ok"]
            interrupted.shutdown()
            passed(f"creation recovery at {fault} preserves one state and completes same operation")

        # A bad explicit loader override must fail clearly, with no shell search
        # fallback or impact on a healthy headless worker. Retry after restart.
        startup.start(env_override={"WINFSP_DIR": str(startup.run / "not-installed")})
        headless = startup.manage("create", "headless", generation=0, operation=str(uuid.uuid4()))
        bad_mount = startup.run / "mount-retry"
        bad_operation = str(uuid.uuid4())
        assert "WORKER_START" in startup.manage("create", "retry", "--mount", bad_mount, generation=1, operation=bad_operation, good=False)
        assert startup.worker(headless)["ok"] and not bad_mount.exists()
        logs = list((startup.data / "logs").glob("*stderr.log"))
        assert any("WINFSP_RUNTIME_UNAVAILABLE" in log.read_text() and "not-installed" in log.read_text() for log in logs)
        assert startup.manage("operation", bad_operation)["status"] == "pending"
        startup.shutdown()
        startup.start()
        mounted = startup.manage("create", "retry", "--mount", bad_mount, generation=1, operation=bad_operation)
        assert bad_mount.exists() and startup.worker(mounted)["ok"] and startup.worker(headless)["ok"]
        startup.shutdown()
        passed("partial worker mount failure isolated; explicit override fails clearly; pending operation completes after corrected restart")

        limited.start()
        first = limited.manage("create", "first", generation=0, operation=str(uuid.uuid4()))
        second_operation = str(uuid.uuid4())
        assert "WORKER_LIMIT" in limited.manage("create", "second", generation=1, operation=second_operation, good=False)
        assert limited.worker(first)["ok"]
        limited.manage("stop", first["state_id"], generation=0, operation=str(uuid.uuid4()))
        wait(lambda: limited.manage("operation", second_operation)["status"] == "completed", "worker slot release")
        second = limited.manage("create", "second", generation=1, operation=second_operation)
        assert limited.worker(second)["ok"]
        limited.shutdown()
        passed("maximum_running enforced; pending worker starts and journal completes when a slot is released")

        fenced.start()
        base_mount, stopped_mount, pending_mount = [fenced.run / name for name in ("mount-base", "mount-stopped", "mount-pending")]
        base = fenced.manage("create", "base", "--mount", base_mount, generation=0, operation=str(uuid.uuid4()))
        stopped = fenced.manage("create", "stopped", "--mount", stopped_mount, generation=1, operation=str(uuid.uuid4()))
        (base_mount / "held.txt").write_bytes(b"shutdown fence\n")
        (stopped_mount / "kept.txt").write_bytes(b"pending start data\n")
        wait(lambda: fenced.worker(stopped)["result"]["open_handles"] == 0, "fence fixture handle cleanup")
        fenced.manage("stop", stopped["state_id"], generation=0, operation=str(uuid.uuid4()))
        fenced.kill_supervisor()
        fenced.start(env_override={"WINFSP_DIR": str(fenced.run / "not-installed")})
        create_id, start_id, shutdown_id = [str(uuid.uuid4()) for _ in range(3)]
        assert "WORKER_START" in fenced.manage("create", "pending", "--mount", pending_mount, generation=2, operation=create_id, good=False)
        assert "WORKER_START" in fenced.manage("start", stopped["state_id"], generation=1, operation=start_id, good=False)
        allocated = next(s for s in fenced.manage("list")["states"] if s["label"] == "pending")
        with (base_mount / "held.txt").open("rb") as held:
            held.read(1)
            assert "BUSY_VIEW" in fenced.manage("shutdown", generation=3, operation=shutdown_id, good=False)
            fenced.kill_supervisor()
            fenced.start()
            # Startup must preserve the fence before recovering either launch.
            assert not pending_mount.exists() and not stopped_mount.exists()
            assert fenced.manage("operation", create_id)["status"] == "pending"
            assert fenced.manage("operation", start_id)["status"] == "pending"
            assert "SHUTDOWN_PENDING" in fenced.manage("create", "pending", "--mount", pending_mount, generation=2, operation=create_id, good=False)
            assert "SHUTDOWN_PENDING" in fenced.manage("start", stopped["state_id"], generation=1, operation=start_id, good=False)
            assert not pending_mount.exists() and not stopped_mount.exists()
        fenced.manage("shutdown", generation=3, operation=shutdown_id)
        fenced.process.wait(timeout=10)
        assert fenced.process.returncode == 0
        fenced.process = None
        fenced.start()
        resumed = fenced.manage("create", "pending", "--mount", pending_mount, generation=2, operation=create_id)
        fenced.manage("start", stopped["state_id"], generation=1, operation=start_id)
        assert resumed["state_id"] == allocated["state_id"] and len(fenced.manage("list")["states"]) == 3
        assert (stopped_mount / "kept.txt").read_bytes() == b"pending start data\n"
        assert (base_mount / "held.txt").read_bytes() == b"shutdown fence\n"
        fenced.shutdown()
        passed("durable busy-shutdown fence blocks pending create/start across crash, startup and exact retries; normal recovery resumes after shutdown completion")

        sharing.start(fault="orchestrator_store_initialized")
        sharing_id = str(uuid.uuid4())
        sharing_mount = sharing.run / "mount-sharing"
        sharing.manage("create", "sharing", "--mount", sharing_mount, generation=0, operation=sharing_id, good=False)
        sharing.process.wait(timeout=10)
        assert sharing.process.returncode == 86
        sharing.process = None
        staging = next((sharing.data / "staging").iterdir())
        original_marker = json.loads((staging / "state.json").read_text())
        (staging / "preserved.bin").write_bytes(b"native bytes survive blocked install")
        held = hold_directory_without_delete_sharing(staging)
        try:
            sharing.start()
            receipt = sharing.manage("operation", sharing_id)
            assert receipt["status"] == "pending" and receipt["response"]["retryable"]
            assert "os error 32" in sharing.manage("create", "sharing", "--mount", sharing_mount, generation=0, operation=sharing_id, good=False)
            assert "MOUNT_RESERVATION_OVERLAP" in sharing.manage("create", "collision", "--mount", sharing_mount, generation=1, operation=str(uuid.uuid4()), good=False)
            # A second crash/restart must retain the same pending allocation.
            sharing.kill_supervisor()
            sharing.start()
            assert sharing.manage("operation", sharing_id)["status"] == "pending"
        finally:
            close_native_handle(held)
        resumed = sharing.manage("create", "sharing", "--mount", sharing_mount, generation=0, operation=sharing_id)
        assert sharing_mount.exists()
        assert resumed["state_id"] == original_marker["state"] and resumed["device_id"] == original_marker["device"]
        assert len(sharing.manage("list")["states"]) == 1
        assert (sharing.root(resumed) / "preserved.bin").read_bytes() == b"native bytes survive blocked install"
        assert sharing.manage("operation", sharing_id)["status"] == "completed"
        sharing.shutdown()
        passed("staging installation sharing violation retains pending intent/identities/mount reservation across retry/restart; release mounts same state with native data intact")
        report["passed"] = True
    except Exception as error:
        report["passed"] = False
        report["error"] = repr(error)
        raise
    finally:
        for current in (fixture, intent, startup, limited, fenced, sharing, *crash_fixtures):
            try:
                current.cleanup()
            except Exception as cleanup_error:
                report.setdefault("cleanup_errors", []).append(repr(cleanup_error))
        report["completed_at"] = datetime.now(timezone.utc).isoformat()
        finish_artifacts(run, report, options, stopped=not report.get("cleanup_errors"))
        options.report.write_text(json.dumps(report, indent=2) + "\n")
        print("REPORT", options.report, flush=True)

if __name__ == "__main__":
    main()
