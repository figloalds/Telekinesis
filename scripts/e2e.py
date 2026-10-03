"""Real WinFsp/OS I/O, two LOCAL processes; no two-computer claim.

Run after cargo build. All fixtures live in test-runs/<unique UUID>, never on
another project. No driver install, firewall change or credentials are persisted.
"""
from __future__ import annotations
import argparse
import json
import mmap
import os
from pathlib import Path
import secrets
import socket
import subprocess
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
EXE = ROOT / "target" / "debug" / "tkfs.exe"
ENV = os.environ.copy()
ENV["PATH"] = str(Path(ENV.get("WINFSP_DIR", r"C:\Program Files (x86)\WinFsp")) / "bin") + os.pathsep + ENV["PATH"]
ENV["TKFS_PEER_KEY"] = secrets.token_hex(32)


def cli(*args, expected_ok=True):
    completed = subprocess.run([str(EXE), *map(str, args)], env=ENV, capture_output=True, text=True, timeout=25)
    if expected_ok:
        assert completed.returncode == 0, (args, completed.stdout, completed.stderr)
        return json.loads(completed.stdout)
    assert completed.returncode != 0, (args, completed.stdout)
    return completed.stderr


def wait_for(check, message, timeout=20):
    end = time.monotonic() + timeout
    last = None
    while time.monotonic() < end:
        try:
            value = check()
            if value:
                return value
        except (OSError, AssertionError, json.JSONDecodeError, subprocess.TimeoutExpired) as exc:
            last = exc
        time.sleep(0.1)
    raise AssertionError(f"{message}: {last}")


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def save(path: Path, data: bytes):
    with path.open("wb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())  # Acknowledged local durable boundary.


class Device:
    def __init__(self, run: Path, name: str, repo: str):
        self.name = name
        self.state = run / name
        self.mount = run / f"mount-{name}"
        info = cli("init", "--state", self.state, "--repo", repo)
        self.identity = info["device"]
        self.port = free_port()
        self.process = None
        self.logs = []

    def start(self, peer=None, fault=None):
        assert self.process is None
        args = [str(EXE), "daemon", "--state", str(self.state), "--mount", str(self.mount)]
        if peer:
            args += ["--listen", f"127.0.0.1:{self.port}", "--peer", f"127.0.0.1:{peer.port}", "--peer-device", peer.identity]
        env = ENV.copy()
        if fault:
            env["TKFS_FAULT"] = fault
        index = len(self.logs)
        out = self.state.parent / f"{self.name}-{index}-stdout.log"
        err = self.state.parent / f"{self.name}-{index}-stderr.log"
        self.logs.append((out, err))
        # Remove only this test's stale discovery file; not metadata or objects.
        (self.state / "runtime.json").unlink(missing_ok=True)
        with out.open("wb") as stdout, err.open("wb") as stderr:
            self.process = subprocess.Popen(args, env=env, stdout=stdout, stderr=stderr, creationflags=subprocess.CREATE_NO_WINDOW)
        if fault:
            # This process may hit its deliberate crash before readiness can be
            # observed. Its durable state was prepared by the previous runtime.
            return

        def ready():
            assert self.process.poll() is None, (out.read_text(), err.read_text())
            if not (self.state / "runtime.json").exists():
                return False
            return self.mount.exists() and self.control("status")
        wait_for(ready, f"{self.name} mount readiness")

    def stop(self):
        if self.process is not None:
            if self.process.poll() is None:
                self.process.kill()
            self.process.wait(timeout=10)
            self.process = None
            wait_for(lambda: not self.mount.exists(), "mount removal after process kill")

    def control(self, *args, **kwargs):
        return cli("--runtime", self.state / "runtime.json", *args, **kwargs)

    def quiet(self):
        return wait_for(lambda: self.control("status")["open_handles"] == 0, "handle cleanup")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--report", type=Path, default=ROOT / "VALIDATION.json")
    args = parser.parse_args()
    run = ROOT / "test-runs" / str(uuid.uuid4())
    run.mkdir(parents=True)
    repo = str(uuid.uuid4())
    a, b = Device(run, "a", repo), Device(run, "b", repo)
    checks = []

    def passed(message):
        checks.append(message)
        print("PASS", message, flush=True)

    try:
        a.start(b)
        b.start(a)
        save(a.mount / "note.txt", b"base\n")
        wait_for(lambda: (b.mount / "note.txt").read_bytes() == b"base\n", "live shared save")
        passed("real WinFsp mounted save is visible on the second LOCAL process without checkpoint")

        # Real .NET directory watcher on the peer mount; no simulated callbacks.
        ready, events, stop = run / "watch.ready", run / "watch.events", run / "watch.stop"
        # Restricted hosts permit ordinary inline commands; do not change their
        # script execution policy to run this filesystem API check.
        literal = lambda p: "'" + str(p).replace("'", "''") + "'"
        watcher_command = "& {\n" + (ROOT / "scripts" / "watch.ps1").read_text() + "\n} -Mount " + literal(b.mount) + " -Ready " + literal(ready) + " -Events " + literal(events) + " -StopPath " + literal(stop)
        with (run / "watch.stdout.log").open("wb") as stdout, (run / "watch.stderr.log").open("wb") as stderr:
            watcher = subprocess.Popen(["powershell", "-NoProfile", "-Command", watcher_command], env=ENV, stdout=stdout, stderr=stderr, creationflags=subprocess.CREATE_NO_WINDOW)
        try:
            wait_for(ready.exists, "watcher readiness")
            def notified(action, name):
                return any(line.lower().startswith(action.lower() + ":") and line.lower().endswith("\\" + name.lower()) for line in events.read_text().splitlines())
            save(a.mount / "watch.txt", b"before\n")
            wait_for(lambda: (b.mount / "watch.txt").read_bytes() == b"before\n", "watch initial bytes")
            wait_for(lambda: notified("Created", "watch.txt"), "peer create notification")
            with (b.mount / "watch.txt").open("rb", buffering=0) as reader:
                assert reader.read() == b"before\n"
                changed_before = events.read_text().lower().count("changed:")
                save(a.mount / "watch.txt", b"after!\n")
                wait_for(lambda: (reader.seek(0), reader.read())[1] == b"after!\n", "existing read-only handle refresh")
                wait_for(lambda: events.read_text().lower().count("changed:") > changed_before, "peer content change notification")
            os.rename(a.mount / "watch.txt", a.mount / "watched-renamed.txt")
            # Namespace projection diffs report a rename as old-path deletion
            # and new-path creation, rather than FileSystemWatcher's Renamed.
            wait_for(lambda: notified("Deleted", "watch.txt") and notified("Created", "watched-renamed.txt"), "peer rename delete/create notifications")
            (a.mount / "watched-renamed.txt").unlink()
            wait_for(lambda: not (b.mount / "watched-renamed.txt").exists(), "peer delete visibility")
            wait_for(lambda: notified("Deleted", "watched-renamed.txt"), "peer delete notification")
        finally:
            stop.write_text("stop")
            watcher.wait(timeout=10)
        assert watcher.returncode == 0, (run / "watch.stderr.log").read_text()
        b.quiet()
        passed("real peer FileSystemWatcher create/change/rename/delete notifications and existing read-only handle refresh")

        # Ordinary PowerShell application calls for mkdir + rename.
        command = f"New-Item -ItemType Directory -Path '{a.mount / 'src'}' | Out-Null; Move-Item -LiteralPath '{a.mount / 'note.txt'}' -Destination '{a.mount / 'src' / 'note.txt'}'"
        ps = subprocess.run(["powershell", "-NoProfile", "-Command", command], env=ENV, capture_output=True, text=True, timeout=20)
        assert ps.returncode == 0, ps.stderr
        wait_for(lambda: (b.mount / "src" / "note.txt").read_bytes() == b"base\n", "live rename")
        passed("PowerShell mkdir/rename replicates with stable file identity")

        target = a.mount / "src" / "note.txt"
        save(a.mount / "src" / "save.tmp", b"atomic replacement\n")
        os.replace(a.mount / "src" / "save.tmp", target)
        wait_for(lambda: (b.mount / "src" / "note.txt").read_bytes() == b"atomic replacement\n", "temp replacement")
        assert not (a.mount / "src" / "save.tmp").exists()
        passed("real mounted temp-file replacement is atomic and replicated")

        a.quiet()
        a.control("branch", "private")
        a.control("checkout", "private")
        save(a.mount / "canary.txt", b"PRIVATE-OLD-CANARY")
        a.quiet()
        a.control("checkpoint", "-m", "private coherent snapshot")
        history = a.control("history")
        checkpoint = next(cp for cp in history if cp["message"] == "private coherent snapshot")
        restored = a.control("restore", checkpoint["id"], "--branch", "snapshot-restored")
        assert not restored["shared"]
        a.control("checkout", restored["id"])
        assert (a.mount / "canary.txt").read_bytes() == b"PRIVATE-OLD-CANARY"
        a.control("checkout", "private")
        passed("checkpoint history and restore produce a useful fresh private mounted branch")
        a.control("checkout", "main")
        assert not (a.mount / "canary.txt").exists()
        assert target.read_bytes() == b"atomic replacement\n"
        a.control("checkout", "private")
        assert (a.mount / "canary.txt").read_bytes() == b"PRIVATE-OLD-CANARY"
        passed("A-B-A checkout at the same mounted path preserves each branch")

        # Keep a real handle open and verify refusal before any branch change.
        with (a.mount / "canary.txt").open("r+b") as handle:
            original = a.control("status")["branch"]["id"]
            assert "BUSY_VIEW" in a.control("checkout", "main", expected_ok=False)
            assert a.control("status")["branch"]["id"] == original
            assert "BUSY_VIEW" in a.control("checkpoint", "-m", "unsafe", expected_ok=False)
        a.quiet()
        passed("checkout/checkpoint refusal with a real open writer preserves selection")

        # A Windows mapping retains a context even after the originating Python
        # file handle is closed; this tests the actual adapter's conservative gate.
        with (a.mount / "canary.txt").open("r+b") as handle:
            mapping = mmap.mmap(handle.fileno(), 0)
        try:
            assert "BUSY_VIEW" in a.control("checkout", "main", expected_ok=False)
        finally:
            mapping.close()
        a.quiet()
        passed("checkout refuses a retained Windows memory mapping")

        a.control("sync")
        time.sleep(1.2)
        assert not (b.mount / "canary.txt").exists()
        private_hash = __import__("hashlib").sha256(b"PRIVATE-OLD-CANARY").hexdigest()
        assert not (b.state / "objects" / private_hash).exists()
        assert all(branch["name"] != "private" for branch in b.control("branches"))
        a.stop()
        a.start(b)
        a.control("sync")
        assert not (b.state / "objects" / private_hash).exists()
        passed("private metadata/objects remain absent from the peer before and after restart")

        bucket = run / "local-test-bucket"
        exported = a.control("bucket-export", "--directory", bucket)
        assert not exported["remote_validated"]
        assert not (bucket / private_hash).exists()
        assert (bucket / exported["manifest"]).exists()
        assert "BUCKET_MUST_BE_OUTSIDE_MOUNT" in a.control("bucket-export", "--directory", a.mount / "bad-bucket", expected_ok=False)
        passed("explicit local test bucket exports shared closure with no private canary or mount recursion")

        # Publish ONLY the visible current cut under a fresh branch ID/name.
        save(a.mount / "canary.txt", b"PUBLIC-CURRENT-STATE")
        a.quiet()
        published = a.control("publish", "released", "--current-state-only")
        a.control("sync")
        wait_for(lambda: any(x["id"] == published["id"] for x in b.control("branches")), "published branch")
        b.quiet()
        b.control("checkout", "released")
        assert (b.mount / "canary.txt").read_bytes() == b"PUBLIC-CURRENT-STATE"
        assert not (b.state / "objects" / private_hash).exists()
        for event in b.control("events"):
            if event["branch"]["id"] == published["id"]:
                assert event.get("dependencies", []) == []
                assert all(not change["parents"] for change in event["changes"])
        b.control("checkout", "main")
        a.control("checkout", "main")
        passed("explicit current-state publication excludes old private ancestry and objects")

        # Offline writes through two REAL mounts, then process restart/reconnect.
        a.stop()
        b.stop()
        a.start()
        b.start()
        save(a.mount / "src" / "note.txt", b"offline-A\n")
        save(b.mount / "src" / "note.txt", b"offline-B\n")
        a.quiet()
        b.quiet()
        a.stop()
        b.stop()
        a.start()
        b.start()
        assert (a.mount / "src" / "note.txt").read_bytes() == b"offline-A\n"
        assert (b.mount / "src" / "note.txt").read_bytes() == b"offline-B\n"
        passed("acknowledged offline saves survive abrupt runtime termination/restart")
        a.stop()
        b.stop()
        a.start(b)
        b.start(a)
        a.control("sync")

        def identical():
            pa, pb = a.control("state"), b.control("state")
            return pa == pb and bool(pa["conflicts"])
        wait_for(identical, "identical conflict convergence")
        assert (a.mount / "src" / "note.txt").read_bytes() == (b.mount / "src" / "note.txt").read_bytes()
        ca, cb = a.control("conflicts"), b.control("conflicts")
        assert ca == cb
        content = [c for c in ca if c["field"] == "content" and c["resolved_by"] is None]
        alternatives = {v["value"] for c in content for v in c["alternatives"]}
        assert __import__("hashlib").sha256(b"offline-A\n").hexdigest() in alternatives
        assert __import__("hashlib").sha256(b"offline-B\n").hexdigest() in alternatives
        passed("disconnected mounted conflicts converge identically; all bytes/base/provenance inspectable")

        a.quiet()
        chosen = content[0]["alternatives"][0]["revision"]
        a.control("resolve", "--revision", chosen, "--conflict", content[0]["id"])
        a.control("sync")
        wait_for(lambda: a.control("state") == b.control("state"), "resolution replication")
        assert all(c["resolved_by"] is not None for c in a.control("conflicts") if c["id"] == content[0]["id"])
        passed("explicit reviewed conflict resolution replicates and retains conflict records")

        # Concurrent same-name creates on real mounts, then exact-review recovery
        # of the displaced stable ID to a free path.
        a.stop()
        b.stop()
        a.start()
        b.start()
        save(a.mount / "collision.txt", b"namespace-A")
        save(b.mount / "collision.txt", b"namespace-B")
        a.stop()
        b.stop()
        a.start(b)
        b.start(a)
        a.control("sync")
        wait_for(lambda: a.control("state") == b.control("state"), "namespace convergence")
        conflict = next(c for c in a.control("conflicts") if c["field"] == "namespace" and c["resolved_by"] is None)
        state = a.control("state")
        contenders = [state["entries"][entity] for entity in conflict["entity"].split(",")]
        loser = next(e for e in contenders if not e["alive"])
        a.quiet()
        a.control("recover", loser["id"], "recovered-collision.txt", "--conflict", conflict["id"])
        a.control("sync")
        wait_for(lambda: (b.mount / "recovered-collision.txt").read_bytes() in [b"namespace-A", b"namespace-B"], "recovered peer namespace")
        assert {(a.mount / "collision.txt").read_bytes(), (a.mount / "recovered-collision.txt").read_bytes()} == {b"namespace-A", b"namespace-B"}
        wait_for(lambda: a.control("conflicts") == b.control("conflicts"), "reviewed namespace records")
        passed("displaced namespace alternatives recover through RPC onto both real mounts with exact review")

        # Simulate lost acknowledgement after durable peer receipt, using a
        # fault-enabled source whose own peer receives an already committed file.
        a.stop()
        a.start()
        save(a.mount / "ack-loss.txt", b"durable-upload-before-ack")
        a.quiet()
        a.stop()
        a.start(b, fault="peer_ack_received")
        wait_for(lambda: a.process.poll() is not None, "fault-injected upload ack loss")
        assert a.process.returncode == 86
        a.stop()
        a.start(b)
        a.control("sync")
        wait_for(lambda: (b.mount / "ack-loss.txt").read_bytes() == b"durable-upload-before-ack", "ack-loss replay")
        wait_for(lambda: a.control("status")["outgoing_unacknowledged"] == 0, "outbox ack catchup")
        events = [e["id"] for e in b.control("events")]
        assert len(events) == len(set(events))
        passed("lost upload acknowledgement restarts/replays without duplicate semantic events")

        # A durable RPC receipt survives a lost result; payload reuse is rejected.
        a.quiet()
        request = str(uuid.uuid4())
        first = a.control("--request-id", request, "checkpoint", "-m", "retry receipt")
        a.stop()
        a.start(b)
        assert first == a.control("--request-id", request, "checkpoint", "-m", "retry receipt")
        assert "REQUEST_ID_REUSED" in a.control("--request-id", request, "checkpoint", "-m", "different", expected_ok=False)
        passed("RPC retry identity survives restart and rejects changed payload")

        report = {"passed": True, "kind": "two-local-processes-real-WinFsp-mounts", "real_two_computers": False,
                  "run_directory": str(run), "devices": [a.identity, b.identity], "checks": checks,
                  "limitations": ["No actual second computer", "No power-loss/storage-controller qualification", "No general editor/build-tool compatibility claim"]}
        args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print("Evidence:", args.report, flush=True)
    except Exception as error:
        args.report.write_text(json.dumps({"passed":False,"checks":checks,"error":repr(error),"run_directory":str(run)},indent=2)+"\n",encoding="utf-8")
        raise
    finally:
        a.stop()
        b.stop()


if __name__ == "__main__":
    main()
