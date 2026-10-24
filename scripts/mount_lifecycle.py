"""Bounded process cleanup for mount harnesses (W6-P0-MOUNT-LIFECYCLE).

Stop callbacks accept a timeout and apply it to their control command.
A successful kill cannot turn a failed graceful stop into passing qualification.
Logs, exit records and recoverable stores must survive failed runs.
"""
import json
import math
import os
import shutil
import subprocess
import time
from pathlib import Path

GRACEFUL_WAIT = 20
DUMP_WAIT = 60
KILL_WAIT = 10


def _kill_command_tree(pid, deadline):
    """Kill Windows launchers and their children within the shared kill budget."""
    if os.name != "nt":
        return None
    if time.monotonic() >= deadline:
        return {"error": "kill deadline expired"}
    taskkill = Path(os.environ.get("SystemRoot", "C:/Windows")) / "System32/taskkill.exe"
    try:
        killer = subprocess.Popen([str(taskkill), "/PID", str(pid), "/T", "/F"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            creationflags=subprocess.CREATE_NO_WINDOW)
    except OSError as error:
        return {"error": str(error)}
    result = {"pid": killer.pid}
    try:
        stdout, stderr = killer.communicate(timeout=max(0, min(2, deadline - time.monotonic())))
        result["output"] = (stdout + stderr).decode(errors="replace")[-4000:]
    except subprocess.TimeoutExpired:
        try:
            killer.kill()
        except OSError as error:
            result["kill_error"] = str(error)
        try:
            killer.communicate(timeout=max(0, min(1, deadline - time.monotonic())))
        except subprocess.TimeoutExpired:
            result["stranded"] = True
    result["returncode"] = killer.poll()
    return result


def run_bounded(command, *, timeout, **kwargs):
    """subprocess.run without its unbounded post-timeout communicate/wait.

    Do not use Popen as a context manager: its __exit__ waits without a bound.
    A timed-out client may itself hold a handle into the namespace under test.
    Windows launchers and descendants share one post-timeout kill deadline.
    """
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("a finite, positive command deadline is required")
    proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, **kwargs)
    try:
        stdout, stderr = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired as error:
        deadline = time.monotonic() + KILL_WAIT
        tree = _kill_command_tree(proc.pid, deadline)
        if tree:
            error.add_note("process-tree termination: " + json.dumps(tree))
        try:
            if proc.poll() is None:
                proc.kill()
        except OSError:
            pass
        try:
            proc.communicate(timeout=max(0, deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            raise RuntimeError(f"STRANDED command pid {proc.pid}, parent exit={proc.poll()}, "
                               f"tree={tree}: {command!r}") from error
        if tree and tree.get("stranded"):
            raise RuntimeError(f"STRANDED process-tree killer: {tree}") from error
        raise
    return subprocess.CompletedProcess(command, proc.returncode, stdout, stderr)


def _capture_dump(pid, target_dir):
    procdump = shutil.which("procdump") or shutil.which("procdump64")
    if not procdump:
        return None
    dump = Path(target_dir) / f"daemon-{pid}.dmp"
    try:
        run_bounded([procdump, "-accepteula", "-ma", str(pid), str(dump)], timeout=DUMP_WAIT)
    except Exception as error:
        return f"{type(error).__name__}: {error}"
    return str(dump) if dump.exists() else "procdump produced no file"


def _record(proc, log_path, mount):
    return {"pid": proc.pid, "mount": str(mount), "log": str(log_path),
            "graceful": None, "killed": False, "dump": None,
            "returncode": None, "stranded": False}


def _kill(proc, record, kill_wait):
    started = time.monotonic()
    try:
        proc.kill()
        record["killed"] = True
    except OSError as error:
        record["kill_error"] = f"{type(error).__name__}: {error}"
    try:
        proc.wait(timeout=kill_wait)
    except subprocess.TimeoutExpired:
        pass
    record["stranded"] = proc.poll() is None
    record["kill_seconds"] = round(time.monotonic() - started, 3)


def _save(proc, record, log_path, started):
    record["returncode"] = proc.poll()
    record["seconds"] = round(time.monotonic() - started, 3)
    state_path = Path(log_path).with_name(Path(log_path).stem + "-exit-state.json")
    record["exit_state"] = str(state_path)
    state_path.write_text(json.dumps(record, indent=2), encoding="utf-8")
    if record["stranded"]:
        print(f"STRANDED daemon pid {proc.pid} serving {record['mount']}; "
              f"evidence in {state_path}", flush=True)
    return record


def stop_daemon(stop, proc, log_path, mount, graceful_wait=GRACEFUL_WAIT,
                kill_wait=KILL_WAIT):
    """The control command and process exit share one graceful deadline."""
    record = _record(proc, log_path, mount)
    started = time.monotonic()
    if proc.poll() is None:
        try:
            stop(graceful_wait)
        except Exception as error:
            record["graceful"] = False
            record["graceful_error"] = f"{type(error).__name__}: {error}"
        # A durability error reply is still a stop request. Give the daemon
        # the rest of its deadline to detach and exit before forcing a kill.
        remaining = max(0, graceful_wait - (time.monotonic() - started))
        try:
            proc.wait(timeout=remaining)
            if record["graceful"] is None:
                record["graceful"] = True
        except subprocess.TimeoutExpired as error:
            record["graceful"] = False
            record["exit_error"] = str(error)
    else:
        record["already_exited"] = True
    if proc.poll() is None:
        record["dump"] = _capture_dump(proc.pid, Path(log_path).parent)
        _kill(proc, record, kill_wait)
    return _save(proc, record, log_path, started)


def kill_daemon(proc, log_path, mount, kill_wait=KILL_WAIT):
    """Explicit forced-exit acceptance: persist evidence even on kill failure."""
    record = _record(proc, log_path, mount)
    started = time.monotonic()
    _kill(proc, record, kill_wait)
    if record["stranded"]:
        record["dump"] = _capture_dump(proc.pid, Path(log_path).parent)
    return _save(proc, record, log_path, started)


def stop_all(entries, graceful_wait=GRACEFUL_WAIT, kill_wait=KILL_WAIT):
    """Attempt all existing daemons; fail on any unclean exit or fallback."""
    records = [stop_daemon(*entry, graceful_wait=graceful_wait, kill_wait=kill_wait)
               for entry in reversed(entries)]
    failed = [r for r in records if r["stranded"] or r["killed"]
              or r["graceful"] is False or r["returncode"] != 0]
    if failed:
        raise RuntimeError("unclean daemon shutdown(s): " + json.dumps(failed))
    return records


def _selftest():
    import sys
    import tempfile
    from unittest.mock import patch
    home = Path(tempfile.mkdtemp(prefix="tkfs-lifecycle-"))
    log = home / "daemon.log"
    log.write_text("")
    sleeper = [sys.executable, "-c", "import time; time.sleep(600)"]
    # No dump utility touches processes during this non-mounted self-test.
    with patch(__name__ + "._capture_dump", return_value=None):
        for mode in ("refused", "ignored", "kill-error", "deadline", "stop-all"):
            proc = subprocess.Popen(sleeper)
            real_kill = proc.kill
            try:
                if mode == "ignored":
                    proc.kill = lambda: None
                if mode == "kill-error":
                    def denied():
                        raise PermissionError("kill refused")
                    proc.kill = denied
                def stop(timeout):
                    if mode in ("refused", "stop-all"):
                        raise RuntimeError("no control")
                    if mode == "deadline":
                        time.sleep(timeout)
                if mode == "stop-all":
                    try:
                        stop_all([(stop, proc, log, "Z:")], graceful_wait=.1, kill_wait=.1)
                    except RuntimeError:
                        pass
                    else:
                        raise AssertionError("fallback kill must fail qualification")
                    continue
                record = stop_daemon(stop, proc, log, "Z:", graceful_wait=.1, kill_wait=.1)
                assert record["graceful"] is False, record
                assert record["stranded"] == (mode in ("ignored", "kill-error")), record
                assert Path(record["exit_state"]).exists()
                if mode == "deadline":
                    assert record["seconds"] < .4, record
            finally:
                real_kill()
                proc.wait(timeout=5)
        result = run_bounded([sys.executable, "-c", "print('ok')"], timeout=5, text=True)
        assert result.returncode == 0 and result.stdout.strip() == "ok"
        try:
            run_bounded(sleeper, timeout=.1)
        except subprocess.TimeoutExpired:
            pass
        else:
            raise AssertionError("a command timeout must fail")
        if os.name == "nt":
            # A launcher may leave a child holding its stdout/stderr pipes.
            # Killing only that launcher must not strand the real workload.
            ready = home / "child-pid.txt"
            launcher = [sys.executable, "-c",
                "import pathlib,subprocess,sys,time; "
                "child=subprocess.Popen([sys.executable,'-c','import time; time.sleep(600)']); "
                f"pathlib.Path({str(ready)!r}).write_text(str(child.pid)); time.sleep(600)"]
            orphaned = False
            try:
                try:
                    run_bounded(launcher, timeout=2)
                except subprocess.TimeoutExpired:
                    assert ready.exists(), "launcher did not spawn its child"
                except Exception:
                    orphaned = True
                    raise
                else:
                    raise AssertionError("a launcher timeout must fail")
            finally:
                if orphaned and ready.exists():
                    _kill_command_tree(int(ready.read_text()), time.monotonic() + KILL_WAIT)
        # A failed RPC must not kill a daemon already exiting with an error.
        proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(.1); raise SystemExit(1)"])
        def failed_stop(timeout):
            raise RuntimeError("durability failure")
        record = stop_daemon(failed_stop, proc, log, "Z:", graceful_wait=5)
        assert record["returncode"] == 1 and not record["killed"] and not record["stranded"], record
    print("mount_lifecycle selftest ok")


if __name__ == "__main__":
    _selftest()
