"""Bounded daemon shutdown for mount harnesses (W6-P0-MOUNT-LIFECYCLE).

Every wait has a deadline. A daemon that is still alive after the forced kill
is reported as stranded: its PID, mount, log path and exit state are written
next to its log, and the caller must fail the run and start no further mounts.
"""
import json
import os
import shutil
import subprocess
import time
from pathlib import Path

GRACEFUL_WAIT = 20
DUMP_WAIT = 60
KILL_WAIT = 10


def _capture_dump(pid, target_dir):
    """Best-effort user-mode dump with procdump when it is on PATH; bounded."""
    procdump = shutil.which("procdump") or shutil.which("procdump64")
    if not procdump:
        return None
    dump = Path(target_dir) / f"daemon-{pid}.dmp"
    try:
        subprocess.run([procdump, "-accepteula", "-ma", str(pid), str(dump)],
                       capture_output=True, timeout=DUMP_WAIT, check=False)
    except subprocess.TimeoutExpired:
        return f"procdump timed out after {DUMP_WAIT}s"
    return str(dump) if dump.exists() else "procdump produced no file"


def stop_daemon(stop, proc, log_path, mount, graceful_wait=GRACEFUL_WAIT,
                kill_wait=KILL_WAIT):
    """Stop one daemon and return its exit record; `stranded` is True when it
    survived the forced kill. `stop()` issues the graceful stop command."""
    record = {"pid": proc.pid, "mount": str(mount), "log": str(log_path),
              "graceful": None, "killed": False, "dump": None,
              "returncode": None, "stranded": False}
    started = time.monotonic()
    try:
        stop()
        proc.wait(timeout=graceful_wait)
        record["graceful"] = True
    except Exception as error:  # timeout, refused stop, dead control socket
        record["graceful"] = False
        record["graceful_error"] = f"{type(error).__name__}: {error}"
    if proc.poll() is None:
        record["dump"] = _capture_dump(proc.pid, Path(log_path).parent)
        proc.kill()
        record["killed"] = True
        try:
            proc.wait(timeout=kill_wait)
        except subprocess.TimeoutExpired:
            record["stranded"] = True
    record["returncode"] = proc.poll()
    record["seconds"] = round(time.monotonic() - started, 3)
    state_path = Path(log_path).with_name(Path(log_path).stem + "-exit-state.json")
    state_path.write_text(json.dumps(record, indent=2))
    record["exit_state"] = str(state_path)
    if record["stranded"]:
        print(f"STRANDED daemon pid {proc.pid} serving {mount}: "
              f"kill did not complete within {kill_wait}s; evidence in {state_path}",
              flush=True)
    return record


def stop_all(entries):
    """Stop daemons in reverse start order. Each entry is
    (stop_callable, proc, log_path, mount). Raises after all were attempted if
    any daemon is stranded, so a harness fails instead of reporting success."""
    records = [stop_daemon(*entry) for entry in reversed(entries)]
    stranded = [r for r in records if r["stranded"]]
    if stranded:
        raise RuntimeError("stranded daemon(s): " + json.dumps(stranded))
    return records


def _selftest():
    import sys
    import tempfile
    home = Path(tempfile.mkdtemp(prefix="tkfs-lifecycle-"))
    log = home / "daemon.log"
    log.write_text("")
    sleeper = [sys.executable, "-c", "import time; time.sleep(600)"]
    # 1. A refused graceful stop falls back to a bounded kill.
    proc = subprocess.Popen(sleeper)
    record = stop_daemon(lambda: (_ for _ in ()).throw(RuntimeError("no control")),
                         proc, log, "Z:", graceful_wait=1, kill_wait=5)
    assert record["graceful"] is False and record["killed"] and not record["stranded"], record
    assert Path(record["exit_state"]).exists()
    # 2. A daemon that ignores the kill is reported as stranded, not waited on.
    proc = subprocess.Popen(sleeper)
    real_kill = proc.kill
    proc.kill = lambda: None  # emulate a process stuck in kernel cleanup
    record = stop_daemon(lambda: None, proc, log, "Z:", graceful_wait=1, kill_wait=1)
    assert record["stranded"], record
    real_kill()
    proc.wait(timeout=5)
    try:
        stop_all([(lambda: None, proc, log, "Z:")])
    except RuntimeError:
        raise AssertionError("an exited process must not count as stranded")
    print("mount_lifecycle selftest ok")


if __name__ == "__main__":
    _selftest()
