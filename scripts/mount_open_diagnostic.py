"""Isolated-VM diagnostic for immutable opens; not a performance gate.

Seed, stop and reopen the store so the first measured read has no live draft.
Namespace I/O runs only in bounded worker processes. Each round exports callback
counters and the native scratch count; shutdown failure prevents the next mount.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parent))
from mount_lifecycle import run_bounded, stop_daemon


def workload(args):
    root = args.root
    payload = bytes(range(256)) * 128
    big = bytes(range(256)) * (256 * 1024)
    if args.worker == "seed":
        for index in range(args.files):
            (root / f"small-{index}.bin").write_bytes(payload)
        (root / "large.bin").write_bytes(big)
    else:
        for index in range(args.files):
            assert (root / f"small-{index}.bin").read_bytes() == payload
        assert (root / "large.bin").read_bytes() == big


def diagnostic(args):
    if os.name != "nt" or not args.isolated_vm:
        raise RuntimeError("Run only in an isolated Windows VM with --isolated-vm")
    exe = args.binary.resolve()
    home = args.home.resolve()
    home.mkdir(parents=True, exist_ok=False)
    state = home / "state"
    record = {"scope": "immutable open/read diagnostic, not full qualification",
              "binary_sha256": hashlib.sha256(exe.read_bytes()).hexdigest(),
              "files": args.files, "large_bytes": 64 * 1024 * 1024, "rounds": []}

    def save():
        for path in [home / "diagnostic.json", args.report]:
            if path:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(json.dumps(record, indent=2))

    def cli(*command, timeout=20):
        result = run_bounded([str(exe), "--state", str(state), *command],
                             timeout=timeout, text=True)
        result.check_returncode()
        return json.loads(result.stdout)

    def worker(mode):
        result = run_bounded([sys.executable, __file__, "--worker", mode,
                              "--root", args.mount + "/", "--files", str(args.files)],
                             timeout=120, text=True)
        result.check_returncode()

    try:
        cli("init", "--repo", "open-diagnostic")
        for phase in ("seed", "measure"):
            log_path = home / (phase + ".log")
            with log_path.open("w") as log:
                proc = subprocess.Popen([str(exe), "--state", str(state), "daemon",
                                         "--listen", "127.0.0.1:0", "--mount", args.mount],
                                        stdout=log, stderr=log,
                                        creationflags=subprocess.CREATE_NO_WINDOW)
                record[phase + "_pid"] = proc.pid
                save()
                try:
                    deadline = time.monotonic() + 30
                    while not (state / "control.json").exists():
                        if proc.poll() is not None:
                            raise RuntimeError(f"daemon exited: {proc.returncode}")
                        if time.monotonic() >= deadline:
                            raise TimeoutError("daemon startup")
                        time.sleep(.02)
                    if phase == "seed":
                        worker("seed")
                        cli("flush")
                    else:
                        for index in range(args.runs):
                            before = cli("rpc", "mount-stats", "{}")
                            started = time.perf_counter()
                            worker("read")
                            after = cli("rpc", "mount-stats", "{}")
                            record["rounds"].append({"round": index,
                                "elapsed_ms": (time.perf_counter() - started) * 1000,
                                "before": before, "after": after,
                                "scratch_files": len(list((state / "scratch").glob("*.scratch")))})
                            save()
                    record[phase + "_stats"] = cli("rpc", "mount-stats", "{}")
                finally:
                    record[phase + "_exit"] = stop_daemon(
                        lambda timeout: cli("stop", timeout=timeout), proc,
                        log_path=log_path, mount=args.mount)
                    save()
                exit_record = record[phase + "_exit"]
                if (exit_record["stranded"] or exit_record["killed"]
                        or exit_record["graceful"] is False or exit_record["returncode"] != 0):
                    raise RuntimeError("unclean daemon shutdown: " + json.dumps(exit_record))
                if "mount I/O operation" in log_path.read_text():
                    raise RuntimeError("mount callback error; see " + str(log_path))
        record["passed"] = True
    except Exception as error:
        record["error"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        save()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--home", type=Path)
    parser.add_argument("--report", type=Path)
    parser.add_argument("--mount", default="P:")
    parser.add_argument("--files", type=int, default=500)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--isolated-vm", action="store_true")
    parser.add_argument("--worker", choices=("seed", "read"), help=argparse.SUPPRESS)
    parser.add_argument("--root", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.worker:
        workload(args)
    else:
        diagnostic(args)
