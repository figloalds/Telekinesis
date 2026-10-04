"""Retention for stopped acceptance fixtures; never follow mounts or junctions.

Successful runs remove bulk output immediately. Failed runs expire after seven
days, with a shared 1 GiB budget. Summary reports live outside fixture folders.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shutil
import stat
import time

ROOT = Path(__file__).resolve().parents[1]
TEST_RUNS = ROOT / "test-runs"
MARKER = ".test-artifacts.json"
MAX_AGE = 7 * 24 * 60 * 60
MAX_BYTES = 1024 ** 3


def linked(path):
    info = path.lstat()
    return stat.S_ISLNK(info.st_mode) or bool(getattr(info, "st_file_attributes", 0) & 0x400)


def add_artifact_options(parser):
    parser.add_argument("--keep-artifacts", action="store_true",
                        help="Keep this run's fixtures for inspection until manually removed")


def checked_tree(run, base):
    """Preflight every entry before deletion, including Windows reparse points."""
    run, base = Path(run).absolute(), Path(base).absolute()
    if run == base or not run.is_relative_to(base):
        raise ValueError(f"Fixture is outside its test output directory: {run}")
    # Check ancestors too: resolving first would hide a junction inside base.
    for path in (run, *run.parents):
        info = path.lstat()
        if (stat.S_ISLNK(info.st_mode)
                or getattr(info, "st_file_attributes", 0) & 0x400
                or (path != base and os.path.ismount(path))):
            raise ValueError(f"Refusing linked or mounted fixture path: {path}")
        if path == base:
            break
    if not run.resolve().is_relative_to(base.resolve()):
        raise ValueError(f"Fixture resolves outside test output: {run}")
    total = 0
    pending = [run]
    while pending:
        folder = pending.pop()
        with os.scandir(folder) as entries:
            for entry in entries:
                info = entry.stat(follow_symlinks=False)
                if (stat.S_ISLNK(info.st_mode)
                        or getattr(info, "st_file_attributes", 0) & 0x400
                        or os.path.ismount(entry.path)):
                    raise ValueError(f"Refusing linked or mounted fixture entry: {entry.path}")
                if stat.S_ISDIR(info.st_mode):
                    pending.append(Path(entry.path))
                elif stat.S_ISREG(info.st_mode):
                    total += info.st_size
                elif stat.S_ISSOCK(info.st_mode) or stat.S_ISFIFO(info.st_mode):
                    pass  # Stopped Linux fixtures may retain local socket names.
                else:
                    raise ValueError(f"Refusing special fixture entry: {entry.path}")
    return total


def remove_run(run, base):
    checked_tree(run, base)

    def writable_retry(function, path, exc_info):
        error = exc_info[1]
        # Git fixtures can contain read-only objects. Never relax directory ACLs.
        if not isinstance(error, PermissionError) or not Path(path).is_file():
            raise error
        os.chmod(path, stat.S_IWRITE | stat.S_IREAD)
        function(path)

    shutil.rmtree(run, onerror=writable_retry)


def retained_runs(base):
    """Only completion markers created after owned processes have stopped qualify."""
    base = Path(base)
    if not base.exists():
        return
    candidates = list(base.iterdir())
    # Linux fixtures are one level below the evidence directory.
    linux = base / "linux-evidence"
    if linux.is_dir() and not linked(linux):
        candidates.extend(linux.iterdir())
    for run in candidates:
        try:
            if not run.is_dir() or linked(run):
                continue
            marker = run / MARKER
            if linked(marker):
                continue
            data = json.loads(marker.read_text(encoding="utf-8"))
            if data.get("version") == 1 and data.get("stopped") and not data.get("keep"):
                yield float(data["completed_at"]), run, checked_tree(run, base)
        except (OSError, ValueError, KeyError, TypeError):
            continue  # Unknown, inaccessible, or mounted output is never pruned.


def prune_runs(base=TEST_RUNS, *, apply=True, now=None,
               max_age=MAX_AGE, max_bytes=MAX_BYTES):
    now = time.time() if now is None else now
    runs = sorted(retained_runs(base), key=lambda item: item[0])
    total = sum(size for _, _, size in runs)
    results = []
    for completed, run, size in runs:
        if now - completed < max_age and total <= max_bytes:
            continue
        result = {"path": str(run), "bytes": size, "removed": False}
        if apply:
            try:
                remove_run(run, base)
                result["removed"] = True
            except (OSError, ValueError) as error:
                result["error"] = str(error)
        results.append(result)
        if not apply or result["removed"]:
            total -= size
    return results


def prepare_artifacts(options):
    TEST_RUNS.mkdir(parents=True, exist_ok=True)
    options.report.parent.mkdir(parents=True, exist_ok=True)
    for result in prune_runs():
        print("ARTIFACT RETENTION", json.dumps(result), flush=True)


def finish_artifacts(run, report, options, *, stopped=True, base=TEST_RUNS):
    result = {"retained": True}
    report["artifacts"] = result
    try:
        if not stopped:
            result["reason"] = "Process or mount cleanup was not confirmed"
            return
        checked_tree(run, base)
        keep = options.keep_artifacts or options.report.resolve().is_relative_to(Path(run).resolve())
        marker = {"version": 1, "stopped": True, "keep": keep,
                  "completed_at": time.time()}
        (Path(run) / MARKER).write_text(json.dumps(marker), encoding="utf-8")
        if keep:
            result["reason"] = "Explicitly retained fixtures or report inside run directory"
        elif report.get("passed"):
            remove_run(run, base)
            result["retained"] = False
        else:
            result["reason"] = "Failure diagnostics; expires after seven days or when budget exceeded"
        result["pruned"] = prune_runs(base)
        result["retained"] = Path(run).exists()
    except (OSError, ValueError) as error:
        result["error"] = str(error)
        print("ARTIFACT CLEANUP", str(error), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Delete expired completed fixtures; default previews")
    options = parser.parse_args()
    print(json.dumps(prune_runs(apply=options.apply), indent=2))
