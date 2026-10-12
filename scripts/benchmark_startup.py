"""Benchmark real remounts on an isolated SQLite backup of an existing state.

The supplied state is only read. No orchestrator or remote peer is started.
"""
from __future__ import annotations

import argparse
from contextlib import closing, contextmanager
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import statistics
import subprocess
import tempfile
import time

from test_artifacts import TEST_RUNS, add_artifact_options, prepare_artifacts, finish_artifacts


@contextmanager
def connect(state):
    db = sqlite3.connect((state / "metadata.sqlite").as_uri() + "?mode=ro", uri=True)
    db.execute("PRAGMA query_only=ON")
    try:
        yield db
    finally:
        db.close()


def fingerprint(state):
    with connect(state) as db:
        db.execute("BEGIN")
        projections = {branch: json.loads(payload) for branch, payload in db.execute("SELECT * FROM current_state")}
        for branch, entity, entry, heads in db.execute("SELECT * FROM current_parts"):
            projections[branch]["entries"][entity] = json.loads(entry)
            projections[branch]["heads"].update(json.loads(heads))
        conflicts = []
        for payload, resolved in db.execute("SELECT payload,resolved_by FROM conflicts ORDER BY id"):
            conflict = json.loads(payload)
            conflict["resolved_by"] = resolved
            conflicts.append(conflict)
        data = {"projections": projections, "conflicts": conflicts,
                "events": list(db.execute("SELECT * FROM events ORDER BY id")),
                "frontiers": list(db.execute("SELECT * FROM event_frontier ORDER BY branch,event")),
                "outbox": list(db.execute("SELECT * FROM outbox ORDER BY event")),
                "peer_known": list(db.execute("SELECT * FROM peer_known ORDER BY peer,event"))}
        return hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()


def run_mount(exe, state, mount, log, *, add_suffix=False):
    flags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
    started = time.perf_counter()
    with log.open("w", encoding="utf-8") as output:
        process = subprocess.Popen([str(exe), "daemon", "--state", str(state), "--mount", str(mount)],
                                   stdout=output, stderr=subprocess.STDOUT, creationflags=flags)
        try:
            with log.open(encoding="utf-8", errors="replace") as reader:
                while True:
                    line = reader.readline()
                    if line:
                        try:
                            item = json.loads(line)
                        except ValueError:
                            continue
                        if item.get("ready"):
                            ready_ms = (time.perf_counter() - started) * 1000
                            assert item["mounted"]
                            break
                    else:
                        if process.poll() is not None:
                            raise RuntimeError(f"Worker exited: {log.read_text(errors='replace')[-4000:]}")
                        if time.perf_counter() - started > 600:
                            raise TimeoutError(f"No ready response in 600 seconds: {log}")
                        time.sleep(0.025)
            root_entries = len(list(mount.iterdir()))
            if add_suffix:
                sample = mount / f"__tkfs_startup_benchmark_{time.time_ns()}__.txt"
                with sample.open("xb") as f:
                    f.write(b"snapshot cursor replay benchmark\n")
                    f.flush()
                    os.fsync(f.fileno())
            subprocess.run([str(exe), "--runtime", str(state / "runtime.json"), "stop"],
                           check=True, capture_output=True, text=True, timeout=30, creationflags=flags)
            assert process.wait(timeout=30) == 0
            return {"ready_ms": round(ready_ms, 3), "root_entries": root_entries,
                    "real_mount": True, "suffix_added": add_suffix}
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=30)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--exe", type=Path, required=True)
    parser.add_argument("--baseline-exe", type=Path)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--report", type=Path, default=Path("test-evidence/startup-benchmark.json"))
    add_artifact_options(parser)
    options = parser.parse_args()
    if options.runs < 1:
        parser.error("--runs must be positive")
    prepare_artifacts(options)
    run = Path(tempfile.mkdtemp(prefix="startup-benchmark-", dir=TEST_RUNS)).resolve()
    state = run / "state"
    state.mkdir()
    report = {"source": str(options.state.resolve()), "passed": False, "runs": [],
              "cache": "OS caches were not flushed", "real_mount": True}
    try:
        with connect(options.state.resolve()) as source, closing(sqlite3.connect(state / "metadata.sqlite")) as dest:
            source.backup(dest)
        shutil.copytree(options.state / "objects", state / "objects")
        if (options.state / "staging").exists():
            shutil.copytree(options.state / "staging", state / "staging")
        with connect(state) as db:
            report["journal_events"] = db.execute("SELECT count(*) FROM events").fetchone()[0]
            report["existing_snapshots"] = db.execute("SELECT count(*) FROM sqlite_master WHERE name='startup_snapshots'").fetchone()[0]
        report["objects"] = len([p for p in (state / "objects").iterdir() if p.is_file()])
        expected = fingerprint(state)
        cases = []
        if options.baseline_exe:
            cases.append(("baseline", options.baseline_exe.resolve(), False))
        cases.append(("first_open", options.exe.resolve(), False))
        cases.extend((f"warm_{i+1}", options.exe.resolve(), i == options.runs - 1) for i in range(options.runs))
        cases.append(("suffix_replay", options.exe.resolve(), False))
        for name, exe, add_suffix in cases:
            print(f"START {name}", flush=True)
            result = run_mount(exe, state, run / "mount", run / f"{name}.log", add_suffix=add_suffix)
            if not add_suffix:
                assert fingerprint(state) == expected, f"Semantic state changed in {name}"
            else:
                expected = fingerprint(state)
                with connect(state) as db:
                    result["suffix_events"] = db.execute("SELECT count(*) FROM events e JOIN journal_sequence j ON j.event=e.id JOIN startup_snapshots s ON s.branch=e.branch WHERE j.seq>s.cursor").fetchone()[0]
                    assert result["suffix_events"] > 0, "No actual suffix to replay"
            result.update(case=name, binary_sha256=hashlib.sha256(exe.read_bytes()).hexdigest())
            report["runs"].append(result)
            print(json.dumps(result), flush=True)
        warm = [r["ready_ms"] for r in report["runs"] if r["case"].startswith("warm_")]
        report["warm_median_ms"] = statistics.median(warm)
        report["passed"] = True
    finally:
        finish_artifacts(run, report, options)
        options.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(f"REPORT {options.report.resolve()}", flush=True)


if __name__ == "__main__":
    main()
