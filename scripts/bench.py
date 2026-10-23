"""The one performance gate: six workloads against a directory, native or mounted.

    python scripts/bench.py --label native
    python scripts/bench.py --label mounted --root T:\\bench --compare test-evidence/bench/native.json

Every workload runs against ``--root`` exactly the same way, so a native run and
a mounted run differ only in the filesystem under that path. One warm-up and
``--runs`` measured repetitions per workload; the report keeps every sample and
prints medians. Warm OS cache throughout; no cache flushing, no antivirus
changes. ``remote_visibility`` needs ``--peer-root`` (a second mount or the
other machine's directory); it is skipped otherwise.
"""
import argparse
import hashlib
import json
import os
import platform
import random
import shutil
import statistics
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import wave6_build  # noqa: E402  (the .NET harness generator and build runner)

ROOT = Path(__file__).resolve().parents[1]
WORKLOADS = ("copy_small", "build", "seq_1g", "random_4k", "git_status_50k", "remote_visibility")
MIB = 1024 * 1024
# Target ratios against native from docs/revamp/WAVE_6_REPLACEMENT.md section 8.
TARGETS = {"copy_small": 1.3, "delete_small": 1.3, "build_clean": 1.3, "build_noop": 1.2,
           "seq_write_1g": 1.2, "seq_read_1g": 1.2, "random_4k": 1.5, "git_status_50k": 1.3}


def ms(started):
    return (time.perf_counter() - started) * 1000


def tree_rm(path):
    def clear_readonly(function, target, _error):  # git object files are read-only on Windows
        os.chmod(target, 0o700)
        function(target)

    if path.exists():
        shutil.rmtree(path, onexc=clear_readonly)


def small_files(seed, count):
    """Deterministic 1-64 KiB payloads in 20 directories; the same set every run."""
    rng = random.Random(seed)
    for index in range(count):
        size = 1024 + rng.randrange(63 * 1024)
        yield f"d{index % 20:02d}/f{index:05d}.bin", rng.randbytes(size)


# --- workloads -------------------------------------------------------------

def copy_small(root, stage, args, run):
    """Copy 2,000 files (1-64 KiB) from a staging tree into the target, then delete them."""
    target = root / "copy"
    tree_rm(target)
    started = time.perf_counter()
    for directory in sorted(p for p in stage.iterdir() if p.is_dir()):
        (target / directory.name).mkdir(parents=True)
        for source in sorted(directory.iterdir()):
            shutil.copyfile(source, target / directory.name / source.name)
    copy_ms = ms(started)
    started = time.perf_counter()
    shutil.rmtree(target)
    return {"copy_small": copy_ms, "delete_small": ms(started)}


def seq_1g(root, buffer, args, run):
    """1 GiB sequential write in 1 MiB pieces, close, fsync separately, then read it back."""
    path = root / "large.bin"
    started = time.perf_counter()
    with path.open("wb") as stream:
        for _ in range(args.large_mib):
            stream.write(buffer)
    write_ms = ms(started)
    started = time.perf_counter()
    with path.open("rb+") as stream:
        os.fsync(stream.fileno())
    fsync_ms = ms(started)
    started = time.perf_counter()
    total = 0
    with path.open("rb") as stream:
        while block := stream.read(MIB):
            total += len(block)
    read_ms = ms(started)
    assert total == args.large_mib * MIB, total
    return {"seq_write_1g": write_ms, "seq_fsync_1g": fsync_ms, "seq_read_1g": read_ms}


def random_4k(root, args, run):
    """10,000 random 4 KiB overwrites of the 1 GiB file, then close."""
    path = root / "large.bin"
    rng = random.Random(run)
    offsets = [rng.randrange(args.large_mib * 256) * 4096 for _ in range(args.random_ops)]
    block = rng.randbytes(4096)
    started = time.perf_counter()
    with path.open("rb+") as stream:
        for offset in offsets:
            stream.seek(offset)
            stream.write(block)
    return {"random_4k": ms(started)}


def git_setup(root, args):
    """50,000 small tracked files in 500 directories, committed once."""
    repo = root / "repo"
    tree_rm(repo)
    repo.mkdir()
    rng = random.Random(7)
    for index in range(args.git_files):
        folder = repo / f"dir{index // 100:03d}"
        if index % 100 == 0:
            folder.mkdir()
        (folder / f"file{index:05d}.txt").write_bytes(rng.randbytes(64 + rng.randrange(192)))
    git = ["git", "-c", "core.fsmonitor=false", "-c", "core.untrackedCache=false",
           "-c", "user.name=bench", "-c", "user.email=bench@example.invalid"]
    wave6_build.command(git + ["init", "-q"], cwd=repo)
    wave6_build.command(git + ["add", "-A"], cwd=repo)
    wave6_build.command(git + ["commit", "-q", "-m", "bench"], cwd=repo)
    return repo


def git_status_50k(repo, args, run):
    elapsed, output = wave6_build.command(
        ["git", "-c", "core.fsmonitor=false", "-c", "core.untrackedCache=false",
         "status", "--porcelain"], cwd=repo)
    assert output == "", f"dirty tree: {output[:200]}"
    return {"git_status_50k": elapsed}


def build(root, args, log_dir):
    """Clean and no-op builds of the generated .NET harness, via the Wave 6 runner."""
    harness = root / "dotnet"
    tree_rm(harness)
    wave6_build.generate(harness, args.projects, args.files, args.framework)
    build_args = argparse.Namespace(runs=args.runs, projects=args.projects, files=args.files,
                                    jobs=args.jobs, framework=args.framework)
    result = wave6_build.measure(harness, build_args, log_dir, {})
    samples = {"build_clean": [], "build_noop": [], "build_incremental": []}
    for sample in result["samples"]:
        if not sample["warmup"]:
            samples[f"build_{sample['scenario']}"].append(sample["elapsed_ms"])
    return samples


def remote_visibility(root, peer_root, args, run):
    """Write on the peer side; time until the same bytes are readable under root."""
    name = f"remote-{run}-{time.time_ns()}.bin"
    payload = random.Random(run).randbytes(4096)
    started = time.perf_counter()
    (peer_root / name).write_bytes(payload)
    deadline = started + 60
    while time.perf_counter() < deadline:
        try:
            if (root / name).read_bytes() == payload:
                elapsed = ms(started)
                (peer_root / name).unlink()
                return {"remote_visibility": elapsed}
        except OSError:
            pass
        time.sleep(0.005)
    raise TimeoutError(f"{name} not visible under {root} within 60 s")


# --- driver ----------------------------------------------------------------

def collect(samples, outcome):
    for key, value in outcome.items():
        samples.setdefault(key, []).append(value)


def run_workload(name, samples, runs, body):
    for run in range(runs + 1):  # run 0 is the warm-up
        outcome = body(run)
        if run:
            collect(samples, outcome)
        print(json.dumps({"workload": name, "run": run, "warmup": run == 0,
                          **{k: round(v, 2) for k, v in outcome.items()}}), flush=True)


def summarize(samples):
    return {key: {"median_ms": statistics.median(values), "min_ms": min(values),
                  "max_ms": max(values), "samples": len(values)}
            for key, values in samples.items()}


def host_info():
    info = {"hostname": platform.node(), "os": platform.platform(), "cpus": os.cpu_count(),
            "python": sys.version.split()[0]}
    for tool in (["git", "--version"], ["dotnet", "--version"]):
        try:
            info[tool[0]] = wave6_build.command(tool)[1]
        except Exception as error:  # noqa: BLE001
            info[tool[0]] = f"unavailable: {error}"
    return info


def print_table(summary, compare):
    base = compare["summary"] if compare else {}
    width = max(len(k) for k in summary)
    header = f"{'metric':{width}s} {'median ms':>11s} {'min':>9s} {'max':>9s}"
    if base:
        header += f" {'vs ' + compare['label']:>12s} {'target':>7s}"
    print(header)
    for key, row in summary.items():
        line = f"{key:{width}s} {row['median_ms']:11.1f} {row['min_ms']:9.1f} {row['max_ms']:9.1f}"
        if base and key in base:
            ratio = row["median_ms"] / base[key]["median_ms"]
            target = TARGETS.get(key)
            mark = "" if target is None else (" ok" if ratio <= target else " FAIL")
            line += f" {ratio:11.2f}x {('<=' + str(target)) if target else '':>7s}{mark}"
        print(line)


def gate(report, compare):
    """A missing measurement or incompatible baseline cannot pass the gate."""
    failures = []
    if compare:
        dimensions = ("runs", "copy_files", "large_mib", "random_ops", "git_files",
                      "projects", "files", "jobs", "framework")
        for key in dimensions:
            if report["args"][key] != compare["args"][key]:
                failures.append(f"incompatible baseline: {key}")
        for key, row in report["summary"].items():
            if key in TARGETS:
                base = compare["summary"].get(key, {}).get("median_ms", 0)
                if base <= 0 or row["median_ms"] > base * TARGETS[key]:
                    failures.append(f"{key}: exceeds {TARGETS[key]}x native")
    if "remote_visibility" in report["summary"]:
        if report["summary"]["remote_visibility"]["median_ms"] > 250:
            failures.append("remote_visibility: exceeds 250 ms")
    failures.extend(f"{key}: {reason}" for key, reason in report["skipped"].items()
                    if key != "remote_visibility" or report["args"].get("peer_root"))
    return failures


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--label", required=True, help="row name, e.g. native or mounted")
    parser.add_argument("--root", type=Path, help="directory under test (default C:/tkfs-bench/<label>)")
    parser.add_argument("--stage", type=Path, help="staging dir for copy sources, on the same drive "
                        "but outside --root (default C:/tkfs-bench/stage)")
    parser.add_argument("--peer-root", type=Path, help="peer-side directory for remote_visibility")
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--only", help="comma-separated subset of " + ",".join(WORKLOADS))
    parser.add_argument("--compare", type=Path, help="earlier report to print ratios against")
    parser.add_argument("--output", type=Path, help="report path (default test-evidence/bench/<label>.json)")
    parser.add_argument("--keep", action="store_true", help="leave generated trees under --root")
    parser.add_argument("--copy-files", type=int, default=2000)
    parser.add_argument("--large-mib", type=int, default=1024)
    parser.add_argument("--random-ops", type=int, default=10_000)
    parser.add_argument("--git-files", type=int, default=50_000)
    parser.add_argument("--projects", type=int, default=8)
    parser.add_argument("--files", type=int, default=128)
    parser.add_argument("--jobs", type=int, default=4)
    parser.add_argument("--framework", default="net10.0")
    args = parser.parse_args()
    if args.runs < 1 or min(args.copy_files, args.large_mib, args.random_ops,
                           args.git_files, args.projects, args.files, args.jobs) < 1:
        parser.error("runs and workload sizes must be positive")

    bench_home = Path("C:/tkfs-bench") if os.name == "nt" else Path.home() / "tkfs-bench"
    root = (args.root or bench_home / args.label).resolve()
    stage = (args.stage or bench_home / "stage").resolve()
    if root == stage or root in stage.parents or stage in root.parents:
        parser.error("--root and --stage must be separate, non-nested directories")
    output = args.output or ROOT / "test-evidence/bench" / f"{args.label}.json"
    selected = set(args.only.split(",")) if args.only else set(WORKLOADS)
    unknown = selected - set(WORKLOADS)
    if unknown:
        parser.error(f"unknown workloads: {sorted(unknown)}")
    root.mkdir(parents=True, exist_ok=True)
    log_dir = ROOT / "test-runs/bench" / f"{args.label}-{datetime.now():%Y%m%dT%H%M%S}"
    log_dir.mkdir(parents=True)

    if "copy_small" in selected:
        tree_rm(stage)
        for relative, payload in small_files(1, args.copy_files):
            path = stage / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(payload)

    report = {"label": args.label, "root": str(root), "started": datetime.now(timezone.utc).isoformat(),
              "args": {k: (str(v) if isinstance(v, Path) else v) for k, v in vars(args).items()},
              "host": host_info(), "script_sha256": hashlib.file_digest(open(__file__, "rb"), "sha256").hexdigest(),
              "samples": {}, "skipped": {}}
    samples = report["samples"]
    try:
        if "copy_small" in selected:
            run_workload("copy_small", samples, args.runs, lambda run: copy_small(root, stage, args, run))
        if "seq_1g" in selected or "random_4k" in selected:
            buffer = random.Random(3).randbytes(MIB)
            if "seq_1g" in selected:
                run_workload("seq_1g", samples, args.runs, lambda run: seq_1g(root, buffer, args, run))
            elif not (root / "large.bin").exists():
                seq_1g(root, buffer, args, 0)
            if "random_4k" in selected:
                run_workload("random_4k", samples, args.runs, lambda run: random_4k(root, args, run))
            if not args.keep:
                (root / "large.bin").unlink()
        if "git_status_50k" in selected:
            repo = git_setup(root, args)
            run_workload("git_status_50k", samples, args.runs, lambda run: git_status_50k(repo, args, run))
            if not args.keep:
                tree_rm(repo)
        if "build" in selected:
            try:
                for key, values in build(root, args, log_dir).items():
                    samples[key] = values
                    print(json.dumps({"workload": key, "samples_ms": [round(v, 1) for v in values]}), flush=True)
            except Exception as error:  # noqa: BLE001  the other workloads still count
                report["skipped"]["build"] = f"{type(error).__name__}: {error}"[:2000]
                print(f"build skipped: {report['skipped']['build'][:300]}", flush=True)
            if not args.keep:
                tree_rm(root / "dotnet")
        if "remote_visibility" in selected:
            if args.peer_root:
                run_workload("remote_visibility", samples, args.runs,
                             lambda run: remote_visibility(root, args.peer_root, args, run))
            else:
                report["skipped"]["remote_visibility"] = "no --peer-root"
    finally:
        report["summary"] = summarize(samples)
        report["finished"] = datetime.now(timezone.utc).isoformat()
        compare = json.loads(args.compare.read_text(encoding="utf-8")) if args.compare else None
        report["failures"] = gate(report, compare)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, indent=1), encoding="utf-8")
    compare = json.loads(args.compare.read_text(encoding="utf-8")) if args.compare else None
    print()
    print_table(report["summary"], compare)
    for name, reason in report["skipped"].items():
        print(f"skipped {name}: {reason[:200]}")
    print(f"\nreport: {output}")
    return 1 if report["failures"] else 0


if __name__ == "__main__":
    sys.exit(main())
