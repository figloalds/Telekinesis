"""The one performance gate: six workloads against a directory, native or mounted.

    python scripts/bench.py --label native
    python scripts/bench.py --label memfs-purge --root M:\\bench --cache-policy purge
    python scripts/bench.py --label mounted --root T:\\bench --cache-policy purge \\
        --compare test-evidence/bench/native.json --reference test-evidence/bench/memfs-purge.json

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
import math
import os
import platform
import random
import shutil
import sqlite3
import statistics
import subprocess
import sys
import time
from datetime import datetime, timezone
from contextlib import closing
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import bench_build  # noqa: E402  (the .NET harness generator and build runner)
from mount_lifecycle import run_bounded, stop_all  # noqa: E402  (bounded daemon shutdown)
from peer_readiness import wait_peer_ready  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
WORKLOADS = ("copy_small", "build", "seq_1g", "random_4k", "git_status_50k", "remote_visibility")
MIB = 1024 * 1024
GIT_FIXTURE_MODE = "packed-native-copy-v1"
# Target ratios from docs/revamp/WAVE_6_REPLACEMENT.md section 8 (user decision 2026-10-26).
TARGETS = {"copy_small": 1.3, "build_clean": 1.3, "build_noop": 1.2,
           "seq_write_1g": 1.2, "seq_read_1g": 1.2, "random_4k": 1.5, "git_status_50k": 1.3}
# Bound by WinFsp's request model, so their target is measured against the
# memory-filesystem reference run in the same guest and session with the same
# cleanup cache policy. Their native ratio is still printed and recorded.
REFERENCE_BOUND = frozenset({"copy_small", "seq_write_1g", "git_status_50k"})
CACHE_POLICIES = ("native", "purge", "standby")
# native/mount.c sets FlushAndPurgeOnCleanup=1 on every production mount.
PRODUCTION_CACHE_POLICY = "purge"


def wait_peer_metadata(source, peer, timeout=120):
    """Native backend reads only; never probe the served namespace here.

    Seeing the committed last event proves metadata arrival at that cut only.
    It does not establish payload readiness; matrix uses wait_peer_ready before
    measuring an idle connected peer. Keep this metadata-only diagnostic separate.
    """
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("peer catch-up timeout must be finite and positive")

    def query(state, sql, params=()):
        uri = (state / "metadata.sqlite").as_uri() + "?mode=ro"
        with closing(sqlite3.connect(uri, uri=True, timeout=.2)) as db:
            return db.execute(sql, params).fetchone()
    head = query(source, "SELECT seq,id FROM events ORDER BY seq DESC LIMIT 1")
    started = time.monotonic()
    if head:
        while not query(peer, "SELECT 1 FROM events WHERE id=?", (head[1],)):
            if time.monotonic() - started >= timeout:
                raise TimeoutError(f"peer initial metadata catch-up did not reach {head} within {timeout}s")
            time.sleep(.05)
    return {"source_sequence": head[0] if head else 0,
            "source_event": head[1] if head else None,
            "seconds": time.monotonic() - started, "timeout_seconds": timeout}


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
    """Copy a fixed tracked tree; Git object creation is outside the workload.

    The gate measures status, not add. Build and pack its immutable fixture on
    native storage once, then copy the same bytes into native and mounted trees.
    This avoids thousands of unrelated object fsyncs during mount preparation.
    """
    repo = root / "repo"
    home = Path("C:/tkfs-bench") if os.name == "nt" else Path.home() / "tkfs-bench"
    fixtures = (home / "git-fixtures").resolve()
    if root == fixtures or root in fixtures.parents or fixtures in root.parents:
        raise ValueError("--root must be separate from the native Git fixture cache")
    fixtures.mkdir(parents=True, exist_ok=True)
    fixture = fixtures / f"{GIT_FIXTURE_MODE}-{args.git_files}"
    marker = fixture / ".git/tkfs-bench-fixture.json"
    expected = {"mode": GIT_FIXTURE_MODE, "files": args.git_files}
    git = ["git", "-c", "core.fsmonitor=false", "-c", "core.untrackedCache=false",
           "-c", "user.name=bench", "-c", "user.email=bench@example.invalid"]
    if not marker.exists():
        if fixture.exists():
            raise RuntimeError(f"incomplete native Git fixture: {fixture}")
        builder = fixtures / f"building-{args.git_files}-{time.time_ns()}"
        builder.mkdir()
        rng = random.Random(7)
        for index in range(args.git_files):
            folder = builder / f"dir{index // 100:03d}"
            if index % 100 == 0:
                folder.mkdir()
            (folder / f"file{index:05d}.txt").write_bytes(rng.randbytes(64 + rng.randrange(192)))
        bench_build.command(git + ["init", "-q"], cwd=builder)
        bench_build.command(git + ["add", "-A"], cwd=builder)
        bench_build.command(git + ["commit", "-q", "-m", "bench"], cwd=builder)
        bench_build.command(git + ["gc", "--quiet", "--prune=now"], cwd=builder)
        (builder / ".git/tkfs-bench-fixture.json").write_text(json.dumps(expected))
        # Both resolved move targets must stay in the generated native cache.
        if builder.resolve().parent != fixtures or fixture.resolve().parent != fixtures:
            raise RuntimeError("Git fixture move escaped its cache directory")
        builder.rename(fixture)
    if json.loads(marker.read_text()) != expected:
        raise RuntimeError(f"incompatible native Git fixture: {fixture}")
    tree_rm(repo)
    shutil.copytree(fixture, repo, copy_function=shutil.copyfile)
    if args.local_state and args.tkfs:
        # Close already delivered the cached bytes through WinFsp Cleanup.
        # Finish their delayed seals before warming the steady-state tree.
        bench_build.command([args.tkfs, "--state", args.local_state, "flush"])
    tracked = bench_build.command(git + ["ls-files", "-z"], cwd=repo)[1]
    assert tracked.count("\0") == args.git_files, "incomplete tracked Git fixture"
    return repo


def git_status_50k(repo, args, run):
    # core.fscache=true is what the Git for Windows installer configures; the
    # Portable Git in the isolated guest leaves it unset (section 8).
    elapsed, output = bench_build.command(
        ["git", "-c", "core.fsmonitor=false", "-c", "core.untrackedCache=false",
         "-c", f"core.fscache={args.git_fscache}", "status", "--porcelain"], cwd=repo)
    assert output == "", f"dirty tree: {output[:200]}"
    return {"git_status_50k": elapsed}


def build(root, args, log_dir, report):
    """Clean and no-op builds of the generated .NET harness, via the Wave 6 runner."""
    harness = root / "dotnet"
    tree_rm(harness)
    bench_build.generate(harness, args.projects, args.files, args.framework)
    build_args = argparse.Namespace(runs=args.runs, projects=args.projects, files=args.files,
                                    jobs=args.jobs, framework=args.framework)
    detail = report.setdefault("build_details", {})
    try:
        bench_build.measure(harness, build_args, log_dir, detail)
    finally:
        # A failed no-op/incremental validation must retain earlier validated
        # clean samples without treating the failed observation as a pass.
        for sample in detail.get("samples", []):
            if sample["validated"] and not sample["warmup"]:
                report["samples"].setdefault(f"build_{sample['scenario']}", []).append(sample["elapsed_ms"])
    samples = {"build_clean": [], "build_noop": [], "build_incremental": []}
    for sample in detail["samples"]:
        if sample["validated"] and not sample["warmup"]:
            samples[f"build_{sample['scenario']}"].append(sample["elapsed_ms"])
    return samples


def remote_visibility(root, peer_root, args, run):
    """Prime caches, update on peer, measure visibility from local event apply."""
    name = "remote-visibility.bin"
    original = random.Random(run + 100).randbytes(4096)
    payload = random.Random(run).randbytes(4096)
    with (peer_root / name).open("wb") as stream:
        stream.write(original)
        stream.flush()
        os.fsync(stream.fileno())
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        try:
            if (root / name).read_bytes() == original:
                break
        except OSError:
            pass
        time.sleep(.005)
    else:
        raise TimeoutError("cache priming did not replicate")
    (root / name).stat()
    with (peer_root / name).open("wb") as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())
    started_ns = time.time_ns()
    if args.local_state and args.tkfs:
        expected = hashlib.sha256(payload).hexdigest()
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            result = json.loads(bench_build.command([args.tkfs, "--state", args.local_state,
                "rpc", "lookup", json.dumps({"path": args.mount_prefix + "/" + name})])[1])
            if result["entry"]["content"]["digest"] == expected:
                started_ns = int(result["applied_ns"])
                break
            time.sleep(.005)
        else:
            raise TimeoutError("peer event not applied")
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        try:
            if (root / name).read_bytes() == payload:
                elapsed = (time.time_ns() - started_ns) / 1_000_000
                (peer_root / name).unlink()
                return {"remote_visibility": elapsed}
        except OSError:
            pass
        time.sleep(0.005)
    raise TimeoutError(f"{name} not visible under {root} within 60 s")


def matrix(args):
    """Run the same gate natively, mounted, and with a paired idle peer."""
    exe = args.matrix_tkfs.resolve()
    common = ["--runs", str(args.runs), "--copy-files", str(args.copy_files),
              "--large-mib", str(args.large_mib), "--random-ops", str(args.random_ops),
              "--git-files", str(args.git_files), "--projects", str(args.projects),
              "--files", str(args.files), "--jobs", str(args.jobs), "--framework", args.framework,
              "--git-fscache", args.git_fscache]
    mounted_policy = ["--cache-policy", args.matrix_cache_policy]
    if args.matrix_reference:
        mounted_policy += ["--reference", str(args.matrix_reference.resolve())]
    if args.only:
        common += ["--only", args.only]
    if args.keep:
        common += ["--keep"]
    base = ROOT / "test-evidence/bench" / f"{args.label}-native.json"
    failures = []
    native = run_bounded([sys.executable, __file__, "--label", args.label + "-native",
                             "--output", str(base), *common], timeout=args.matrix_timeout, text=True)
    print(native.stdout, end="", flush=True)
    if native.returncode:
        return native.returncode
    flags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    home = ROOT / "test-runs/bench" / f"{args.label}-lifecycle-{time.time_ns()}"
    home.mkdir(parents=True)
    (home / "matrix-context.json").write_text(json.dumps({
        "binary": str(exe), "binary_sha256": hashlib.sha256(exe.read_bytes()).hexdigest(),
        "command": sys.argv, "baseline": str(base),
        "reference": str(args.matrix_reference) if args.matrix_reference else None,
        "reference_sha256": hashlib.sha256(args.matrix_reference.read_bytes()).hexdigest()
            if args.matrix_reference else None,
        "cache_policy": args.matrix_cache_policy}, indent=2), encoding="utf-8")
    processes = []
    def cli(state, *command, timeout=20):
        result = run_bounded(list(map(str, [exe, "--state", state, *command])), timeout=timeout, text=True)
        result.check_returncode()
        return json.loads(result.stdout)
    def start(name, mount):
        state = home / name
        cli(state, "init", "--repo", "benchmark")
        log = (home / f"{name}.log").open("w")
        command = [str(exe), "--state", str(state), "daemon", "--listen", "127.0.0.1:0"]
        if mount:
            command += ["--mount", mount]
        proc = subprocess.Popen(command, stdout=log, stderr=log, creationflags=flags)
        processes.append((state, proc, log, mount))
        (home / f"{name}-process.json").write_text(json.dumps({
            "pid": proc.pid, "mount": mount, "state": str(state),
            "log": str(home / f"{name}.log")}), encoding="utf-8")
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                raise RuntimeError((home / f"{name}.log").read_text())
            if (state / "control.json").exists():
                return state
            time.sleep(.05)
        raise TimeoutError("mount startup")
    try:
        if os.name != "nt":
            raise RuntimeError("matrix mount letters currently require Windows; use --root on Linux")
        state = start("mounted", args.matrix_mount)
        root = Path(args.matrix_mount + "/bench")
        mounted = run_bounded([sys.executable, __file__, "--label", args.label + "-mounted",
            "--root", str(root), "--local-state", str(state), "--tkfs", str(exe),
            "--compare", str(base), *mounted_policy, *common], timeout=args.matrix_timeout, text=True)
        print(mounted.stdout, end="", flush=True)
        # Retain callback costs even when completed measurements miss a target.
        # They distinguish adapter/metadata work from driver cache purge cost.
        (home / "mounted-stats.json").write_text(json.dumps(cli(state, "rpc", "mount-stats", "{}")))
        mounted_report = json.loads((ROOT / "test-evidence/bench" / f"{args.label}-mounted.json").read_text())
        if mounted.returncode:
            if not performance_only_failure(mounted_report):
                raise RuntimeError("mounted workload failed: " + mounted.stderr[-2000:])
            # Complete correctness with slow measurements is still useful for
            # the peer comparison. Keep the gate failed; any missing sample,
            # I/O/build error or cleanup timeout still stops further mounts.
            failures.append("mounted-performance: " + "; ".join(mounted_report["failures"]))
        peer = start("peer", args.matrix_peer_mount)
        endpoint = json.loads((home / "mounted.log").read_text().splitlines()[0])["peer"]
        cli(peer, "pair", endpoint, cli(state, "pairing-code")["code"])
        cli(state, "flush")
        catchup = wait_peer_ready(state, peer, args.peer_catchup_timeout,
            observation=lambda value: (home / "peer-catchup-progress.json").write_text(
                json.dumps(value, indent=2)))
        cli(peer, "flush")
        (home / "peer-catchup.json").write_text(json.dumps(catchup, indent=2))
        peer_root = Path(args.matrix_peer_mount + "/bench")
        idle = run_bounded([sys.executable, __file__, "--label", args.label + "-idle-peer",
            "--root", str(root), "--peer-root", str(peer_root), "--local-state", str(state),
            "--tkfs", str(exe), "--mount-prefix", "bench", "--compare", str(base), *mounted_policy, *common],
            timeout=args.matrix_timeout, text=True)
        print(idle.stdout, end="", flush=True)
        if idle.returncode:
            failures.append("idle-peer")
        idle_report = json.loads((ROOT / "test-evidence/bench" / f"{args.label}-idle-peer.json").read_text())
        overhead = peer_overhead(mounted_report, idle_report)
        (home / "peer-overhead.json").write_text(json.dumps(overhead, indent=2))
        failures.extend(overhead["failures"])
        (home / "idle-stats.json").write_text(json.dumps(cli(state, "rpc", "mount-stats", "{}")))
    except Exception as error:
        failures.append(f"{type(error).__name__}: {error}")
    finally:
        # Bounded graceful stop, bounded kill, stranded daemons fail the
        # run (W6-P0-MOUNT-LIFECYCLE). Never wait forever on a deleting process.
        try:
            stop_all([(lambda timeout, s=state: cli(s, "stop", timeout=timeout), proc, home / f"{state.name}.log", mnt)
                      for state, proc, log, mnt in processes], )
        except RuntimeError as error:
            failures.append(f"unclean-daemon: {error}")
        finally:
            for _, _, log, _ in processes:
                log.close()
        (home / "matrix-result.json").write_text(json.dumps({"failures": failures}, indent=2), encoding="utf-8")
    print("Matrix failures:", failures)
    return int(bool(failures))


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


def observe_workload(report, args, name, run, body):
    """Bracket each mounted repetition; keep RPC cost outside its timed body."""
    if not (args.local_state and args.tkfs):
        return body()

    def snapshot():
        return json.loads(bench_build.command(
            [args.tkfs, "--state", args.local_state, "rpc", "mount-stats", "{}"],
            timeout=20)[1])

    record = {"run": run, "warmup": run == 0, "before": snapshot()}
    report.setdefault("callback_observations", {}).setdefault(name, []).append(record)
    started = time.perf_counter()
    try:
        return body()
    except BaseException as error:
        record["error"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        record["wall_ms"] = ms(started)
        try:
            record["after"] = snapshot()
        except Exception as error:
            record["snapshot_error"] = f"{type(error).__name__}: {error}"
            if "error" not in record:
                raise


def summarize(samples):
    return {key: {"median_ms": statistics.median(values), "min_ms": min(values),
                  "max_ms": max(values), "samples": len(values)}
            for key, values in samples.items()}


def host_info():
    info = {"hostname": platform.node(), "os": platform.platform(), "cpus": os.cpu_count(),
            "python": sys.version.split()[0]}
    for tool in (["git", "--version"], ["dotnet", "--version"]):
        try:
            info[tool[0]] = bench_build.command(tool)[1]
        except Exception as error:  # noqa: BLE001
            info[tool[0]] = f"unavailable: {error}"
    return info


def target_basis(metric):
    return "reference" if metric in REFERENCE_BOUND else "native"


def target_failure(metric):
    return f"{metric}: exceeds {TARGETS[metric]}x {target_basis(metric)}"


def print_table(summary, compare, reference=None):
    base = compare["summary"] if compare else {}
    floor = reference["summary"] if reference else {}
    width = max((len(k) for k in summary), default=6)
    header = f"{'metric':{width}s} {'median ms':>11s} {'min':>9s} {'max':>9s}"
    if base:
        header += f" {'vs ' + compare['label']:>16s}"
        if floor:
            header += f" {'vs ' + reference['label']:>16s}"
        header += f" {'target':>16s}"
    print(header)
    for key, row in summary.items():
        line = f"{key:{width}s} {row['median_ms']:11.1f} {row['min_ms']:9.1f} {row['max_ms']:9.1f}"
        if base and key in base:
            ratios = {"native": row["median_ms"] / base[key]["median_ms"]}
            line += f" {ratios['native']:15.2f}x"
            if floor.get(key, {}).get("median_ms", 0) > 0:
                ratios["reference"] = row["median_ms"] / floor[key]["median_ms"]
            if floor:
                line += f" {ratios['reference']:15.2f}x" if "reference" in ratios else f" {'':>16s}"
            target = TARGETS.get(key)
            if target is not None:
                basis = target_basis(key)
                ratio = ratios.get(basis)
                mark = " no reference" if ratio is None else (" ok" if ratio <= target else " FAIL")
                line += f" {'<=' + str(target) + 'x ' + basis:>16s}{mark}"
        print(line)


def performance_only_failure(report):
    if report.get("errors"):
        return False
    if any(name != "remote_visibility" or reason != "no --peer-root"
           for name, reason in report.get("skipped", {}).items()):
        return False
    allowed = {target_failure(metric) for metric in TARGETS}
    failures = report.get("failures")
    return isinstance(failures, list) and bool(failures) and all(f in allowed for f in failures)


def peer_overhead(mounted, idle):
    result = {"limit": 1.2, "ratios": {}, "failures": []}
    for metric in TARGETS:
        if metric not in mounted["summary"]:
            continue
        base = mounted["summary"][metric]["median_ms"]
        row = idle["summary"].get(metric)
        if base <= 0 or not row:
            result["failures"].append(f"idle-peer overhead: {metric} missing measurement")
            continue
        ratio = row["median_ms"] / base
        result["ratios"][metric] = ratio
        if ratio > result["limit"]:
            result["failures"].append(f"idle-peer overhead: {metric} exceeds 1.2x mounted")
    return result


def compatibility(report, other, name):
    """Workload sizes and the Git client must match whatever the run is judged against."""
    failures = []
    dimensions = ("runs", "copy_files", "large_mib", "random_ops", "git_files",
                  "projects", "files", "jobs", "framework")
    for key in dimensions:
        if report["args"].get(key) != other.get("args", {}).get(key):
            failures.append(f"incompatible {name}: {key}")
    if "git_status_50k" in report["summary"]:
        if report.get("git_fixture_mode") != other.get("git_fixture_mode"):
            failures.append(f"incompatible {name}: git_fixture_mode")
        if report["args"].get("git_fscache") != other.get("args", {}).get("git_fscache"):
            failures.append(f"incompatible {name}: git_fscache")
    return failures


def gate(report, compare, reference=None):
    """A missing measurement or incompatible baseline/reference cannot pass the gate."""
    failures = []
    failures.extend(report.get("errors", []))
    expected = {"copy_small": ["copy_small"], "build": ["build_clean", "build_noop", "build_incremental"],
                "seq_1g": ["seq_write_1g", "seq_read_1g"], "random_4k": ["random_4k"],
                "git_status_50k": ["git_status_50k"], "remote_visibility": ["remote_visibility"]}
    for workload in (report["args"].get("only") or ",".join(WORKLOADS)).split(","):
        if workload == "remote_visibility" and not report["args"].get("peer_root"):
            continue
        for metric in expected[workload]:
            if report["summary"].get(metric, {}).get("samples", 0) != report["args"]["runs"]:
                failures.append(f"{metric}: missing measurements")
    if compare:
        failures.extend(compatibility(report, compare, "baseline"))
        bound = [key for key in report["summary"] if key in REFERENCE_BOUND]
        if reference is not None and bound:
            failures.extend(compatibility(report, reference, "reference"))
            # The reference must share the candidate's cleanup cache policy and
            # the guest and client it ran in; a cross-session floor proves nothing.
            policy = reference.get("args", {}).get("cache_policy")
            if policy not in ("purge", "standby") or policy != report["args"].get("cache_policy"):
                failures.append("incompatible reference: cache_policy")
            for key in ("hostname", "git"):
                if reference.get("host", {}).get(key) != report["host"].get(key):
                    failures.append(f"incompatible reference: host {key}")
        for key, row in report["summary"].items():
            if key not in TARGETS:
                continue
            basis = target_basis(key)
            if basis == "reference" and reference is None:
                failures.append(f"{key}: no memory-filesystem reference")
                continue
            source = reference if basis == "reference" else compare
            base = source["summary"].get(key, {}).get("median_ms", 0)
            if base <= 0:
                failures.append(f"{key}: missing {basis} measurement")
            elif row["median_ms"] > base * TARGETS[key]:
                failures.append(target_failure(key))
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
    parser.add_argument("--matrix-tkfs", type=Path, help="run the native/mounted/idle-peer matrix with this binary")
    parser.add_argument("--matrix-timeout", type=float, default=3600,
                        help="deadline in seconds for each workload child")
    parser.add_argument("--peer-catchup-timeout", type=float, default=120,
                        help="cumulative metadata/payload import deadline, excluded from idle-peer timing")
    parser.add_argument("--matrix-mount", default="X:")
    parser.add_argument("--matrix-peer-mount", default="Y:")
    parser.add_argument("--local-state", type=Path)
    parser.add_argument("--tkfs", type=Path)
    parser.add_argument("--mount-prefix", default="bench")
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--only", help="comma-separated subset of " + ",".join(WORKLOADS))
    parser.add_argument("--compare", type=Path, help="native report to judge native-bound targets against")
    parser.add_argument("--reference", type=Path, help="same-session memory-filesystem report for the "
                        "reference-bound targets (copy, sequential write, Git)")
    parser.add_argument("--cache-policy", choices=CACHE_POLICIES, default="native",
                        help="cleanup cache policy of the filesystem under --root")
    parser.add_argument("--git-fscache", choices=("true", "false"), default="true",
                        help="Git client core.fscache for git_status_50k (section 8: true)")
    parser.add_argument("--matrix-reference", type=Path,
                        help="memory-filesystem reference report for the matrix's mounted runs")
    parser.add_argument("--matrix-cache-policy", choices=CACHE_POLICIES[1:], default=PRODUCTION_CACHE_POLICY,
                        help="cleanup cache policy of the matrix binary's mount")
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
    if not math.isfinite(args.peer_catchup_timeout) or args.peer_catchup_timeout <= 0:
        parser.error("peer catch-up timeout must be finite and positive")
    if args.matrix_tkfs:
        selected = set(args.only.split(",")) if args.only else set(WORKLOADS)
        if selected & {"copy_small", "seq_1g", "git_status_50k"} and not args.matrix_reference:
            parser.error("copy, sequential write and Git are judged against a same-session "
                         "memory-filesystem reference; pass --matrix-reference")
        if args.matrix_reference and not args.matrix_reference.is_file():
            parser.error(f"reference report not found: {args.matrix_reference}")
        return matrix(args)
    if args.runs < 1 or min(args.copy_files, args.large_mib, args.random_ops,
                           args.git_files, args.projects, args.files, args.jobs) < 1:
        parser.error("runs and workload sizes must be positive")

    bench_home = Path("C:/tkfs-bench") if os.name == "nt" else Path.home() / "tkfs-bench"
    # GetFinalPathNameByHandle is not implemented by every mounted filesystem.
    root = Path(os.path.abspath(args.root or bench_home / args.label))
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
    def measure(name, body):
        run_workload(name, samples, args.runs,
                     lambda run: observe_workload(report, args, name, run, lambda: body(run)))
    if "git_status_50k" in selected:
        report["git_fixture_mode"] = GIT_FIXTURE_MODE
    try:
        if "copy_small" in selected:
            measure("copy_small", lambda run: copy_small(root, stage, args, run))
        if "seq_1g" in selected or "random_4k" in selected:
            buffer = random.Random(3).randbytes(MIB)
            if "seq_1g" in selected:
                measure("seq_1g", lambda run: seq_1g(root, buffer, args, run))
            elif not (root / "large.bin").exists():
                seq_1g(root, buffer, args, 0)
            if "random_4k" in selected:
                measure("random_4k", lambda run: random_4k(root, args, run))
            if not args.keep:
                (root / "large.bin").unlink()
        if "git_status_50k" in selected:
            repo = git_setup(root, args)
            measure("git_status_50k", lambda run: git_status_50k(repo, args, run))
            if not args.keep:
                tree_rm(repo)
        if "build" in selected:
            try:
                for key, values in observe_workload(report, args, "build", None,
                        lambda: build(root, args, log_dir, report)).items():
                    samples[key] = values
                    print(json.dumps({"workload": key, "samples_ms": [round(v, 1) for v in values]}), flush=True)
            except Exception as error:  # noqa: BLE001  the other workloads still count
                report["skipped"]["build"] = f"{type(error).__name__}: {error}"[:2000]
                print(f"build skipped: {report['skipped']['build'][:300]}", flush=True)
            if not args.keep:
                tree_rm(root / "dotnet")
        if "remote_visibility" in selected:
            if args.peer_root:
                measure("remote_visibility", lambda run: remote_visibility(root, args.peer_root, args, run))
            else:
                report["skipped"]["remote_visibility"] = "no --peer-root"
    except Exception as error:
        report.setdefault("errors", []).append(f"{type(error).__name__}: {error}")
        raise
    finally:
        report["summary"] = summarize(samples)
        report["finished"] = datetime.now(timezone.utc).isoformat()
        compare = json.loads(args.compare.read_text(encoding="utf-8")) if args.compare else None
        reference = json.loads(args.reference.read_text(encoding="utf-8")) if args.reference else None
        if reference is not None:
            report["reference"] = {
                "path": str(args.reference), "label": reference.get("label"),
                "sha256": hashlib.sha256(args.reference.read_bytes()).hexdigest(),
                "cache_policy": reference.get("args", {}).get("cache_policy"),
                "started": reference.get("started")}
        report["failures"] = gate(report, compare, reference)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, indent=1), encoding="utf-8")
    print()
    print_table(report["summary"], compare, reference)
    for name, reason in report["skipped"].items():
        print(f"skipped {name}: {reason[:200]}")
    print(f"\nreport: {output}")
    return 1 if report["failures"] else 0


if __name__ == "__main__":
    sys.exit(main())
