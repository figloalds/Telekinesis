"""Matched many-small-file .NET builds on native storage or an isolated TKFS mount.

All sources, intermediates and outputs stay below a newly created run directory.
No existing store or mount is opened. This measures warm builds; it never claims
cold-cache, synchronization, or the user's real-solution qualification.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import sqlite3
import statistics
import subprocess
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
FLAGS = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0


def command(argv, *, cwd=None, timeout=600):
    started = time.perf_counter()
    result = subprocess.run(list(map(str, argv)), cwd=cwd, capture_output=True,
                            text=True, timeout=timeout, creationflags=FLAGS)
    elapsed = (time.perf_counter() - started) * 1000
    if result.returncode:
        raise RuntimeError(f"{argv}: {result.returncode}\n{result.stdout}\n{result.stderr}")
    return elapsed, result.stdout.strip()


def digest(path):
    with Path(path).open("rb") as stream:
        result = hashlib.file_digest(stream, "sha256")
    return result.hexdigest()


def generate(root, projects, files, framework):
    root.mkdir(parents=True, exist_ok=False)
    (root / "NuGet.Config").write_text(
        "<configuration><packageSources><clear/></packageSources></configuration>\n",
        encoding="utf-8")
    (root / "Directory.Build.props").write_text(
        '<Project><PropertyGroup><Deterministic>true</Deterministic>'
        '<UseSharedCompilation>false</UseSharedCompilation>'
        '<BuildInParallel>true</BuildInParallel>'
        f'<TargetFramework>{framework}</TargetFramework>'
        '</PropertyGroup></Project>\n', encoding="utf-8")
    for project in range(projects):
        folder = root / f"Lib{project}"
        folder.mkdir()
        (folder / f"Lib{project}.csproj").write_text(
            '<Project Sdk="Microsoft.NET.Sdk"/>\n', encoding="utf-8")
        for index in range(files):
            (folder / f"C{index}.cs").write_text(
                source(project, index, index), encoding="utf-8")
        (folder / "Sum.cs").write_text(
            f"namespace Lib{project}; public static class Sum {{ public static int Value() => "
            + " + ".join(f"C{i}.Value()" for i in range(files)) + "; }\n",
            encoding="utf-8")
    app = root / "App"
    app.mkdir()
    (app / "App.csproj").write_text(
        '<Project Sdk="Microsoft.NET.Sdk"><PropertyGroup><OutputType>Exe</OutputType>'
        '</PropertyGroup><ItemGroup>'
        + "".join(f'<ProjectReference Include="../Lib{i}/Lib{i}.csproj"/>'
                  for i in range(projects))
        + '</ItemGroup></Project>\n', encoding="utf-8")
    (app / "Program.cs").write_text(
        "System.Console.WriteLine("
        + " + ".join(f"Lib{i}.Sum.Value()" for i in range(projects))
        + ");\n", encoding="utf-8")


def source(project, index, value):
    return (f"namespace Lib{project}; public static class C{index} "
            f"{{ public static int Value() => {value}; }}\n")


def outputs(root):
    # obj assemblies identify compilation, whereas bin timestamps can include copies.
    return {path.relative_to(root).as_posix(): {"mtime_ns": path.stat().st_mtime_ns,
                                        "sha256": digest(path)}
            for path in sorted(root.glob("*/obj/Release/*/*.dll"))}


def measure(root, args, log_dir, report):
    base = ["dotnet", "build", "App/App.csproj", "-c", "Release",
            "--no-restore", f"-m:{args.jobs}", "-nologo", "-v:minimal"]
    command(["dotnet", "restore", "App/App.csproj", "--ignore-failed-sources"], cwd=root)
    samples = []
    report["samples"] = samples  # retain completed samples even if a later build fails
    expected = args.projects * args.files * (args.files - 1) // 2
    for scenario in ("clean", "noop", "incremental"):
        command(base, cwd=root)
        for repetition in range(args.runs + 1):  # one warm-up per scenario
            if scenario == "clean":
                command(["dotnet", "clean", "App/App.csproj", "-c", "Release",
                         "-nologo"], cwd=root)
            delta = 0
            if scenario == "incremental":
                delta = repetition + 1
                path = root / "Lib0/C0.cs"
                path.write_text(source(0, 0, delta), encoding="utf-8")
                # Ensure timestamp-sensitive tools see each edit on coarse filesystems.
                # This delay is outside the measured build.
                time.sleep(0.02)
            before = outputs(root)
            elapsed, output = command(base, cwd=root)
            after = outputs(root)
            _, answer = command(["dotnet", f"App/bin/Release/{args.framework}/App.dll"], cwd=root)
            if int(answer) != expected + delta:
                raise AssertionError(f"stale/wrong build: {answer} != {expected + delta}")
            changed = sorted(path for path in after if after[path] != before.get(path))
            if scenario == "noop" and changed:
                raise AssertionError(f"no-op recompiled targets: {changed}")
            if scenario == "incremental":
                if not any(path.startswith("Lib0/") for path in changed):
                    raise AssertionError("incremental build missed edited target")
                if any(path.startswith(f"Lib{i}/") for i in range(1, args.projects)
                       for path in changed):
                    raise AssertionError(f"incremental recompiled unrelated libraries: {changed}")
            (log_dir / f"{scenario}-{repetition}.txt").write_text(output, encoding="utf-8")
            item = {"scenario": scenario, "repetition": repetition, "warmup": repetition == 0,
                    "elapsed_ms": elapsed, "changed_targets": changed, "outputs": after}
            samples.append(item)
            print(json.dumps({key: value for key, value in item.items() if key != "outputs"}),
                  flush=True)
    medians = {}
    for scenario in ("clean", "noop", "incremental"):
        times = [s["elapsed_ms"] for s in samples
                 if s["scenario"] == scenario and not s["warmup"]]
        medians[scenario] = {"median_ms": statistics.median(times),
                             "min_ms": min(times), "max_ms": max(times), "samples": len(times)}
    return {"samples": samples, "summary": medians}


def start_mount(exe, state, mount, log):
    command([exe, "init", "--state", state, "--repo", "wave6-build"])
    if os.name != "nt":
        mount.mkdir()
    output = log.open("w", encoding="utf-8")
    process = subprocess.Popen([str(exe), "daemon", "--state", str(state),
                               "--mount", str(mount)], stdout=output,
                               stderr=subprocess.STDOUT, creationflags=FLAGS)
    try:
        started = time.monotonic()
        with log.open(encoding="utf-8", errors="replace") as reader:
            while time.monotonic() - started < 120:
                if process.poll() is not None:
                    raise RuntimeError(f"mount exited: {log.read_text(errors='replace')[-4000:]}")
                line = reader.readline()
                if line:
                    try:
                        message = json.loads(line)
                    except ValueError:
                        continue
                    if message.get("ready"):
                        if not message.get("mounted"):
                            raise RuntimeError(f"mount failed: {message.get('mount_error')}")
                        return process, output
                else:
                    time.sleep(0.025)
        raise TimeoutError("isolated mount not ready in 120 seconds")
    except BaseException:
        process.kill()
        process.wait(timeout=30)
        output.close()
        raise


def fence_probe(exe, run):
    """Probe the actual old executable, never a source-level approximation."""
    state = run / "fence-probe"
    command([exe, "init", "--state", state, "--repo", "wave6-fence"])
    with sqlite3.connect(state / "metadata.sqlite") as db:
        db.execute("PRAGMA user_version=600")
        db.execute("INSERT OR REPLACE INTO config VALUES('schema:content','999')")
    result = subprocess.run([str(exe), "init", "--state", str(state), "--repo", "wave6-fence"],
                            capture_output=True, text=True, timeout=60, creationflags=FLAGS)
    return {"exe_sha256": digest(exe), "markers": {"user_version": 600, "schema:content": "999"},
            "returncode": result.returncode, "stdout": result.stdout, "stderr": result.stderr,
            "rejects_future_marker": result.returncode != 0,
            "requires_isolated_store_and_launcher_fence": result.returncode == 0}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--label", required=True)
    parser.add_argument("--exe", type=Path, help="release TKFS binary; omitted for native")
    parser.add_argument("--output", type=Path, default=ROOT / "test-runs/wave6-build")
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--projects", type=int, default=8)
    parser.add_argument("--files", type=int, default=128)
    parser.add_argument("--jobs", type=int, default=4)
    parser.add_argument("--framework", default="net10.0")
    parser.add_argument("--antivirus-policy", required=True)
    parser.add_argument("--fence-probe", action="store_true")
    args = parser.parse_args()
    if min(args.runs, args.projects, args.files, args.jobs) < 1:
        parser.error("counts must be positive")
    if args.fence_probe and not args.exe:
        parser.error("--fence-probe requires --exe")
    run = args.output.resolve() / str(uuid.uuid4())
    run.mkdir(parents=True)
    report = {"label": args.label, "passed": False, "host": platform.node(),
              "platform": platform.platform(), "cpu_count": os.cpu_count(),
              "source_revision": command(["git", "rev-parse", "HEAD"], cwd=ROOT)[1],
              "source_diff_sha256": hashlib.sha256(
                  command(["git", "diff", "--binary"], cwd=ROOT)[1].encode()).hexdigest(),
              "harness_sha256": digest(__file__), "dotnet": command(["dotnet", "--info"])[1],
              "rustc": command(["rustc", "--version"])[1], "run": str(run),
              "configuration": vars(args) | {"output": str(args.output), "exe": str(args.exe)},
              "cache": "warm; OS cache not flushed", "sync": "none",
              "durability": "default TKFS FULL SQLite, native close may be weaker; no explicit fsync",
              "not_measured": ["cold cache", "explicit fsync workload", "sync load",
                               "user real solution", "IDE/watch", "interrupted build"]}
    process = output = None
    try:
        if args.exe:
            args.exe = args.exe.resolve()
            report["exe_sha256"] = digest(args.exe)
            report["exe_build"] = json.loads((args.exe.parent / "BUILD.json").read_text())
            if args.fence_probe:
                report["fence_probe"] = fence_probe(args.exe, run)
            process, output = start_mount(args.exe, run / "state", run / "mount",
                                          run / "worker.txt")
            workspace = run / "mount/solution"
        else:
            workspace = run / "native/solution"
        generate(workspace, args.projects, args.files, args.framework)
        report.update(measure(workspace, args, run, report))
        report["passed"] = True
    except BaseException as error:
        report["error"] = repr(error)
        raise
    finally:
        if process:
            try:
                # MSBuild nodes can retain read handles after a successful build.
                # This is our disposable mount; force-stop persists dirty work.
                command([args.exe, "--runtime", run / "state/runtime.json", "stop", "--force"],
                        timeout=60)
                if process.wait(timeout=60) != 0:
                    raise RuntimeError("worker shutdown failed")
            except BaseException as error:
                report["passed"] = False
                report["shutdown_error"] = repr(error)
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=30)
            output.close()
        (run / "result.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(f"EVIDENCE {run / 'result.json'}", flush=True)
    if not report["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
