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
    from mount_lifecycle import run_bounded
    started = time.perf_counter()
    result = run_bounded(list(map(str, argv)), cwd=cwd,
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
            # Preserve the command and both observations before a correctness
            # assertion can abort the benchmark. A changed mtime alone does not
            # establish that the compiler ran, but still fails no-op stability.
            (log_dir / f"{scenario}-{repetition}.txt").write_text(output, encoding="utf-8")
            changed = sorted(path for path in after if after[path] != before.get(path))
            item = {"scenario": scenario, "repetition": repetition, "warmup": repetition == 0,
                    "elapsed_ms": elapsed, "changed_targets": changed, "before": before,
                    "outputs": after, "answer": answer, "validated": False}
            samples.append(item)
            observation = log_dir / f"{scenario}-{repetition}.json"
            observation.write_text(json.dumps(item, indent=1), encoding="utf-8")
            if int(answer) != expected + delta:
                raise AssertionError(f"stale/wrong build: {answer} != {expected + delta}")
            if scenario == "noop" and changed:
                raise AssertionError(f"no-op changed targets: {changed}")
            if scenario == "incremental":
                if not any(path.startswith("Lib0/") for path in changed):
                    raise AssertionError("incremental build missed edited target")
                if any(path.startswith(f"Lib{i}/") for i in range(1, args.projects)
                       for path in changed):
                    raise AssertionError(f"incremental recompiled unrelated libraries: {changed}")
            item["validated"] = True
            observation.write_text(json.dumps(item, indent=1), encoding="utf-8")
            print(json.dumps({key: value for key, value in item.items() if key not in ("before", "outputs")}),
                  flush=True)
    medians = {}
    for scenario in ("clean", "noop", "incremental"):
        times = [s["elapsed_ms"] for s in samples
                 if s["scenario"] == scenario and not s["warmup"]]
        medians[scenario] = {"median_ms": statistics.median(times),
                             "min_ms": min(times), "max_ms": max(times), "samples": len(times)}
    return {"samples": samples, "summary": medians}


