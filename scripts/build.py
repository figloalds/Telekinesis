"""Build x64 Windows and Linux runnables; Windows uses WSL for the Linux build."""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import tarfile
import tempfile

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ("Cargo.toml", "Cargo.lock", "build.rs", "src", "native", "ui")


def run(argv, *, label=None, **kwargs):
    print("BUILD", label or subprocess.list2cmdline(list(map(str, argv))), flush=True)
    return subprocess.run(list(map(str, argv)), check=True, **kwargs)


def capture(argv, **kwargs):
    return subprocess.check_output(list(map(str, argv)), text=True, **kwargs).strip()


def linux_environment(cargo_home=None):
    env = os.environ.copy()
    env["PATH"] = str(Path.home() / ".cargo/bin") + os.pathsep + env.get("PATH", "")
    if cargo_home:
        env["CARGO_HOME"] = str(Path(cargo_home).expanduser().resolve())
    return env


def preflight(system, env=None):
    if platform.machine().lower() not in ("amd64", "x86_64"):
        raise RuntimeError("This script supports x86_64 Windows and Linux only")
    search_path = (env or os.environ).get("PATH")
    for tool in (("cargo", "rustc") if system == "windows" else ("cargo", "rustc", "cc")):
        if not shutil.which(tool, path=search_path):
            raise RuntimeError(f"Missing {tool}; install Rust and the platform C build tools first")
    if system == "windows":
        sdk = Path(os.environ.get("WINFSP_DIR", "C:/Program Files (x86)/WinFsp"))
        for relative in ("inc/winfsp/winfsp.h", "lib/winfsp-x64.lib"):
            if not (sdk / relative).is_file():
                raise RuntimeError(f"Missing WinFsp SDK file: {sdk / relative}. Set WINFSP_DIR if needed")


def compile_binary(source, target_dir, system, options, output, env=None):
    preflight(system, env)
    triple = "x86_64-pc-windows-msvc" if system == "windows" else "x86_64-unknown-linux-gnu"
    command = ["cargo", "build", "--locked", "--bin", "tkfs", "--target", triple,
               "--target-dir", target_dir, "--jobs", options["jobs"]]
    if options["profile"] == "release":
        command.append("--release")
    if options["offline"]:
        command.append("--offline")
    run(command, cwd=source, env=env)
    name = "tkfs.exe" if system == "windows" else "tkfs"
    binary = Path(target_dir) / triple / options["profile"] / name
    # Validate the executable on its own OS before publishing it to the output folder.
    version = capture([binary, "--version"], env=env)
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    destination = output / name
    shutil.copy2(binary, destination)
    facts = {"target": triple, "profile": options["profile"], "version": version,
             "rustc": capture(["rustc", "--version"], env=env),
             "sha256": hashlib.sha256(destination.read_bytes()).hexdigest()}
    if system == "linux":
        facts["runtime_libraries"] = capture(["ldd", destination], env=env)
    (output / "BUILD.json").write_text(json.dumps(facts, indent=2) + "\n", encoding="utf-8")
    return destination


def snapshot(destination):
    for name in SOURCE:
        source = ROOT / name
        if source.is_dir():
            shutil.copytree(source, destination / name)
        else:
            shutil.copy2(source, destination / name)


def linux_worker(encoded):
    options = json.loads(base64.b64decode(encoded))
    env = linux_environment(options.get("linux_cargo_home"))
    preflight("linux", env)
    # Source and compiler intermediates stay on WSL's native filesystem.
    with tempfile.TemporaryDirectory(prefix="tkfs-build-") as temp:
        source = Path(temp) / "source"
        source.mkdir()
        with tarfile.open(options["archive"], "r:gz") as archive:
            archive.extractall(source, filter="data")
        cache = (Path.home() / ".cache/telekinesis/build/linux"
                 if options["keep_build_cache"] else Path(temp) / "target")
        compile_binary(source, cache, "linux", options, options["output"], env)


def wsl_path(path, distribution):
    return capture(["wsl.exe", "--distribution", distribution, "--exec", "wslpath", "-a", "-u", str(path)])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--platform", choices=("both", "windows", "linux"),
                        default="both" if os.name == "nt" else "linux")
    parser.add_argument("--profile", choices=("release", "debug"), default="release")
    parser.add_argument("--output", type=Path, default=ROOT / "target/runnables")
    parser.add_argument("--offline", action="store_true", help="Use cached Cargo dependencies only")
    parser.add_argument("--keep-build-cache", action="store_true", help="Keep one reusable compiler cache per OS")
    parser.add_argument("--jobs", type=int, default=min(4, os.cpu_count() or 1))
    parser.add_argument("--linux-distro", default="Ubuntu", help="WSL distribution for Linux builds on Windows")
    parser.add_argument("--linux-cargo-home", help="Optional native Linux Cargo cache directory")
    args = parser.parse_args()
    if args.jobs < 1:
        parser.error("--jobs must be positive")
    if os.name != "nt" and args.platform != "linux":
        parser.error("Windows builds require a Windows host with Rust/MSVC and the WinFsp SDK")
    output = args.output.resolve()
    options = {"profile": args.profile, "offline": args.offline, "jobs": args.jobs,
               "keep_build_cache": args.keep_build_cache, "linux_cargo_home": args.linux_cargo_home}
    if os.name == "nt" and args.platform in ("both", "windows"):
        preflight("windows")
    if os.name == "nt" and args.platform in ("both", "linux"):
        if not shutil.which("wsl.exe"):
            parser.error("Linux builds on Windows require WSL with Rust, Python 3.12+ and a C compiler")
        # Check WSL before spending time on the Windows build.
        probe = "import os,pathlib,shutil,sys; os.environ['PATH']=str(pathlib.Path.home()/'.cargo/bin')+':'+os.environ['PATH']; assert sys.version_info >= (3,12), 'WSL needs Python 3.12+'; assert all(shutil.which(t) for t in ('cargo','rustc','cc')), 'WSL needs Rust and a C compiler'"
        run(["wsl.exe", "--distribution", args.linux_distro, "--exec", "python3", "-c", probe])
    scratch_parent = ROOT / "target"
    scratch_parent.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="runnable-build-", dir=scratch_parent) as temp:
        scratch = Path(temp).resolve()
        if not scratch.is_relative_to(scratch_parent.resolve()):
            raise RuntimeError("Temporary build directory escaped the workspace target directory")
        source = scratch / "source"
        source.mkdir()
        snapshot(source)
        built = []
        if args.platform in ("both", "windows"):
            cache = ROOT / "target/build-cache/windows" if args.keep_build_cache else scratch / "windows-target"
            built.append(compile_binary(source, cache, "windows", options, output / "windows-x86_64"))
        if args.platform in ("both", "linux"):
            if os.name == "nt":
                archive_path = scratch / "source.tar.gz"
                with tarfile.open(archive_path, "w:gz") as archive:
                    for name in SOURCE:
                        archive.add(source / name, arcname=name)
                linux_options = {**options, "archive": wsl_path(archive_path, args.linux_distro),
                                 "output": wsl_path(output / "linux-x86_64", args.linux_distro)}
                encoded = base64.b64encode(json.dumps(linux_options).encode()).decode()
                run(["wsl.exe", "--distribution", args.linux_distro, "--exec", "python3",
                     wsl_path(Path(__file__).resolve(), args.linux_distro), "--linux-worker", encoded],
                    label=f"Linux {args.profile} build in WSL ({args.linux_distro})")
                built.append(output / "linux-x86_64/tkfs")
            else:
                cache = ROOT / "target/build-cache/linux" if args.keep_build_cache else scratch / "linux-target"
                built.append(compile_binary(source, cache, "linux", options, output / "linux-x86_64",
                                            linux_environment(args.linux_cargo_home)))
        for binary in built:
            for name in ("LICENSE-GPL-3.0.txt", "LICENSE-SLINT-ROYALTY-FREE-2.0.md", "THIRD-PARTY-NOTICES.md"):
                shutil.copy2(ROOT / name, binary.parent / name)
        (output / "README.txt").write_text(
            "TKFS x86_64 runnables\n\nWindows: windows-x86_64/tkfs.exe (desktop and CLI).\n"
            "Install WinFsp for filesystem mounts.\n"
            "Linux: linux-x86_64/tkfs (headless CLI and FUSE daemon).\n"
            "Built against this Linux environment's glibc; see BUILD.json for libraries.\n"
            "Install fusermount3 and enable /dev/fuse for mounts.\n"
            "Run tkfs --help for commands. BUILD.json records each binary's version and SHA-256.\n",
            encoding="utf-8")
        print("\nCreated runnables:", flush=True)
        for binary in built:
            print(binary, flush=True)
    print("Temporary source snapshots and build output removed.", flush=True)


if __name__ == "__main__":
    try:
        if len(sys.argv) == 3 and sys.argv[1] == "--linux-worker":
            linux_worker(sys.argv[2])
        else:
            main()
    except (RuntimeError, OSError, subprocess.CalledProcessError) as error:
        print(f"Build failed: {error}", file=sys.stderr)
        raise SystemExit(1)
