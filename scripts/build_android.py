"""Build the Android app: the native library for each ABI, then an installable debug APK."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import struct
import subprocess
import sys
import zipfile

from build import ROOT, capture, run

PROJECT = ROOT / "android"
LIBRARY = "libtkfs_android.so"
ABIS = {"arm64-v8a": "aarch64-linux-android", "x86_64": "x86_64-linux-android"}
NDK_VERSION = "27.0.12077973"
# Android 15+ devices may use 16 KiB pages; every loadable segment must be aligned to that.
PAGE_ALIGNMENT = 16384
# Matches minSdk in android/app/build.gradle.
API = 26


def find_sdk():
    for name in ("ANDROID_HOME", "ANDROID_SDK_ROOT"):
        if os.environ.get(name):
            sdk = Path(os.environ[name])
            if not sdk.is_dir():
                raise RuntimeError(f"{name} points to a missing folder: {sdk}")
            return sdk
    raise RuntimeError("Set ANDROID_HOME to your Android SDK folder")


def find_toolchain(sdk):
    configured = os.environ.get("ANDROID_NDK_HOME")
    ndk = Path(configured) if configured else sdk / "ndk" / NDK_VERSION
    host = "windows-x86_64" if os.name == "nt" else "linux-x86_64"
    bin_dir = ndk / "toolchains/llvm/prebuilt" / host / "bin"
    if not bin_dir.is_dir():
        raise RuntimeError(f"Android NDK toolchain not found: {bin_dir}. "
                           f"Install NDK {NDK_VERSION} or set ANDROID_NDK_HOME")
    return ndk, bin_dir


def preflight(abis):
    if not (PROJECT / "native/Cargo.toml").is_file() or not (PROJECT / "settings.gradle").is_file():
        raise RuntimeError(f"No Android project in {PROJECT}. Run this from a full checkout")
    for tool in ("cargo", "rustc", "java"):
        if not shutil.which(tool) and not (tool == "java" and os.environ.get("JAVA_HOME")):
            raise RuntimeError(f"Missing {tool}; install it or add it to PATH")
    if shutil.which("rustup"):
        installed = capture(["rustup", "target", "list", "--installed"]).split()
        missing = [ABIS[abi] for abi in abis if ABIS[abi] not in installed]
        if missing:
            raise RuntimeError(f"Missing Rust targets; run: rustup target add {' '.join(missing)}")


def build_native(abi, ndk, bin_dir, target_dir):
    triple = ABIS[abi]
    suffix = (".cmd", ".exe") if os.name == "nt" else ("", "")
    clang = bin_dir / f"{triple}{API}-clang{suffix[0]}"
    lower = triple.replace("-", "_")
    env = {**os.environ, "ANDROID_NDK_HOME": str(ndk), "CARGO_TARGET_DIR": str(target_dir),
           f"CC_{lower}": str(clang),
           f"AR_{lower}": str(bin_dir / f"llvm-ar{suffix[1]}"),
           f"RANLIB_{lower}": str(bin_dir / f"llvm-ranlib{suffix[1]}"),
           f"CARGO_TARGET_{lower.upper()}_LINKER": str(clang),
           f"CARGO_TARGET_{lower.upper()}_RUSTFLAGS": f"-C link-arg=-Wl,-z,max-page-size={PAGE_ALIGNMENT}"}
    run(["cargo", "build", "--locked", "--release", "--target", triple,
         "--manifest-path", PROJECT / "native/Cargo.toml"], env=env)
    library = target_dir / triple / "release" / LIBRARY
    check_page_alignment(library)
    return library


def check_page_alignment(library):
    """Fails unless every PT_LOAD segment of an ELF64 library is aligned to PAGE_ALIGNMENT."""
    PT_LOAD = 1
    data = library.read_bytes()
    if data[:4] != b"ELF" or data[4] != 2 or data[5] != 1:
        raise RuntimeError(f"{library} is not a little-endian ELF64 file")
    phoff, = struct.unpack_from("<Q", data, 0x20)
    phentsize, phnum = struct.unpack_from("<HH", data, 0x36)
    for index in range(phnum):
        header = phoff + index * phentsize
        kind, = struct.unpack_from("<I", data, header)
        align, = struct.unpack_from("<Q", data, header + 0x30)
        if kind == PT_LOAD and align < PAGE_ALIGNMENT:
            raise RuntimeError(f"{library} has a PT_LOAD segment aligned to {align}, below {PAGE_ALIGNMENT}")


def publish_libraries(built):
    """Replace jniLibs as a whole, so an ABI left out of this build cannot ship a stale library."""
    jni_libs = PROJECT / "app/src/main/jniLibs"
    for abi in ABIS:
        shutil.rmtree(jni_libs / abi, ignore_errors=True)
    for abi, library in built.items():
        (jni_libs / abi).mkdir(parents=True)
        shutil.copy2(library, jni_libs / abi / LIBRARY)


def build_apk(sdk, abis, output, ndk):
    gradle = PROJECT / ("gradlew.bat" if os.name == "nt" else "gradlew")
    run([gradle, ":app:assembleDebug", "--console=plain"], cwd=PROJECT, env={**os.environ, "ANDROID_HOME": str(sdk)})
    apk = PROJECT / "app/build/outputs/apk/debug/app-debug.apk"
    with zipfile.ZipFile(apk) as archive:
        packaged = {name.split("/")[1] for name in archive.namelist()
                    if name.startswith("lib/") and name.endswith("/" + LIBRARY)}
    if packaged != set(abis):
        raise RuntimeError(f"APK holds native libraries for {sorted(packaged)}, expected {sorted(abis)}")
    output.mkdir(parents=True, exist_ok=True)
    destination = output / "tkfs-debug.apk"
    shutil.copy2(apk, destination)
    facts = {"abis": sorted(abis), "native_profile": "release", "apk_signing": "debug", "ndk": ndk.name,
             "rustc": capture(["rustc", "--version"]),
             "sha256": hashlib.sha256(destination.read_bytes()).hexdigest()}
    (output / "BUILD.json").write_text(json.dumps(facts, indent=2) + "\n", encoding="utf-8")
    return destination


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--abi", choices=(*ABIS, "all"), default="all",
                        help="arm64-v8a for phones, x86_64 for emulators (default: all)")
    parser.add_argument("--native-only", action="store_true", help="Build the native library and skip the APK")
    parser.add_argument("--output", type=Path, default=ROOT / "target/runnables/android")
    args = parser.parse_args()
    abis = list(ABIS) if args.abi == "all" else [args.abi]
    preflight(abis)
    sdk = find_sdk()
    ndk, bin_dir = find_toolchain(sdk)
    # Separate from the desktop build, which uses different features and targets.
    target_dir = Path(os.environ.get("CARGO_TARGET_DIR", ROOT / "target/android")).resolve()
    publish_libraries({abi: build_native(abi, ndk, bin_dir, target_dir) for abi in abis})
    if args.native_only:
        print(f"\nNative libraries in {PROJECT / 'app/src/main/jniLibs'}", flush=True)
        return
    apk = build_apk(sdk, abis, args.output.resolve(), ndk)
    print(f"\nCreated {apk}\nInstall with: adb install -r \"{apk}\"", flush=True)


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, OSError, zipfile.BadZipFile, subprocess.CalledProcessError) as error:
        print(f"Build failed: {error}", file=sys.stderr)
        raise SystemExit(1)
