"""Exercise the public v2 CLI with two disposable headless WS orchestrators."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--exe", type=Path, default=ROOT / "target/debug" / ("tkfs.exe" if os.name == "nt" else "tkfs"))
    args = parser.parse_args()
    exe = args.exe.resolve()
    flags = {"creationflags": subprocess.CREATE_NO_WINDOW} if os.name == "nt" else {}

    def run(*arguments, input=None, check=True):
        return subprocess.run([str(exe), *map(str, arguments)], input=input,
                              capture_output=True, check=check, timeout=25, **flags)

    def call(config, *arguments, input=None):
        result = json.loads(run("-f", config, *arguments, input=input).stdout)
        if isinstance(result, dict) and result.get("starting"):
            until(lambda: any(p["state"] == result["state"] and p["running"]
                              for p in call(config, "manage", "list")["folders"]))
            return json.loads(run("-f", config, "manage", "inspect", result["state"]).stdout)
        return result

    def until(predicate):
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(0.1)
        raise RuntimeError("Timed out waiting for CLI result")

    with tempfile.TemporaryDirectory(prefix="tkfs-ws-cli-") as folder:
        root = Path(folder)
        processes = []
        logs = []
        try:
            configs = []
            for name in ("a", "b"):
                config = root / name / "orchestrator.toml"
                call(config, "init")
                config.write_text(config.read_text().replace("127.0.0.1:43180", "127.0.0.1:0"))
                log = (config.parent / "daemon.log").open("wb")
                logs.append(log)
                process = subprocess.Popen([str(exe), "-f", str(config), "orchestrator"],
                                           stdin=subprocess.DEVNULL, stdout=log, stderr=log, **flags)
                processes.append((process, config))
                until(lambda: run("-f", config, "manage", "list", check=False).returncode == 0)
                configs.append(config)
            a, b = configs
            endpoint = json.loads((a.parent / "data/orchestrator.json").read_text())["network"]
            code = call(a, "manage", "reset-pairing-code")["code"]
            call(b, "manage", "pair", endpoint, input=(code + "\n").encode())
            source = call(a, "create", "Project", "--headless")
            catalog = call(b, "manage", "remote-list", endpoint)
            assert catalog[0]["repo"] == source["repo"]
            replica = call(b, "manage", "receive", endpoint, source["repo"], "Project", "--headless")
            for origin, target, name, content in (
                (source, replica, "from-a.txt", b"A to B over persistent WS\n"),
                (replica, source, "from-b.txt", b"B to A over the same pool\n"),
            ):
                native = root / name
                native.write_bytes(content)
                run("--runtime", Path(origin["directory"]) / "runtime.json", "import", name, native)
                digest = hashlib.sha256(content).hexdigest()
                until(lambda: run("--runtime", Path(target["directory"]) / "runtime.json",
                                  "cat-object", digest, check=False).stdout == content)
            until(lambda: all(lane["connected"] for lane in call(b, "manage", "peers")[0]["lanes"]))
            print(json.dumps({"ok": True, "transport": "ws", "connections": 4,
                              "bidirectional": True, "public_cli": True}))
        finally:
            for process, config in reversed(processes):
                if process.poll() is None:
                    run("-f", config, "manage", "shutdown", check=False)
                    process.wait(timeout=20)
            for log in logs:
                log.close()


if __name__ == "__main__":
    main()
