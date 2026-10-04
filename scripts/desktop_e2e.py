"""Real Slint Windows window + isolated mounted desktop acceptance.

Uses the application's explicit gui-test callbacks, not physical mouse/keyboard
automation. Keeps fixtures/screenshots, and shuts down only its own installation.
"""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import time
import uuid
import tomllib

ROOT = Path(__file__).resolve().parents[1]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--exe", type=Path, default=ROOT / "target/slint-ui/debug/tkfs.exe")
    parser.add_argument("--report", type=Path, default=ROOT / "test-evidence" / "DESKTOP-VALIDATION.json")
    parser.add_argument("--theme", choices=("system", "light", "dark"), default="system")
    parser.add_argument("--branches", action="store_true", help="Exercise branch controls in the same disposable mounted installation")
    options = parser.parse_args()
    run = ROOT / "test-runs" / f"desktop-{uuid.uuid4()}"
    app = run / "portable"
    app.mkdir(parents=True)
    exe = app / "tkfs.exe"
    shutil.copy2(options.exe, exe)
    config = app / "orchestrator.toml"
    env = os.environ.copy()
    env["TKFS_UI_TEST_THEME"] = options.theme
    for key in ("TKFS_FAULT", "TKFS_PEER_KEY", "WINFSP_DIR"):
        env.pop(key, None)
    checks = []
    report = {"scope":"one Windows computer, real Slint window, callback automation, real WinFsp mounts; no network", "theme":options.theme, "run":str(run), "exe":str(options.exe.resolve()), "exe_sha256":hashlib.sha256(exe.read_bytes()).hexdigest(), "started_at":datetime.now(timezone.utc).isoformat(), "checks":checks, "screenshots":[]}
    def cli(*args, good=True):
        result = subprocess.run([str(exe),*map(str,args)], env=env,capture_output=True,text=True,timeout=35)
        if good:
            assert result.returncode == 0, (args,result.returncode,result.stdout,result.stderr)
            return json.loads(result.stdout)
        assert result.returncode != 0, (args,result.stdout)
        return result.stderr
    def manage(action,*args,**kwargs):
        return cli("manage","--defaults-file",config,action,*args,**kwargs)
    def phase(name):
        phase_report = run / name / "report.json"
        phase_report.parent.mkdir()
        with (phase_report.parent / "stdout.log").open("wb") as out, (phase_report.parent / "stderr.log").open("wb") as err:
            process = subprocess.Popen([str(exe),"gui-test","--report",str(phase_report)],cwd=ROOT,env={**env,"TKFS_UI_TEST_PHASE":name},stdout=out,stderr=err)
            try:
                process.wait(timeout=115)
            finally:
                if process.poll() is None:
                    process.terminate()
                    process.wait(timeout=10)
        assert process.returncode == 0, (name,process.returncode,(phase_report.parent / "stderr.log").read_text())
        data = json.loads(phase_report.read_text())
        assert data["passed"], data
        checks.extend(data["checks"])
        report["screenshots"].extend(str(p) for p in phase_report.parent.glob("*.bmp"))
    try:
        phase("first")
        config_before = config.read_bytes()
        catalog = manage("list")
        assert len(catalog["states"]) == 2
        assert all(s["observation"]["status"] == "running" for s in catalog["states"])
        assert (app / "Projects/Design studio/hello.txt").read_bytes() == b"UI acceptance durable data"
        checks.append("GUI process exit leaves both independent mounts and supervisor alive")
        hello = manage("hello")
        assert hello["installation_id"] == tomllib.loads(config.read_text())["installation_id"]
        assert "ALREADY_OWNED" in cli("orchestrator","--defaults-file",config,good=False)
        checks.append("versioned installation handshake and duplicate supervisor refusal")
        if options.branches:
            phase("branches-pending")
            pending = json.loads((app / ".tkfs-ui-branch-operation.json").read_text())
            assert pending["response"]["retryable"] and pending["payload"]["op"] == "checkout"
            assert len(manage("list")["states"]) == 2
            phase("branches-resume")
        phase("reopen")
        assert config.read_bytes() == config_before
        for mount in (app / "Projects/Design studio", app / "Projects/Research lab"):
            assert not mount.exists(), mount
        checks.append("GUI reopen preserves exact configuration; explicit shutdown removes fixture mounts")
        # Headless CLI remains available in the same application binary.
        result = subprocess.run([str(exe),"--version"],env=env,capture_output=True,text=True,timeout=10)
        assert result.returncode == 0 and result.stdout.startswith("tkfs ")
        checks.append("CLI modes dispatch without creating a UI window")
        report["passed"] = True
    except Exception as error:
        report["passed"] = False
        report["error"] = repr(error)
        raise
    finally:
        if config.exists():
            try:
                catalog = manage("list")
                cli("--generation",catalog["catalog_generation"],"--request-id",uuid.uuid4(),"manage","--defaults-file",config,"shutdown")
            except Exception:
                pass  # The successful reopen phase has already stopped it.
        report["completed_at"] = datetime.now(timezone.utc).isoformat()
        report["fixture_mounts_removed"] = not (app / "Projects/Design studio").exists() and not (app / "Projects/Research lab").exists()
        options.report.write_text(json.dumps(report,indent=2)+"\n")
        print(json.dumps(report,indent=2),flush=True)

if __name__ == "__main__":
    main()
