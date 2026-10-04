"""Isolated catalog refusal and initial-bootstrap crash recovery acceptance."""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
import subprocess
import uuid
from orchestrator_e2e import ENV, ROOT, Fixture
from test_artifacts import add_artifact_options, prepare_artifacts, finish_artifacts


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--exe", type=Path, default=ROOT / "target/slint-ui/debug/tkfs.exe")
    parser.add_argument("--report", type=Path, default=ROOT / "test-evidence" / "DESKTOP-CATALOG-VALIDATION.json")
    add_artifact_options(parser)
    options = parser.parse_args()
    prepare_artifacts(options)
    exe = options.exe.resolve()
    run = ROOT / "test-runs" / f"catalog-startup-{uuid.uuid4()}"
    run.mkdir()
    fixtures = []
    checks = []
    report = {"exe":str(exe), "exe_sha256":hashlib.sha256(exe.read_bytes()).hexdigest(), "run":str(run), "started_at":datetime.now(timezone.utc).isoformat(), "checks":checks}
    try:
        for fault in ("orchestrator_bootstrap_marked", "orchestrator_catalog_opened", "orchestrator_catalog_committed"):
            fixture = Fixture(exe, run, fault)
            fixtures.append(fixture)
            process = subprocess.run([str(exe), "orchestrator", "--defaults-file", str(fixture.config)], env={**ENV, "TKFS_FAULT":fault}, capture_output=True, text=True, timeout=20)
            assert process.returncode == 86, (fault, process.returncode, process.stderr)
            marker_path = fixture.data / ".orchestrator-bootstrap.json"
            identity = json.loads(marker_path.read_text())["installation_id"]
            fixture.start()
            assert fixture.manage("hello")["installation_id"] == identity
            assert fixture.manage("list")["states"] == []
            assert not marker_path.exists()
            fixture.shutdown()
            fixture.start()
            assert fixture.manage("hello")["installation_id"] == identity
            fixture.shutdown()
            checks.append(f"{fault}: interrupted fresh bootstrap recovers the same durable installation identity, then strictly reopens")
        fixture = Fixture(exe, run, "established")
        fixtures.append(fixture)
        fixture.start()
        fixture.manage("create", "Retained project", generation=0, operation=str(uuid.uuid4()))
        catalog = fixture.manage("list")
        state = catalog["states"][0]
        hello = fixture.manage("hello")
        portable_exe = fixture.run / "tkfs.exe"
        shutil.copy2(exe, portable_exe)
        portable_config = fixture.run / "orchestrator.toml"
        shutil.copy2(fixture.config, portable_config)
        pin = fixture.run / ".tkfs-ui-identity.json"
        pin.write_text(json.dumps({"installation_id":hello["installation_id"], "data_directory":hello["data_directory"]}))
        config_bytes = portable_config.read_bytes()
        pin_bytes = pin.read_bytes()
        store = fixture.root(state)
        # Literal native evidence belongs only to this fixture; metadata is real.
        sentinel = store / "literal-fixture.bin"
        sentinel.write_bytes(b"literal data preserved across catalog startup refusal")
        fixture.shutdown()
        database = fixture.data / "orchestrator.sqlite"
        original = database.read_bytes()
        baseline = {str(p.relative_to(store)):hashlib.sha256(p.read_bytes()).hexdigest() for p in store.rglob("*") if p.is_file()}
        for invalid in ("empty", "missing", "truncated", "version-zero-schema"):
            database.unlink()
            if invalid == "empty":
                database.write_bytes(b"")
            elif invalid == "truncated":
                database.write_bytes(b"SQLite format 3\0truncated")
            elif invalid == "version-zero-schema":
                with sqlite3.connect(database) as db:
                    db.execute("CREATE TABLE precious(value TEXT)")
                    db.execute("INSERT INTO precious VALUES('keep literal fixture')")
            before = database.read_bytes() if database.exists() else None
            error = fixture.cli("orchestrator", "--defaults-file", fixture.config, good=False)
            assert any(code in error for code in ("REGISTRY_UNINITIALIZED", "DATA_DIRECTORY_NOT_EMPTY", "INVALID_REGISTRY")), error
            gui_report = fixture.run / f"gui-{invalid}" / "report.json"
            gui_report.parent.mkdir()
            process = subprocess.run([str(portable_exe), "gui-test", "--report", str(gui_report)], env={**ENV, "TKFS_UI_TEST_PHASE":"invalid-catalog"}, capture_output=True, text=True, timeout=35)
            assert process.returncode == 0, (process.returncode, process.stderr)
            gui = json.loads(gui_report.read_text())
            assert gui["passed"], gui
            assert portable_config.read_bytes() == config_bytes and pin.read_bytes() == pin_bytes
            assert (database.read_bytes() if database.exists() else None) == before
            assert {str(p.relative_to(store)):hashlib.sha256(p.read_bytes()).hexdigest() for p in store.rglob("*") if p.is_file()} == baseline
            assert not (fixture.data / ".orchestrator-bootstrap.json").exists()
            database.write_bytes(original)
            checks.append(f"established {invalid} catalog: CLI and real GUI refuse startup; config, pin, exact catalog and all retained project-file hashes unchanged")
        fixture.start()
        assert fixture.manage("list")["states"][0]["state_id"] == state["state_id"]
        fixture.shutdown()
        checks.append("restoring the original established catalog recovers the same retained project")
        report["passed"] = True
    except Exception as error:
        report["passed"] = False
        report["error"] = repr(error)
        raise
    finally:
        for fixture in fixtures:
            try:
                fixture.cleanup()
            except Exception as error:
                report.setdefault("cleanup_errors", []).append(repr(error))
        report["completed_at"] = datetime.now(timezone.utc).isoformat()
        finish_artifacts(run, report, options, stopped=not report.get("cleanup_errors"))
        options.report.write_text(json.dumps(report, indent=2)+"\n")
        print(json.dumps(report, indent=2), flush=True)


if __name__ == "__main__":
    main()
