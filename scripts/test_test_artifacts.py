"""Exercise retention and deletion boundaries using disposable native fixtures."""
import json
import os
from pathlib import Path
import stat
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from test_artifacts import MARKER, checked_tree, finish_artifacts, prune_runs, remove_run


class RetentionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.base = self.root / "runs"
        self.base.mkdir()
        self.options = SimpleNamespace(keep_artifacts=False, report=self.root / "summary.json")
        self.options.report.write_text('{"passed":true}')

    def tearDown(self):
        self.temp.cleanup()

    def run_dir(self, name="run", completed=None, size=8, keep=False):
        run = self.base / name
        run.mkdir()
        (run / "objects.bin").write_bytes(b"x" * size)
        if completed is not None:
            (run / MARKER).write_text(json.dumps({"version": 1, "stopped": True,
                                                "keep": keep, "completed_at": completed}))
        return run

    def test_success_removes_bulk_output_and_preserves_external_summary(self):
        run = self.run_dir()
        readonly = run / "readonly-object"
        readonly.write_bytes(b"data")
        readonly.chmod(stat.S_IREAD)
        report = {"passed": True}
        finish_artifacts(run, report, self.options, base=self.base)
        self.assertFalse(run.exists())
        self.assertTrue(self.options.report.is_file())
        self.assertFalse(report["artifacts"]["retained"])

    def test_failure_is_retained_then_expires(self):
        run = self.run_dir()
        with patch("test_artifacts.time.time", return_value=100):
            finish_artifacts(run, {"passed": False}, self.options, base=self.base)
        self.assertTrue(run.exists())
        prune_runs(self.base, now=201, max_age=100)
        self.assertFalse(run.exists())

    def test_budget_removes_oldest_first_including_linux_fixtures(self):
        old = self.run_dir("old", completed=90, size=512)
        linux = self.base / "linux-evidence"
        linux.mkdir()
        newest = linux / "mounted-new"
        newest.mkdir()
        (newest / "objects.bin").write_bytes(b"x" * 512)
        (newest / MARKER).write_text(json.dumps({"version": 1, "stopped": True,
                                               "keep": False, "completed_at": 95}))
        budget = checked_tree(newest, self.base)
        prune_runs(self.base, now=100, max_age=100, max_bytes=budget)
        self.assertFalse(old.exists())
        self.assertTrue(newest.exists())

    def test_unmarked_active_and_explicitly_kept_runs_survive(self):
        unknown = self.run_dir("unknown")
        active = self.run_dir("active", completed=1)
        data = json.loads((active / MARKER).read_text())
        data["stopped"] = False
        (active / MARKER).write_text(json.dumps(data))
        kept = self.run_dir("kept", completed=1, keep=True)
        prune_runs(self.base, now=100, max_age=1, max_bytes=0)
        self.assertTrue(all(path.exists() for path in (unknown, active, kept)))

    def test_unconfirmed_shutdown_never_creates_prunable_marker(self):
        run = self.run_dir()
        finish_artifacts(run, {"passed": True}, self.options, stopped=False, base=self.base)
        self.assertTrue(run.exists())
        self.assertFalse((run / MARKER).exists())

    def test_report_inside_run_and_keep_option_protect_output(self):
        run = self.run_dir()
        self.options.report = run / "report.json"
        self.options.report.write_text("{}")
        finish_artifacts(run, {"passed": True}, self.options, base=self.base)
        self.assertTrue(self.options.report.exists())
        second = self.run_dir("second")
        self.options.keep_artifacts = True
        finish_artifacts(second, {"passed": True}, self.options, base=self.base)
        prune_runs(self.base, now=10**12, max_bytes=0)
        self.assertTrue(second.exists())

    def test_preview_does_not_delete(self):
        run = self.run_dir(completed=1)
        results = prune_runs(self.base, now=100, max_age=1, apply=False)
        self.assertEqual(len(results), 1)
        self.assertTrue(run.exists())

    def test_rejects_parent_and_outside_paths(self):
        outside = self.root / "outside"
        outside.mkdir()
        for path in (self.base, outside):
            with self.assertRaises(ValueError):
                remove_run(path, self.base)
        self.assertTrue(outside.exists())

    def test_mount_is_preserved_before_any_file_is_deleted(self):
        run = self.run_dir()
        mount = run / "mount"
        mount.mkdir()
        real_ismount = os.path.ismount
        with patch("test_artifacts.os.path.ismount",
                   side_effect=lambda path: Path(path) == mount or real_ismount(path)):
            with self.assertRaises(ValueError):
                remove_run(run, self.base)
        self.assertTrue((run / "objects.bin").exists())

    def test_junction_or_symlink_never_deletes_external_target(self):
        run = self.run_dir()
        target = self.root / "external"
        target.mkdir()
        sentinel = target / "preserved.txt"
        sentinel.write_text("keep")
        link = run / "linked"
        if os.name == "nt":
            import _winapi
            _winapi.CreateJunction(str(target), str(link))
        else:
            link.symlink_to(target, target_is_directory=True)
        try:
            with self.assertRaises(ValueError):
                remove_run(run, self.base)
            self.assertEqual(sentinel.read_text(), "keep")
            self.assertTrue((run / "objects.bin").exists())
        finally:
            if os.name == "nt":
                link.rmdir()
            else:
                link.unlink()


if __name__ == "__main__":
    unittest.main()
