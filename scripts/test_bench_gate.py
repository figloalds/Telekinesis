"""Host-only checks of the benchmark gate; never mounts, builds or runs a workload."""
import contextlib
import io
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

import bench

RUNS = 5
EVIDENCE = bench.ROOT / "test-evidence/wave-6/memfs-floor-amortized-sandbox-20261026"


def report(label, medians, policy="native", fscache="true", hostname="guest-a",
           git="git version 2.56.0.windows.3", **overrides):
    args = {"runs": RUNS, "copy_files": 2000, "large_mib": 1024, "random_ops": 10_000,
            "git_files": 50_000, "projects": 8, "files": 128, "jobs": 4, "framework": "net10.0",
            "only": "copy_small,seq_1g,random_4k,git_status_50k", "peer_root": None,
            "cache_policy": policy, "git_fscache": fscache}
    args.update(overrides)
    return {"label": label, "args": args, "host": {"hostname": hostname, "git": git},
            "git_fixture_mode": bench.GIT_FIXTURE_MODE, "skipped": {},
            "summary": {key: {"median_ms": value, "min_ms": value, "max_ms": value, "samples": RUNS}
                        for key, value in medians.items()}}


NATIVE = {"copy_small": 600.0, "seq_write_1g": 500.0, "seq_read_1g": 550.0,
          "random_4k": 45.0, "git_status_50k": 190.0}
REFERENCE = {"copy_small": 1800.0, "seq_write_1g": 2000.0, "seq_read_1g": 600.0,
             "random_4k": 50.0, "git_status_50k": 300.0}
PASSING = {"copy_small": 2300.0, "seq_write_1g": 2390.0, "seq_read_1g": 650.0,
           "random_4k": 60.0, "git_status_50k": 380.0}


class GateTests(unittest.TestCase):
    def setUp(self):
        self.native = report("native", NATIVE)
        self.reference = report("memfs-purge", REFERENCE, policy="purge")

    def gate(self, medians=PASSING, reference="default", **overrides):
        candidate = report("mounted", medians, policy="purge", **overrides)
        return bench.gate(candidate, self.native,
                          self.reference if reference == "default" else reference)

    def test_reference_bound_metrics_pass_against_the_reference_not_native(self):
        # 2,300 ms is 3.8x native copy but 1.28x the reference.
        self.assertEqual(self.gate(), [])

    def test_each_reference_bound_metric_fails_above_its_reference_target(self):
        failures = self.gate({**PASSING, "copy_small": 2400.0, "seq_write_1g": 2500.0,
                              "git_status_50k": 400.0})
        self.assertEqual(sorted(failures), ["copy_small: exceeds 1.3x reference",
                                            "git_status_50k: exceeds 1.3x reference",
                                            "seq_write_1g: exceeds 1.2x reference"])

    def test_native_bound_metrics_keep_native_targets(self):
        failures = self.gate({**PASSING, "seq_read_1g": 700.0, "random_4k": 70.0})
        self.assertEqual(sorted(failures), ["random_4k: exceeds 1.5x native",
                                            "seq_read_1g: exceeds 1.2x native"])

    def test_a_run_without_reference_cannot_pass_reference_bound_metrics(self):
        failures = self.gate(reference=None)
        self.assertEqual(sorted(failures), ["copy_small: no memory-filesystem reference",
                                            "git_status_50k: no memory-filesystem reference",
                                            "seq_write_1g: no memory-filesystem reference"])

    def test_reference_must_share_the_candidate_cache_policy(self):
        self.reference["args"]["cache_policy"] = "standby"
        self.assertIn("incompatible reference: cache_policy", self.gate())
        del self.reference["args"]["cache_policy"]
        self.assertIn("incompatible reference: cache_policy", self.gate())

    def test_a_native_reference_is_not_a_memory_filesystem_reference(self):
        self.reference["args"]["cache_policy"] = "native"
        failures = bench.gate(report("mounted", PASSING, policy="native"), self.native, self.reference)
        self.assertIn("incompatible reference: cache_policy", failures)

    def test_git_client_cache_setting_must_match_baseline_and_reference(self):
        del self.native["args"]["git_fscache"]
        self.reference["args"]["git_fscache"] = "false"
        failures = self.gate()
        self.assertIn("incompatible baseline: git_fscache", failures)
        self.assertIn("incompatible reference: git_fscache", failures)

    def test_reference_must_come_from_the_same_guest_and_git(self):
        self.reference["host"] = {"hostname": "guest-b", "git": "git version 2.55.0.windows.1"}
        failures = self.gate()
        self.assertIn("incompatible reference: host hostname", failures)
        self.assertIn("incompatible reference: host git", failures)

    def test_reference_workload_sizes_must_match(self):
        self.reference["args"]["large_mib"] = 512
        self.assertIn("incompatible reference: large_mib", self.gate())

    def test_missing_reference_measurement_is_not_a_performance_failure(self):
        del self.reference["summary"]["seq_write_1g"]
        failures = self.gate()
        self.assertIn("seq_write_1g: missing reference measurement", failures)
        self.assertFalse(bench.performance_only_failure({"failures": failures, "skipped": {}}))

    def test_target_misses_alone_remain_performance_only(self):
        failures = self.gate({**PASSING, "copy_small": 2400.0, "random_4k": 70.0})
        self.assertTrue(bench.performance_only_failure({"failures": failures, "skipped": {}}))

    def test_runs_that_measure_no_reference_bound_metric_ignore_the_reference(self):
        medians = {"seq_read_1g": 650.0, "random_4k": 60.0}
        candidate = report("mounted", medians, policy="purge", only="random_4k")
        candidate["summary"]["seq_read_1g"]["samples"] = RUNS
        self.reference["args"]["cache_policy"] = "standby"
        self.assertEqual(bench.gate(candidate, self.native, self.reference), [])

    def test_table_prints_native_and_reference_ratios(self):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            bench.print_table(report("mounted", PASSING)["summary"], self.native, self.reference)
        lines = {line.split()[0]: line for line in out.getvalue().splitlines()[1:]}
        self.assertIn("3.83x", lines["copy_small"])
        self.assertIn("1.28x", lines["copy_small"])
        self.assertTrue(lines["copy_small"].endswith("<=1.3x reference ok"))
        self.assertTrue(lines["random_4k"].endswith("<=1.5x native ok"))

    def test_table_marks_reference_bound_metrics_without_a_reference(self):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            bench.print_table(report("mounted", PASSING)["summary"], self.native, None)
        self.assertIn("<=1.2x reference no reference", out.getvalue())


class GitAndCommandLineTests(unittest.TestCase):
    def test_git_status_passes_the_configured_client_cache(self):
        calls = []
        with patch.object(bench.bench_build, "command",
                          side_effect=lambda command, cwd: calls.append(command) or (1.0, "")):
            for value in ("true", "false"):
                bench.git_status_50k(Path("repo"), type("Args", (), {"git_fscache": value}), 1)
        self.assertIn(f"core.fscache=true", calls[0])
        self.assertIn(f"core.fscache=false", calls[1])

    def test_matrix_refuses_to_start_without_a_reference(self):
        with patch.object(sys, "argv", ["bench.py", "--label", "x", "--matrix-tkfs", "tkfs.exe"]), \
                patch.object(bench, "matrix", side_effect=AssertionError("matrix started")), \
                contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as raised:
                bench.main()
        self.assertEqual(raised.exception.code, 2)

    def test_matrix_without_reference_bound_workloads_needs_no_reference(self):
        with patch.object(sys, "argv", ["bench.py", "--label", "x", "--matrix-tkfs", "tkfs.exe",
                                        "--only", "random_4k"]), \
                patch.object(bench, "matrix", return_value=0) as matrix:
            self.assertEqual(bench.main(), 0)
        self.assertEqual(matrix.call_args.args[0].matrix_cache_policy, "purge")


@unittest.skipUnless((EVIDENCE / "memfs-purge-report.json").is_file(), "retained section 9 evidence absent")
class RetainedEvidenceTests(unittest.TestCase):
    def test_reports_from_before_the_decision_cannot_serve_as_a_reference(self):
        native = json.loads((EVIDENCE / "native-before-report.json").read_text(encoding="utf-8"))
        reference = json.loads((EVIDENCE / "memfs-purge-report.json").read_text(encoding="utf-8"))
        candidate = json.loads(json.dumps(reference))
        candidate["args"].update(cache_policy="purge", git_fscache="true")
        failures = bench.gate(candidate, native, reference)
        self.assertIn("incompatible reference: cache_policy", failures)
        self.assertIn("incompatible reference: git_fscache", failures)
        self.assertIn("incompatible baseline: git_fscache", failures)


if __name__ == "__main__":
    unittest.main()
