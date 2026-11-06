"""What the end-to-end harness makes of adb's output, on samples from an API 36 emulator.

Run from scripts/: python -m unittest test_android_e2e
"""
import base64
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import MagicMock, patch

import android_e2e
from android_e2e_lib import (Completed, Device, Emulator, GiveUp, HarnessError, ManageError, PcHost, Proc, Report,
                             Session, bounds_center, children_uri, document_uri, emulator_processes, encode_payload,
                             is_frozen, parse_call, parse_rows, poll, run_bounded, shell_line, split_qr,
                             watched_processes, with_parents)


class DocumentUris(unittest.TestCase):
    def test_a_hard_name_is_one_ascii_path_segment(self):
        self.assertEqual(document_uri("F:/a b%#:é.txt"),
                         "content://dev.tkfs.documents/document/F%3A%2Fa%20b%25%23%3A%C3%A9.txt")

    def test_children_follow_the_encoded_id(self):
        self.assertEqual(children_uri("F:/"), "content://dev.tkfs.documents/document/F%3A%2F/children")


class ShellQuoting(unittest.TestCase):
    def test_each_word_survives_a_remote_shell(self):
        words = ["content", "a b", "it's", "$HOME", "`id`", "%41", "{x}", ";&|", 'say "hi"']
        self.assertEqual(shlex.split(shell_line(words)), words)

    def test_a_payload_has_no_colon_for_content_call_to_refuse_and_round_trips(self):
        payload = {"label": """It's "é" $HOME \\ {x}""", "endpoint": "ws://10.0.2.2:1"}
        encoded = encode_payload(payload)
        self.assertTrue(encoded.isascii())
        self.assertNotIn(":", encoded)
        self.assertEqual(json.loads(base64.b64decode(encoded)), payload)


class ContentCall(unittest.TestCase):
    def test_a_result_is_its_value(self):
        self.assertEqual(parse_call('Result: Bundle[{result={"a":"}]","b":[1]}}]\n'), '{"a":"}]","b":[1]}')

    def test_an_error_raises_with_its_code(self):
        with self.assertRaisesRegex(ManageError, "^UNSUPPORTED_ACTION"):
            parse_call('Result: Bundle[{error=UNSUPPORTED_ACTION: "x"}]')

    def test_anything_else_is_a_harness_error_not_a_refusal(self):
        for output in ("Result: null", "Error while accessing provider:dev.tkfs.documents"):
            with self.assertRaises(HarnessError) as caught:
                parse_call(output)
            self.assertNotIsInstance(caught.exception, ManageError)


class ContentQuery(unittest.TestCase):
    def test_a_row_keeps_commas_equals_and_colons_in_values(self):
        output = ("Row: 0 document_id=F:/a=b, c:d.txt, _display_name=a=b, c:d.txt, _size=7, flags=0\n"
                  "Row: 1 document_id=F:/x, _display_name=x, _size=0, flags=0\n")
        rows = parse_rows(output)
        self.assertEqual(rows[0]["document_id"], "F:/a=b, c:d.txt")
        self.assertEqual(rows[0]["_display_name"], "a=b, c:d.txt")
        self.assertEqual(rows[0]["_size"], "7")
        self.assertEqual(rows[1]["document_id"], "F:/x")

    def test_no_result_is_no_rows(self):
        self.assertEqual(parse_rows("No result found."), [])


class Frozen(unittest.TestCase):
    DUMP = ("  *APP* UID 10216 ProcessRecord{9029783 1154:com.other/u0a216}\n"
            "    proc=ProcessRecord{860d2a 5647:dev.tkfs/u0a217}\n"
            "    isFreezeExempt=false isPendingFreeze=false isFrozen={other}\n"
            "  *APP* UID 10217 ProcessRecord{860d2a 5647:dev.tkfs/u0a217}\n"
            "    hasPendingCompaction=false    isFreezeExempt=false isPendingFreeze=false isFrozen={mine}\n")

    def dump(self, mine, other):
        return self.DUMP.replace("{mine}", mine).replace("{other}", other)

    def test_its_own_record_decides(self):
        self.assertTrue(is_frozen(self.dump("true", "false"), "dev.tkfs"))
        self.assertFalse(is_frozen(self.dump("false", "true"), "dev.tkfs"))

    def test_no_process_is_not_frozen(self):
        self.assertFalse(is_frozen(self.dump("true", "true"), "dev.tkfs.other"))


class PairingQr(unittest.TestCase):
    def test_the_reply_and_the_qr_lines_separate(self):
        reply, lines = split_qr('{\n  "code": "c",\n  "link": "l"\n}\n██▀▀\n█▄ █\n\n')
        self.assertEqual(reply, {"code": "c", "link": "l"})
        self.assertEqual(lines, ["██▀▀", "█▄ █"])


class Trees(unittest.TestCase):
    def test_every_directory_above_a_file_is_expected(self):
        self.assertEqual(with_parents(["a/b/c.txt", "d.txt"]), {"a", "a/b", "a/b/c.txt", "d.txt"})


class Screen(unittest.TestCase):
    def test_the_center_of_bounds(self):
        self.assertEqual(bounds_center("[0,100][1080,300]"), (540, 200))


class EmulatorProcesses(unittest.TestCase):
    RUNNING = [
        Proc(1, "emulator.exe", "emulator.exe -avd Phone -port 5554"),
        Proc(2, "qemu-system-x86_64-headless.exe", "qemu-system.exe -avd Phone -port 5554"),
        Proc(3, "emulator.exe", r"C:\sdk\emulator/emulator  -kill 2 -sleep 20 "),
        Proc(4, "emulator.exe", "emulator.exe -avd Other"),
        Proc(5, "emulator.exe", r"C:\sdk\emulator/emulator  -kill 77 -sleep 20 "),
        Proc(6, "tkfs.exe", "tkfs -avd Phone"),
    ]

    def found(self, known=frozenset()):
        with patch("android_e2e_lib.watched_processes", return_value=self.RUNNING):
            return [proc.pid for proc in emulator_processes("Phone", known)]

    def test_only_this_avds(self):
        self.assertEqual(self.found(), [1, 2])

    def test_the_watchdog_an_exiting_emulator_leaves_counts_when_it_names_ours(self):
        self.assertEqual(self.found({1, 2}), [1, 2, 3])


def answers(*replies):
    """A stand-in for a process query: its replies in turn, repeating the last. An exception
    among them is raised."""
    replies = list(replies)

    def answer(*_):
        reply = replies.pop(0) if len(replies) > 1 else replies[0]
        if isinstance(reply, Exception):
            raise reply
        return reply

    return answer


class ProcessQuery(unittest.TestCase):
    """A query that failed must not read as "no process left"."""

    @unittest.skipUnless(os.name == "nt", "the query is PowerShell's on Windows")
    def test_a_failed_windows_query_is_an_error_not_an_empty_list(self):
        with self.assertRaises(HarnessError):
            watched_processes("Win32_NoSuchClass")

    @unittest.skipUnless(os.name == "nt", "the query is PowerShell's on Windows")
    def test_a_real_query_gives_a_list(self):
        self.assertIsInstance(watched_processes(), list)

    def test_an_unreadable_listing_is_an_error(self):
        with (patch("android_e2e_lib.run_bounded", return_value=Completed([], 0, b"not a listing", b"")),
              self.assertRaisesRegex(HarnessError, "not a listing")):
            watched_processes()

    def test_a_failed_command_is_an_error(self):
        with (patch("android_e2e_lib.run_bounded", return_value=Completed(["query"], 1, b"[]", b"denied")),
              self.assertRaisesRegex(HarnessError, "denied")):
            watched_processes()


class StoppingThePc(unittest.TestCase):
    """A PC process that will not stop is a failure and is not left running, whatever else
    goes wrong on the way, and "none left" is only said of a query that saw the daemon."""

    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix="tkfs-e2e-test-")
        self.addCleanup(temp.cleanup)
        self.pc = PcHost(Path(sys.executable), Path(temp.name))
        self.pc.log.touch()

    def sleeper(self, script="import time; time.sleep(60)"):
        process = subprocess.Popen([sys.executable, "-c", script])
        self.addCleanup(process.wait)
        self.addCleanup(process.kill)
        return process

    def daemon(self, script="import time; time.sleep(60)") -> Proc:
        self.pc.process = self.sleeper(script)
        return Proc(self.pc.process.pid, "tkfs.exe", "tkfs start")

    def stop(self, processes, shutdown=None, **bounds):
        replies = {"return_value": {"stopped": True}} if shutdown is None else {"side_effect": shutdown}
        constants = {f"android_e2e_lib.{name}": bound for name, bound in bounds.items()}
        patches = [patch(name, bound) for name, bound in constants.items()]
        for active in patches:
            active.start()
            self.addCleanup(active.stop)
        with (patch.object(PcHost, "manage", **replies) as self.manage,
              patch("android_e2e_lib.tkfs_processes", side_effect=processes)):
            return self.pc.stop()

    def test_a_clean_stop_says_what_ran(self):
        daemon = self.daemon("pass")
        self.assertEqual(self.stop(answers([daemon, Proc(2, "tkfs.exe", "")], [])),
                         ([], "2 process(es) ran, none left"))

    def test_a_daemon_that_ignores_shutdown_is_killed_and_reported(self):
        daemon = self.daemon()
        process = self.pc.process
        failures, summary = self.stop(answers([daemon], []), PC_STOP_BOUND=0.2)
        self.assertEqual(len(failures), 1, failures)
        self.assertIn(f"STRANDED_PROCESS: daemon pid {daemon.pid}", failures[0])
        self.assertIsNotNone(process.poll())
        self.assertEqual(summary, "")

    def test_a_worker_that_outlives_its_daemon_is_killed_and_reported(self):
        daemon = self.daemon("pass")
        worker = self.sleeper()
        left = Proc(worker.pid, "tkfs.exe", "tkfs --state home")
        failures, _ = self.stop(answers([daemon, left], [left]), KILL_BOUND=0.2)
        self.assertEqual(len(failures), 1, failures)
        self.assertIn(f"STRANDED_PROCESS: tkfs.exe pid {worker.pid}", failures[0])
        self.assertIsNotNone(worker.wait(10))

    def test_a_failed_first_query_still_shuts_the_daemon_down(self):
        self.daemon("pass")
        failures, summary = self.stop(answers(HarnessError("no first query"), []))
        self.manage.assert_called_once()
        self.assertEqual(len(failures), 1, failures)
        self.assertIn("no first query", failures[0])
        self.assertEqual(summary, "")

    def test_a_bad_reply_to_shutdown_still_ends_the_daemon_at_once(self):
        daemon = self.daemon()
        process = self.pc.process
        started = time.monotonic()
        failures, _ = self.stop(answers([daemon], []), shutdown=ValueError("not JSON"))
        self.assertLess(time.monotonic() - started, 30)
        self.assertIn("not JSON", failures[0])
        self.assertIn(f"STRANDED_PROCESS: daemon pid {daemon.pid}", failures[1])
        self.assertIsNotNone(process.poll())

    def test_a_query_that_cannot_see_the_running_daemon_is_a_failure(self):
        self.pc.process = process = self.sleeper()
        failures, summary = self.stop(answers([], []), shutdown=lambda *_, **__: process.kill() or {})
        self.assertIn(f"did not show daemon pid {process.pid}", failures[0])
        self.assertEqual(summary, "")

    def test_a_failed_last_query_is_not_none_left(self):
        daemon = self.daemon("pass")
        failures, summary = self.stop(answers([daemon], HarnessError("no last query")), KILL_BOUND=0.2)
        self.assertEqual(len(failures), 1, failures)
        self.assertIn("no last query", failures[0])
        self.assertEqual(summary, "")

    def test_any_exception_from_the_last_query_is_a_failure(self):
        daemon = self.daemon("pass")
        failures, _ = self.stop(answers([daemon], ValueError("not JSON")), KILL_BOUND=0.2)
        self.assertEqual(len(failures), 1, failures)
        self.assertIn("not JSON", failures[0])


class ReachingTheProvider(unittest.TestCase):
    """Android's low-memory killer can take the cached app between two calls, and a call that
    races with that finds no provider. It reached nothing, so it is safe to repeat."""

    NOT_FOUND = Completed([], 0, b"", b"Error while accessing provider:dev.tkfs.documents\n"
                                      b"java.lang.IllegalStateException: Could not find provider: dev.tkfs.documents\n")
    REFUSED = Completed([], 0, b"", b"Error while accessing provider:dev.tkfs.documents\n"
                                    b"java.lang.SecurityException: Permission Denial\n")
    ANSWER = Completed([], 0, b'Result: Bundle[{result={"ok":1}}]\n', b"")
    ROWS = Completed([], 0, b"Row: 0 root_id=F, title=T\n", b"")

    def setUp(self):
        self.device = Device(Path("adb"), "emulator-5554")
        for name, value in (("android_e2e_lib.PROVIDER_RETRY", 0.01), ("builtins.print", MagicMock())):
            active = patch(name, value)
            active.start()
            self.addCleanup(active.stop)

    def test_a_manage_call_repeats_until_the_provider_is_back(self):
        with patch("android_e2e_lib.run_bounded", side_effect=[self.NOT_FOUND, self.NOT_FOUND, self.ANSWER]) as run:
            self.assertEqual(self.device.shell_manage("status"), {"ok": 1})
        self.assertEqual(run.call_count, 3)

    def test_a_query_repeats_too(self):
        with patch("android_e2e_lib.run_bounded", side_effect=[self.NOT_FOUND, self.ROWS]):
            self.assertEqual(self.device.content_query("content://x"), [{"root_id": "F", "title": "T"}])

    def test_a_provider_that_never_comes_back_fails_within_the_bound(self):
        with (patch("android_e2e_lib.PROVIDER_START_BOUND", 0.05),
              patch("android_e2e_lib.run_bounded", return_value=self.NOT_FOUND)):
            started = time.monotonic()
            with self.assertRaisesRegex(HarnessError, "Could not find provider"):
                self.device.shell_manage("status")
        self.assertLess(time.monotonic() - started, 5)

    def test_any_other_failure_is_not_repeated(self):
        with patch("android_e2e_lib.run_bounded", return_value=self.REFUSED) as run:
            with self.assertRaisesRegex(HarnessError, "Permission Denial"):
                self.device.shell_manage("status")
        self.assertEqual(run.call_count, 1)

    def test_a_process_that_dies_during_a_call_is_not_repeated(self):
        died = Completed([], 0, b"", b"Error while accessing provider:dev.tkfs.documents\n"
                                     b"android.os.DeadObjectException\n")
        with patch("android_e2e_lib.run_bounded", return_value=died) as run:
            with self.assertRaisesRegex(HarnessError, "DeadObjectException"):
                self.device.shell_manage("receive", {"label": "x"})
        self.assertEqual(run.call_count, 1)


class StoppingTheEmulator(unittest.TestCase):
    def setUp(self):
        self.emulator = Emulator(Path("adb"), Path("emulator"), "Phone", Path("."))
        self.emulator.device = Device(Path("adb"), "emulator-5554")
        self.done = Completed([], 0, b"", b"")

    def test_a_failed_first_query_still_asks_it_to_exit(self):
        with (patch("android_e2e_lib.emulator_processes", side_effect=answers(HarnessError("no query"), [])),
              patch("android_e2e_lib.run_bounded", return_value=self.done) as run):
            failures = self.emulator.stop()
        self.assertIn("emu", run.call_args.args[0])
        self.assertEqual(len(failures), 1, failures)
        self.assertIn("no query", failures[0])

    def test_a_failed_kill_command_still_checks_what_is_left(self):
        with (patch("android_e2e_lib.emulator_processes", side_effect=answers([Proc(1, "emulator", "")], [])),
              patch("android_e2e_lib.run_bounded", side_effect=HarnessError("adb hung"))):
            failures = self.emulator.stop()
        self.assertEqual(len(failures), 1, failures)
        self.assertIn("adb hung", failures[0])

    def test_what_is_left_is_killed_and_reported(self):
        left = Proc(7, "qemu.exe", "qemu -avd Phone")
        with (patch("android_e2e_lib.emulator_processes", return_value=[left]),
              patch("android_e2e_lib.EMULATOR_STOP_BOUND", 0.2),
              patch("android_e2e_lib.run_bounded", return_value=self.done),
              patch("android_e2e_lib.kill_pid") as kill):
            failures = self.emulator.stop()
        kill.assert_called_once_with(7)
        self.assertEqual(len(failures), 1, failures)
        self.assertIn("STRANDED_PROCESS: qemu.exe pid 7", failures[0])

    def test_a_clean_stop_has_no_failures(self):
        with (patch("android_e2e_lib.emulator_processes", side_effect=answers([Proc(1, "emulator", "")], [])),
              patch("android_e2e_lib.run_bounded", return_value=self.done)):
            self.assertEqual(self.emulator.stop(), [])


class RunningAScenario(unittest.TestCase):
    def run_one(self, stop):
        with tempfile.TemporaryDirectory(prefix="tkfs-e2e-test-") as temp:
            report = Report(Path(temp), {})
            with (patch.dict(android_e2e.SCENARIOS, {"x": lambda s: s.check("ran", True)}),
                  patch.object(PcHost, "start"), patch.object(Session, "check_pairing_qr"),
                  patch.object(PcHost, "stop", side_effect=stop), patch("builtins.print")):
                android_e2e.run_scenario("x", MagicMock(), report, Path(temp), reset=True)
            return report.data["scenarios"][0]

    def test_a_crash_while_stopping_the_pc_still_ends_the_scenario_as_failed(self):
        scenario = self.run_one(ValueError("not JSON"))
        self.assertEqual(scenario["status"], "fail")
        self.assertEqual(scenario["checks"][-1]["status"], "fail")
        self.assertIn("not JSON", scenario["checks"][-1]["detail"])

    def test_a_clean_run_passes(self):
        scenario = self.run_one([([], "1 process(es) ran, none left")])
        self.assertEqual(scenario["status"], "pass")


class CommandLine(unittest.TestCase):
    def test_a_known_scenario_and_all_are_accepted(self):
        self.assertEqual(android_e2e.parse_args(["--scenario", "read"]).scenario, "read")
        self.assertEqual(android_e2e.parse_args([]).scenario, "all")

    def test_an_unknown_scenario_is_refused(self):
        with patch("sys.stderr"), self.assertRaises(SystemExit) as caught:
            android_e2e.parse_args(["--scenario", "no-such"])
        self.assertEqual(caught.exception.code, 2)

    def test_release_pc_is_off_until_asked_for(self):
        self.assertFalse(android_e2e.parse_args([]).release_pc)
        self.assertTrue(android_e2e.parse_args(["--release-pc"]).release_pc)


class WhichTkfs(unittest.TestCase):
    def test_the_speed_scenario_always_measures_a_release_pc(self):
        self.assertTrue(android_e2e.uses_release_pc("speed", False))
        self.assertFalse(android_e2e.uses_release_pc("read", False))
        self.assertTrue(android_e2e.uses_release_pc("read", True))

    def test_each_profile_has_its_own_binary(self):
        self.assertEqual(android_e2e.tkfs_binary().parent.name, "debug")
        self.assertEqual(android_e2e.tkfs_binary(True).parent.name, "release")

    def run_build(self, names, release_pc):
        ran = []

        def recording(argv, *_, **__):
            ran.append([str(word) for word in argv])
            return MagicMock()

        with patch.object(android_e2e, "run_bounded", recording):
            android_e2e.build(Path("logs"), names, release_pc)
        return [" ".join(argv) for argv in ran if argv[0] == "cargo"]

    def test_a_build_makes_only_the_tkfs_its_scenarios_use(self):
        debug = "cargo build --locked --bin tkfs"
        release = "cargo build --locked --release --bin tkfs"
        self.assertEqual(self.run_build(["read"], False), [debug])
        self.assertEqual(self.run_build(["speed"], False), [release])
        self.assertEqual(self.run_build(["read"], True), [release])
        self.assertEqual(self.run_build(["read", "speed"], False), [debug, release])


class Bounds(unittest.TestCase):
    def test_a_poll_gives_up_at_its_bound_with_the_last_reason(self):
        def refused():
            raise HarnessError("still no")

        with self.assertRaisesRegex(HarnessError, "still no"):
            poll(0.05, "something", refused, interval=0.01)

    def test_a_poll_stops_at_once_on_give_up(self):
        def hopeless():
            raise GiveUp("no point")

        started = time.monotonic()
        with self.assertRaisesRegex(GiveUp, "no point"):
            poll(30, "something", hopeless)
        self.assertLess(time.monotonic() - started, 5)

    def test_a_command_past_its_bound_is_an_error_and_is_gone(self):
        with self.assertRaisesRegex(HarnessError, "did not finish within"):
            run_bounded([sys.executable, "-c", "import time; time.sleep(60)"], 0.5)

    def test_a_command_keeps_both_outputs_apart_and_binary(self):
        code = "import os; os.write(1, b'\\x00\\r\\n\\xff'); os.write(2, b'oops')"
        done = run_bounded([sys.executable, "-c", code], 30)
        self.assertEqual((done.code, done.stdout, done.stderr), (0, b"\x00\r\n\xff", b"oops"))


class ReadingTheScreen(unittest.TestCase):
    def device(self):
        return Device(Path("adb"), "emulator-5554")

    def test_wait_screen_returns_the_texts_once_wanted_and_names_them_when_not(self):
        device = self.device()
        with patch.object(Device, "screen_texts", side_effect=[["Loading"], ["Documents", "Synced"]]):
            self.assertEqual(device.wait_screen("a folder", lambda texts: "Synced" in texts, bound=5),
                             ["Documents", "Synced"])
        with patch.object(Device, "screen_texts", return_value=["Loading"]):
            with self.assertRaises(HarnessError) as raised:
                device.wait_screen("a folder", lambda texts: "Synced" in texts, bound=0)
        self.assertIn("Loading", str(raised.exception))

    def test_a_screenshot_that_is_not_a_png_is_refused_and_a_png_is_saved(self):
        device = self.device()
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "shots" / "a.png"
            done = lambda data: Completed([], 0, data, b"")
            with patch.object(Device, "adb", return_value=done(b"not a picture")):
                with self.assertRaises(HarnessError):
                    device.screenshot(path)
            self.assertFalse(path.exists())
            with patch.object(Device, "adb", return_value=done(b"\x89PNG\r\n\x1a\nrest")):
                device.screenshot(path)
            self.assertEqual(path.read_bytes(), b"\x89PNG\r\n\x1a\nrest")


if __name__ == "__main__":
    unittest.main()
