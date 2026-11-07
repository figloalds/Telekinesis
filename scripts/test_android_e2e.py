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
from unittest.mock import MagicMock, call, patch

import android_e2e
from android_e2e_lib import (Completed, ContentError, Device, Emulator, GiveUp, HarnessError, ManageError, PcHost, Proc, Report,
                             Session, bounds_center, children_uri, crashes_in, document_uri, emulator_processes, encode_payload,
                             is_frozen, parse_call, parse_df_available, parse_rows, png_noise, poll, run_bounded, shell_line, size_text, split_qr,
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


class FillingTheVolume(unittest.TestCase):
    def device(self, *free: int) -> Device:
        device = Device(Path("adb"), "serial")
        device.free_bytes = MagicMock(side_effect=list(free))
        device.shell = MagicMock()
        return device

    def test_df_says_what_an_app_can_use(self):
        output = "Filesystem 1K-blocks Used Available Use% Mounted on\n/dev/block/dm-55 6082144 5939940 7 100% /data/user/0"
        self.assertEqual(parse_df_available(output), 7 * 1024)

    def test_nothing_is_written_at_or_below_what_is_wanted(self):
        device = self.device(100)
        self.assertEqual(device.fill_until_free(100), 100)
        device.shell.assert_not_called()

    def test_it_writes_what_takes_it_past_what_is_wanted(self):
        mib = 1024 * 1024
        device = self.device(10 * mib, mib // 2)
        self.assertEqual(device.fill_until_free(mib), mib // 2)
        words = device.shell.call_args_list[0].args
        self.assertEqual(words[:1], ("dd",))
        self.assertIn("count=10", words)
        self.assertTrue(any(word.startswith("of=/sdcard/Download/tkfs_e2e_filler-") for word in words), words)

    def test_it_gives_up_with_how_far_it_got(self):
        device = self.device(*[10] * 3)
        with patch("android_e2e_lib.time.monotonic", side_effect=[0, 0, 1000]):
            with self.assertRaisesRegex(HarnessError, "10 bytes are free"):
                device.fill_until_free(1, bound=5)


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


# What an API 36 emulator logs for each, at threadtime.
OWN_EXCEPTION = """11-06 23:53:49.792  6752  6752 E AndroidRuntime: FATAL EXCEPTION: main
11-06 23:53:49.792  6752  6752 E AndroidRuntime: Process: dev.tkfs, PID: 6752
11-06 23:53:49.792  6752  6752 E AndroidRuntime: android.app.RemoteServiceException: did not then call Service.startForeground()
11-06 23:53:49.792  6752  6752 E AndroidRuntime: 	at android.app.ActivityThread.generateForegroundServiceDidNotStartInTimeException(ActivityThread.java:2391)
"""
OTHER_EXCEPTION = """11-06 23:53:49.792  6752  6752 E AndroidRuntime: FATAL EXCEPTION: main
11-06 23:53:49.792  6752  6752 E AndroidRuntime: Process: com.android.other, PID: 6752
11-06 23:53:49.792  6752  6752 E AndroidRuntime: java.lang.IllegalStateException: not ours
"""
OWN_SIGNAL = "11-06 23:53:49.792  6752  6790 F libc    : Fatal signal 11 (SIGSEGV), code 1 (SEGV_MAPERR), fault addr 0x0 in tid 6790 (tkfs-sealer), pid 6752 (dev.tkfs)\n"
OTHER_SIGNAL = "11-06 23:53:49.792  6752  6790 F libc    : Fatal signal 6 (SIGABRT), code -1 (SI_QUEUE) in tid 6790 (x), pid 6752 (dev.tkfs.other)\n"
QUIET = "11-06 23:53:49.792  6752  6752 I tkfs    : transfer: foreground\n"


class FindingCrashes(unittest.TestCase):
    def test_an_uncaught_exception_of_the_app_is_one(self):
        found = crashes_in(QUIET + OWN_EXCEPTION + QUIET)
        self.assertEqual(len(found), 1)
        self.assertIn("did not then call Service.startForeground()", found[0])

    def test_a_fatal_signal_of_the_app_is_one(self):
        self.assertEqual(len(crashes_in(QUIET + OWN_SIGNAL)), 1)

    def test_those_of_other_processes_are_not(self):
        self.assertEqual(crashes_in(OTHER_EXCEPTION + OTHER_SIGNAL + QUIET), [])

    def test_each_counts(self):
        self.assertEqual(len(crashes_in(OWN_EXCEPTION + OWN_SIGNAL + OWN_EXCEPTION)), 3)

    def test_a_dump_that_ends_on_the_first_line_still_says_so(self):
        self.assertEqual(len(crashes_in(OWN_EXCEPTION.splitlines()[0])), 0, "it does not say whose it is")
        self.assertEqual(len(crashes_in("\n".join(OWN_EXCEPTION.splitlines()[:2]))), 1)


class KeepingLogcat(unittest.TestCase):
    def device(self, *dumps):
        device = Device(Path("adb"), "emulator-5554")
        answers = iter(dumps)

        def adb(*args, **_):
            return Completed([], 0, next(answers).encode() if "-d" in args else b"", b"")

        device.adb = adb
        return device

    def test_what_a_scenario_clears_still_counts(self):
        device = self.device(OWN_EXCEPTION, QUIET)
        device.start_logcat()
        device.clear_logcat()
        with tempfile.TemporaryDirectory(prefix="tkfs-e2e-test-") as temp:
            log = Path(temp) / "logcat.txt"
            logged = device.append_logcat(log, "x")
            self.assertEqual(len(crashes_in(logged)), 1)
            self.assertIn("FATAL EXCEPTION", log.read_text())

    def test_what_came_before_a_scenario_does_not(self):
        device = self.device(OWN_EXCEPTION, QUIET)
        device.clear_logcat()
        device.start_logcat()
        with tempfile.TemporaryDirectory(prefix="tkfs-e2e-test-") as temp:
            self.assertEqual(crashes_in(device.append_logcat(Path(temp) / "logcat.txt", "x")), [])


class RetryingTheProvider(unittest.TestCase):
    def test_a_retry_is_on_record_and_taken_once(self):
        device = Device(Path("adb"), "emulator-5554")
        answers = iter([Completed([], 0, b"", b"Could not find provider"), Completed([], 0, b"Result: done", b"")])
        device.adb = lambda *_, **__: next(answers)
        with patch("android_e2e_lib.PROVIDER_RETRY", 0), patch("builtins.print"):
            self.assertEqual(device.call_method("tkfs.fault"), "Result: done")
        taken = device.take_retries()
        self.assertEqual(len(taken), 1)
        self.assertIn("tkfs.fault", taken[0])
        self.assertEqual(device.take_retries(), [])

    def test_no_retry_is_none_on_record(self):
        device = Device(Path("adb"), "emulator-5554")
        device.adb = lambda *_, **__: Completed([], 0, b"Result: done", b"")
        device.call_method("tkfs.fault")
        self.assertEqual(device.take_retries(), [])


class RunningAScenario(unittest.TestCase):
    def run_one(self, stop=None, logcat="", retries=(), provoked=None):
        stop = [([], "1 process(es) ran, none left")] if stop is None else stop
        device = MagicMock()
        device.append_logcat.return_value = logcat
        device.take_retries.return_value = list(retries)
        with tempfile.TemporaryDirectory(prefix="tkfs-e2e-test-") as temp:
            report = Report(Path(temp), {})
            with (patch.dict(android_e2e.SCENARIOS, {"x": lambda s: s.check("ran", True)}),
                  patch.dict(android_e2e.PROVOKED_CRASHES, provoked or {}),
                  patch.object(PcHost, "start"), patch.object(Session, "check_pairing_qr"),
                  patch.object(PcHost, "stop", side_effect=stop), patch("builtins.print")):
                android_e2e.run_scenario("x", device, report, Path(temp), reset=True)
            return report.data["scenarios"][0]

    def test_a_crash_in_its_logcat_fails_it(self):
        scenario = self.run_one(logcat=QUIET + OWN_EXCEPTION)
        self.assertEqual(scenario["status"], "fail")
        crash = next(check for check in scenario["checks"] if check["name"] == "the app did not crash")
        self.assertEqual(crash["status"], "fail")
        self.assertIn("startForeground", crash["detail"])

    def test_a_crash_of_another_process_does_not(self):
        self.assertEqual(self.run_one(logcat=OTHER_EXCEPTION)["status"], "pass")

    def test_a_crash_it_provokes_itself_does_not(self):
        scenario = self.run_one(logcat=OWN_SIGNAL, provoked={"x": ["SIGSEGV"]})
        self.assertEqual(scenario["status"], "pass")
        self.assertEqual(self.run_one(logcat=OWN_EXCEPTION, provoked={"x": ["SIGSEGV"]})["status"], "fail")

    def test_its_retries_are_in_the_report(self):
        self.assertEqual(self.run_one(retries=["content call"])["retries"], ["content call"])
        self.assertEqual(self.run_one()["retries"], [])

    def test_a_crash_while_stopping_the_pc_still_ends_the_scenario_as_failed(self):
        scenario = self.run_one(ValueError("not JSON"))
        self.assertEqual(scenario["status"], "fail")
        self.assertEqual(scenario["checks"][-1]["status"], "fail")
        self.assertIn("not JSON", scenario["checks"][-1]["detail"])

    def test_a_clean_run_passes(self):
        scenario = self.run_one()
        self.assertEqual(scenario["status"], "pass")


class TheSpeedScenario(unittest.TestCase):
    def test_a_reverse_that_will_not_go_does_not_hide_what_ended_it(self):
        session = MagicMock()
        session.device.remove_reverse.side_effect = HarnessError("adb: no such reverse")
        ended = HarnessError("the first read failed")
        with (patch.object(android_e2e, "first_read_rate", side_effect=[(1.0, "normal"), ended]),
              patch.object(android_e2e, "poll"), patch.object(android_e2e.random, "Random")):
            with self.assertRaises(HarnessError) as raised:
                android_e2e.scenario_speed(session)
        self.assertIs(raised.exception, ended)
        session.report.record.assert_called_once_with("the adb reverse is removed", False, "adb: no such reverse")


class ReadingOnAFullDisk(unittest.TestCase):
    def refused(self, digest=None, error=None):
        device = MagicMock()
        device.content_digest.return_value = digest
        device.content_digest.side_effect = error
        return android_e2e.read_refused(device, "doc")

    def test_only_a_read_that_fails_is_a_refusal(self):
        self.assertIn("no room", self.refused(error=ContentError("no room")))

    def test_a_read_that_gives_bytes_is_none_whichever_bytes_they_are(self):
        self.assertEqual(self.refused(digest="the right ones"), "")
        self.assertEqual(self.refused(digest="other ones"), "")


class StartingASession(unittest.TestCase):
    def test_a_filler_left_by_a_hard_kill_goes_before_anything_needs_the_room(self):
        device = MagicMock()
        android_e2e.prepare_device(device, Path("app.apk"))
        self.assertEqual(device.method_calls, [call.remove_filler(), call.install(Path("app.apk"))])


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


class AdbReverse(unittest.TestCase):
    def setUp(self):
        self.device = Device(Path("adb"), "emulator-5554")

    def test_the_port_is_the_one_adb_picked(self):
        with patch.object(Device, "adb", return_value=Completed([], 0, b"43211\r\n", b"")) as adb:
            self.assertEqual(self.device.reverse(5000), 43211)
        adb.assert_called_once_with("reverse", "tcp:0", "tcp:5000")

    def test_anything_but_a_port_is_an_error(self):
        with patch.object(Device, "adb", return_value=Completed([], 0, b"error: more than one device", b"")):
            with self.assertRaisesRegex(HarnessError, "more than one device"):
                self.device.reverse(5000)

    def test_a_refusal_is_an_error(self):
        with patch("android_e2e_lib.run_bounded", return_value=Completed(["adb"], 1, b"", b"closed")):
            with self.assertRaisesRegex(HarnessError, "closed"):
                self.device.reverse(5000)
            with self.assertRaisesRegex(HarnessError, "closed"):
                self.device.remove_reverse(43211)


class WhichScenarios(unittest.TestCase):
    def test_all_runs_every_scenario(self):
        chosen = android_e2e.chosen_scenarios("all")
        self.assertEqual(chosen, list(android_e2e.SCENARIOS))
        self.assertIn("speed", chosen)
        self.assertIn("read", chosen)

    def test_speed_runs_by_name(self):
        self.assertEqual(android_e2e.parse_args(["--scenario", "speed"]).scenario, "speed")
        self.assertEqual(android_e2e.chosen_scenarios("speed"), ["speed"])


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


class DocumentOperations(unittest.TestCase):
    def device(self):
        return Device(Path("adb"), "emulator-5554")

    def test_a_release_counts_for_its_own_document_only(self):
        log = "open F:/a w\nreleased F:/a\nreleased F:/a b\nF:/a: sync: INVALID_HANDLE\nreleased F:/a\n"
        device = self.device()
        with patch.object(Device, "adb", return_value=Completed([], 0, log.encode(), b"")):
            self.assertEqual(device.releases("F:/a"), 2)
            self.assertEqual(device.releases("F:/a b"), 1)
            self.assertEqual(device.releases("F:/"), 0)

    def test_an_operation_sends_its_request_as_base64_json_and_returns_the_reply(self):
        device = self.device()
        sent = []

        def provider(line, bound):
            sent.append(shlex.split(line))
            return Completed([], 0, b'Result: Bundle[{result={"id":"F:/b"}}]', b"")

        with patch.object(Device, "_provider", side_effect=provider):
            self.assertEqual(device.rename_document("F:/a", "b:c"), "F:/b")
        words = sent[0]
        self.assertEqual(words[words.index("--method") + 1], "tkfs.document")
        self.assertEqual(words[words.index("--arg") + 1], "rename")
        encoded = words[words.index("--extra") + 1].removeprefix("payload:s:")
        self.assertEqual(json.loads(base64.b64decode(encoded)), {"id": "F:/a", "name": "b:c"})

    def test_a_write_says_where_and_how_and_carries_its_bytes_as_base64(self):
        device = self.device()
        with patch.object(Device, "document", return_value={"written": 3}) as document:
            self.assertEqual(device.write_document("F:/a", "rw", b"\x00\xff\n", offset=7, close=False), 3)
        document.assert_called_once_with("write", id="F:/a", mode="rw", offset=7, fsync=False, close=False, data="AP8K")

    def test_a_refusal_raises_with_its_code(self):
        reply = Completed([], 0, b"Result: Bundle[{error=INVALID_NAME: \"a/b\"}]", b"")
        with patch.object(Device, "_provider", return_value=reply):
            with self.assertRaisesRegex(ManageError, "^INVALID_NAME"):
                self.device().create_document("F:/", "a/b")


class SharingIn(unittest.TestCase):
    def device(self):
        return Device(Path("adb"), "emulator-1")

    def test_a_share_carries_its_grant_on_the_intents_data(self):
        device = self.device()
        with patch.object(Device, "shell", return_value=Completed([], 0, b"", b"")) as shell:
            device.start_share("content://media/external/file/7")
        words = shell.call_args.args
        self.assertEqual(words[words.index("-d") + 1], "content://media/external/file/7")
        self.assertEqual(words[words.index("--eu") + 2], "content://media/external/file/7")
        self.assertIn("--grant-read-uri-permission", words)

    def test_the_exemption_goes_both_ways(self):
        device = self.device()
        with patch.object(Device, "shell", return_value=Completed([], 0, b"", b"")) as shell:
            device.battery_exempt(True)
            device.battery_exempt(False)
        self.assertEqual([call.args[-1] for call in shell.call_args_list], ["+dev.tkfs", "-dev.tkfs"])

    def test_the_transfer_log_is_the_jobs_and_the_shares_lines_only(self):
        log = "transfer: foreground\nopen F:/a w\nshare: dataSync timed out\nreleased F:/a\n"
        with patch.object(Device, "adb", return_value=Completed([], 0, log.encode(), b"")):
            self.assertEqual(self.device().transfer_log(), ["transfer: foreground", "share: dataSync timed out"])

    def test_the_raw_names_include_the_hidden_working_files(self):
        reply = Completed([], 0, b'Result: Bundle[{result=[{"name":"a.txt"},{"name":".tkfs-part-1"}]}]', b"")
        with patch.object(Device, "_provider", return_value=reply):
            self.assertEqual(self.device().raw_names("F", "/"), ["a.txt", ".tkfs-part-1"])

    def test_a_content_summary_of_a_pc_entry_is_its_content_or_none(self):
        pc = PcHost(Path("tkfs"), Path("home"))
        pc._tkfs = MagicMock(return_value=Completed([], 0, b'{"entry":{"content":{"digest":"x","total_len":3}}}', b""))
        self.assertEqual(pc.pc_content("state", "a"), '{"digest": "x", "total_len": 3}')
        pc._tkfs = MagicMock(return_value=Completed([], 0, b'{"entry":{"content":null}}', b""))
        self.assertIsNone(pc.pc_content("state", "a"))
        pc._tkfs = MagicMock(return_value=Completed([], 1, b"", b"NOT_FOUND"))
        self.assertIsNone(pc.pc_content("state", "a"))


class OnThePc(unittest.TestCase):
    def pc(self, code, err):
        pc = PcHost(Path("tkfs"), Path("home"))
        pc._tkfs = MagicMock(return_value=Completed(["tkfs"], code, b"", err))
        return pc

    def test_a_path_that_is_there_or_not(self):
        self.assertTrue(self.pc(0, b"").pc_has("state", "a"))
        self.assertFalse(self.pc(1, b"Error: NOT_FOUND").pc_has("state", "a"))

    def test_any_other_failure_says_nothing_of_the_path(self):
        with self.assertRaisesRegex(HarnessError, "WORKER_NOT_RUNNING"):
            self.pc(1, b"Error: WORKER_NOT_RUNNING").pc_has("state", "a")


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


class OlderPc(unittest.TestCase):
    """A PC that predates `content-have` is made by a fault point in its environment, for one start."""

    def pc(self):
        return PcHost(Path(sys.executable), Path(tempfile.gettempdir()) / "tkfs-e2e-env-test")

    def test_a_start_gets_the_fault_it_was_given_and_the_next_does_not(self):
        pc = self.pc()
        pc.extra_env = {"TKFS_FAULT_POINT": "session.features.none"}
        self.assertEqual(pc._env()["TKFS_FAULT_POINT"], "session.features.none")
        pc.extra_env = {}
        self.assertNotEqual(pc._env().get("TKFS_FAULT_POINT"), "session.features.none")

    def test_the_metered_answer_is_the_apps_own(self):
        device = Device(Path("adb"), "emulator-5554")
        with patch.object(Device, "call_method", return_value='Result: Bundle[{result={"metered":true}}]'):
            self.assertTrue(device.app_sees_metered())
        with patch.object(Device, "call_method", return_value='Result: Bundle[{result={"metered":false}}]'):
            self.assertFalse(device.app_sees_metered())


if __name__ == "__main__":
    unittest.main()


class BrowseHelpers(unittest.TestCase):
    def test_sizes_read_as_the_app_words_them(self):
        # The same cases as the app's FileRowTest: a row's line is checked against these.
        cases = {999: "999 B", 1_000: "1.0 kB", 3_400_000: "3.4 MB", 812_000_000: "812 MB",
                 1_500_000_000: "1.5 GB", 12_300_000_000: "12 GB", 0: "0 B", 999_999: "1.0 MB"}
        for size, text in cases.items():
            self.assertEqual(size_text(size), text, size)

    def test_a_noise_png_is_a_png_that_does_not_shrink(self):
        import struct
        import zlib
        data = png_noise(3, 40)
        self.assertEqual(data[:8], b"\x89PNG\r\n\x1a\n")
        width, height = struct.unpack(">II", data[16:24])
        self.assertEqual((width, height), (40, 40))
        self.assertEqual(data, png_noise(3, 40))
        self.assertNotEqual(data, png_noise(4, 40))
        self.assertGreater(len(data), 40 * 40 * 3)
        idat = data.index(b"IDAT")
        length = struct.unpack(">I", data[idat - 4:idat])[0]
        self.assertEqual(len(zlib.decompress(data[idat + 4:idat + 4 + length])), 40 * (1 + 40 * 3))

    def test_the_browse_scenario_is_listed(self):
        self.assertIn("ui_browse", android_e2e.SCENARIOS)
