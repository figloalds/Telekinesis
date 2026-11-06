"""What the end-to-end harness makes of adb's output, on samples from an API 36 emulator.

Run from scripts/: python -m unittest test_android_e2e
"""
import base64
import json
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from android_e2e import (GiveUp, HarnessError, ManageError, PcHost, Proc, bounds_center, children_uri, document_uri,
                         emulator_processes, encode_payload, is_frozen, parse_call, parse_rows, poll, run_bounded,
                         shell_line, split_qr, with_parents)


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
        with patch("android_e2e.watched_processes", return_value=self.RUNNING):
            return [proc.pid for proc in emulator_processes("Phone", known)]

    def test_only_this_avds(self):
        self.assertEqual(self.found(), [1, 2])

    def test_the_watchdog_an_exiting_emulator_leaves_counts_when_it_names_ours(self):
        self.assertEqual(self.found({1, 2}), [1, 2, 3])


class StoppingThePc(unittest.TestCase):
    """A PC process that will not stop is a failure, and is not left running."""

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

    def test_a_daemon_that_ignores_shutdown_is_killed_and_reported(self):
        self.pc.process = stubborn = self.sleeper()
        with (patch.object(PcHost, "manage", return_value={"stopped": True}),
              patch("android_e2e.PC_STOP_BOUND", 0.2), patch("android_e2e.tkfs_processes", return_value=[])):
            failures, _ = self.pc.stop()
        self.assertEqual(len(failures), 1)
        self.assertIn(f"STRANDED_PROCESS: daemon pid {stubborn.pid}", failures[0])
        self.assertIsNotNone(stubborn.poll())

    def test_a_worker_that_outlives_its_daemon_is_killed_and_reported(self):
        self.pc.process = self.sleeper("pass")
        worker = self.sleeper()
        left = [Proc(worker.pid, "tkfs.exe", f"tkfs --state {self.pc.home}")]
        with (patch.object(PcHost, "manage", return_value={"stopped": True}),
              patch("android_e2e.KILL_BOUND", 0.2), patch("android_e2e.tkfs_processes", return_value=left)):
            failures, _ = self.pc.stop()
        self.assertEqual(len(failures), 1)
        self.assertIn(f"STRANDED_PROCESS: tkfs.exe pid {worker.pid}", failures[0])
        self.assertIsNotNone(worker.wait(10))

    def test_a_clean_stop_says_what_ran(self):
        self.pc.process = self.sleeper("pass")
        ran = [[Proc(1, "tkfs.exe", ""), Proc(2, "tkfs.exe", "")], []]
        with (patch.object(PcHost, "manage", return_value={"stopped": True}),
              patch("android_e2e.tkfs_processes", side_effect=ran)):
            self.assertEqual(self.pc.stop(), ([], "2 process(es) ran, none left"))


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


if __name__ == "__main__":
    unittest.main()
