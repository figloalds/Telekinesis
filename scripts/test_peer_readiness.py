"""Native-only regression checks; never loads a filesystem driver or mounts."""
from pathlib import Path
from contextlib import closing, contextmanager
import hashlib
import json
import sqlite3
import struct
import tempfile
import unittest
from unittest.mock import patch

from peer_readiness import inventory, required_objects, wait_peer_ready


@contextmanager
def writable(path, **kwargs):
    with closing(sqlite3.connect(path, **kwargs)) as db:
        with db:
            yield db


class PeerReadinessTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="tkfs-peer-readiness-")
        self.addCleanup(self.temp.cleanup)
        self.source, self.peer = [Path(self.temp.name) / name for name in ["source", "peer"]]
        for state in [self.source, self.peer]:
            (state / "objects/packs").mkdir(parents=True)
            with writable(state / "objects/index.sqlite") as db:
                db.execute("CREATE TABLE objects(digest BLOB PRIMARY KEY,segment INTEGER,offset INTEGER,length INTEGER,kind INTEGER)")
            with writable(state / "metadata.sqlite") as db:
                db.executescript("CREATE TABLE config(key TEXT,value TEXT); CREATE TABLE events(seq INTEGER,id TEXT); CREATE TABLE current(branch TEXT,state BLOB);")
                db.execute("INSERT INTO config VALUES('active','main')")
        self.direct = hashlib.sha256(b"direct").digest()
        self.chunks = [hashlib.sha256(b"a" * 65536).digest(), hashlib.sha256(b"b").digest()]
        self.manifest = b"TKFM\x02" + struct.pack("<Q", 65537) + b"".join(self.chunks)
        self.root = hashlib.sha256(self.manifest).digest()
        self.pack = self.source / "objects/packs/0000000000000000.pack"
        self.pack.write_bytes(self.manifest)
        with writable(self.source / "objects/index.sqlite") as db:
            db.execute("INSERT INTO objects VALUES(?,0,0,?,2)", (self.root, len(self.manifest)))
        self.rows = [("main", False, self.ref(self.direct, "direct", 6)),
                     ("main", False, self.ref(self.root, "manifest", 65537)),
                     ("main", True, self.ref(bytes(32), "direct", 1)),
                     ("other", False, self.ref(bytes([1]) * 32, "direct", 1))]
        self.write_source_rows()
        with writable(self.source / "metadata.sqlite") as db:
            db.execute("INSERT INTO events VALUES(3,'head')")
        self.wanted = {self.direct: 1, self.root: 2, self.chunks[0]: 1, self.chunks[1]: 1}

    @staticmethod
    def ref(digest, kind, length):
        return dict(format=1, algorithm="sha256", digest=digest.hex(), kind=kind, total_len=length)

    def write_source_rows(self):
        with writable(self.source / "metadata.sqlite") as db:
            db.execute("DELETE FROM current")
            db.executemany("INSERT INTO current VALUES(?,?)", [
                (branch, json.dumps(dict(deleted=deleted, content=content)).encode())
                for branch, deleted, content in self.rows])

    def metadata_arrives(self):
        with writable(self.peer / "metadata.sqlite") as db:
            db.execute("INSERT INTO events VALUES(3,'head')")

    def payload_arrives(self):
        with writable(self.peer / "objects/index.sqlite") as db:
            db.executemany("INSERT INTO objects VALUES(?,0,0,1,?)", self.wanted.items())

    def test_inventory_includes_manifest_chunks_and_excludes_deleted_other_branch(self):
        wanted, description = required_objects(self.source)
        self.assertEqual(wanted, self.wanted)
        self.assertEqual(description["live_files"], 2)
        self.assertEqual(description["source_event"], "head")
        self.assertEqual(description["required_manifests"], 1)
        self.assertFalse(inventory(self.peer, wanted)["ready"])
        self.payload_arrives()
        self.assertTrue(inventory(self.peer, wanted)["ready"])

    def test_metadata_arrival_does_not_start_workload_before_payload(self):
        self.metadata_arrives()
        observed = []
        with patch("peer_readiness.time.sleep", side_effect=lambda _: self.payload_arrives()):
            result = wait_peer_ready(self.source, self.peer, 2,
                                     observation=lambda value: observed.append(json.loads(json.dumps(value))))
        self.assertEqual(result["inventory_at_metadata"]["missing_objects"], 4)
        self.assertFalse(observed[0]["inventory"]["ready"])
        self.assertTrue(result["inventory"]["ready"])
        self.assertLessEqual(result["metadata_seconds"], result["seconds"])
        self.assertEqual(result["timeout_seconds"], 2)

    def test_uncommitted_payload_and_wrong_kind_are_not_ready(self):
        with writable(self.peer / "objects/index.sqlite") as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("BEGIN")
            db.executemany("INSERT INTO objects VALUES(?,0,0,1,?)", self.wanted.items())
            self.assertFalse(inventory(self.peer, self.wanted)["ready"])
            db.commit()
            self.assertTrue(inventory(self.peer, self.wanted)["ready"])
            db.execute("UPDATE objects SET kind=3 WHERE digest=?", (self.chunks[0],))
        result = inventory(self.peer, self.wanted)
        self.assertFalse(result["ready"])
        self.assertEqual(result["wrong_kind_objects"], 1)

    def test_source_manifest_corruption_and_malformed_framing_fail_closed(self):
        self.pack.write_bytes(bytes([0]) + self.manifest[1:])
        with self.assertRaisesRegex(ValueError, "corrupt"):
            required_objects(self.source)
        for invalid in [b"BAD!!" + self.manifest[5:],
                        self.manifest[:5] + struct.pack("<Q", 65538) + self.manifest[13:],
                        self.manifest[:-1]]:
            with self.subTest(invalid=invalid[:13]):
                digest = hashlib.sha256(invalid).digest()
                self.pack.write_bytes(invalid)
                with writable(self.source / "objects/index.sqlite") as db:
                    db.execute("DELETE FROM objects")
                    db.execute("INSERT INTO objects VALUES(?,0,0,?,2)", (digest, len(invalid)))
                self.rows = [("main", False, self.ref(digest, "manifest", 65537))]
                self.write_source_rows()
                with self.assertRaisesRegex(ValueError, "manifest"):
                    required_objects(self.source)

    def test_one_deadline_covers_both_metadata_and_payload(self):
        now = [0.0]
        observations = []

        def advance(duration):
            if not now[0]:
                now[0] = 2.0
                self.metadata_arrives()
            else:
                now[0] += duration

        with patch("peer_readiness.time.monotonic", side_effect=lambda: now[0]), \
                patch("peer_readiness.time.sleep", side_effect=advance):
            with self.assertRaises(TimeoutError):
                wait_peer_ready(self.source, self.peer, 3,
                                observation=lambda value: observations.append(dict(value)))
        self.assertEqual(now[0], 3)
        self.assertEqual(observations[-1]["metadata_seconds"], 2)
        self.assertIn("TimeoutError", observations[-1]["error"])
        self.assertFalse(observations[-1]["inventory"]["ready"])

    def test_metadata_never_arrives_has_bounded_failure(self):
        with self.assertRaises(TimeoutError):
            wait_peer_ready(self.source, self.peer, .03)

    def test_deadline_interrupts_source_sql_scan(self):
        with writable(self.source / "metadata.sqlite") as db:
            db.executemany("INSERT INTO current VALUES('main',?)",
                           [(b'{"deleted":true,"content":null}',)] * 5000)
        ticks = iter([0.0])
        with patch("peer_readiness.time.monotonic", side_effect=lambda: next(ticks, 2.0)):
            with self.assertRaises(TimeoutError):
                required_objects(self.source, deadline=1.0)

    def test_empty_initial_store_is_ready_without_waiting_for_an_event(self):
        with writable(self.source / "metadata.sqlite") as db:
            db.execute("DELETE FROM current")
            db.execute("DELETE FROM events")
        result = wait_peer_ready(self.source, self.peer, 2)
        self.assertEqual(result["source_sequence"], 0)
        self.assertEqual(result["required_objects"], 0)
        self.assertTrue(result["inventory"]["ready"])

    def test_observation_and_return_retain_no_database_handles(self):
        self.metadata_arrives()
        self.payload_arrives()

        def exclusive_transactions(_):
            for state in [self.source, self.peer]:
                for name in ["metadata.sqlite", "objects/index.sqlite"]:
                    with writable(state / name, timeout=0) as db:
                        db.execute("BEGIN EXCLUSIVE")
                        db.rollback()

        result = wait_peer_ready(self.source, self.peer, 2, observation=exclusive_transactions)
        self.assertTrue(result["inventory"]["ready"])
        exclusive_transactions(result)

    def test_inventory_bounds_and_invalid_timeout_fail_closed(self):
        for timeout in [0, -1, float("inf"), float("nan")]:
            with self.subTest(timeout=timeout), self.assertRaises(ValueError):
                wait_peer_ready(self.source, self.peer, timeout)
        with patch("peer_readiness.MAX_FILES", 1), self.assertRaisesRegex(ValueError, "live-file bound"):
            required_objects(self.source)
        with patch("peer_readiness.MAX_OBJECTS", 1), self.assertRaisesRegex(ValueError, "object bound"):
            required_objects(self.source)


if __name__ == "__main__":
    unittest.main()
