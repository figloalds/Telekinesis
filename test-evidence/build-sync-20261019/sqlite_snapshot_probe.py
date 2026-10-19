"""Isolated SQLite model of a lost best-effort availability-index update.

Uses only a temporary directory. Does not access TKFS data or run TKFS code.
Run: python sqlite_snapshot_probe.py
The controlled WAL interleaving proves a SQLite mechanism, not incident cause.
"""

import json
import sqlite3
import tempfile
from pathlib import Path


def main():
    with tempfile.TemporaryDirectory(prefix="tkfs-index-probe-") as directory:
        root = Path(directory)
        db = root / "model.sqlite"
        a = sqlite3.connect(db, isolation_level=None)
        b = sqlite3.connect(db, isolation_level=None)
        try:
            assert a.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
            a.executescript(
                "CREATE TABLE availability(hash TEXT PRIMARY KEY);"
                "CREATE TABLE refs(hash TEXT PRIMARY KEY);"
                "CREATE TABLE unrelated(value INTEGER);"
            )
            # Represents bytes already installed before a derived index update.
            # This is not a model of TKFS's fsync/rename durability guarantees.
            (root / "object").write_bytes(b"model bytes")
            a.execute("SAVEPOINT object_available")
            assert a.execute("SELECT * FROM availability").fetchall() == []
            b.execute("INSERT INTO unrelated VALUES (1)")
            try:
                a.execute("INSERT INTO availability VALUES ('object')")
            except sqlite3.OperationalError as error:
                code = getattr(error, "sqlite_errorcode", None)
                name = getattr(error, "sqlite_errorname", None)
                assert code == sqlite3.SQLITE_BUSY_SNAPSHOT, (code, name)
                a.execute("ROLLBACK TO object_available")
                a.execute("RELEASE object_available")
            else:
                raise AssertionError("Expected a WAL snapshot-upgrade conflict")

            # A later transaction can succeed after the failed savepoint ends.
            a.execute("INSERT INTO refs VALUES ('object')")
            wants = a.execute(
                "SELECT hash FROM refs WHERE hash NOT IN "
                "(SELECT hash FROM availability)"
            ).fetchall()
            assert wants == [("object",)]
            assert (root / "object").read_bytes() == b"model bytes"
            a.execute("INSERT INTO availability VALUES ('object')")
            repaired = a.execute(
                "SELECT hash FROM refs WHERE hash NOT IN "
                "(SELECT hash FROM availability)"
            ).fetchall()
            assert repaired == []
            print(json.dumps({
                "sqlite_version": sqlite3.sqlite_version,
                "error_code": code,
                "error_name": name,
                "local_bytes_present_but_model_wanted_object": True,
                "index_repair_removed_model_want": True,
                "tkfs_reproduction": False,
                "incident_causality_proven": False,
            }, indent=2))
        finally:
            b.close()
            a.close()


if __name__ == "__main__":
    main()
