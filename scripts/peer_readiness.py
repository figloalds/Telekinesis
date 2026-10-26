"""Bound initial peer import using native, read-only backing-store snapshots.

The object inventory proves committed membership/kind, not independent peer
payload integrity. Fetch validates the received bytes before committing them.
No connections or pack handles remain open when the workload starts.
"""
from contextlib import contextmanager
from pathlib import Path
import hashlib
import json
import math
import sqlite3
import struct
import time

CHUNK_SIZE = 65536
MAX_OBJECT_BYTES = 64 * 1024 * 1024
MAX_FILES = 100000
MAX_OBJECTS = 1000000


def require(value, message):
    if not value:
        raise ValueError(message)


def check_deadline(deadline):
    if deadline is not None and time.monotonic() >= deadline:
        raise TimeoutError("peer initial metadata/payload import deadline exceeded")


@contextmanager
def read_only(path, deadline):
    db = sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True, timeout=.2)
    try:
        if deadline is not None:
            db.set_progress_handler(lambda: int(time.monotonic() >= deadline), 1000)
        yield db
    except sqlite3.OperationalError:
        check_deadline(deadline)
        raise
    finally:
        db.close()


def required_objects(state, deadline=None):
    """Capture the source head and current refs in the same metadata snapshot."""
    state = Path(state)
    check_deadline(deadline)
    with read_only(state / "metadata.sqlite", deadline) as db:
        db.execute("BEGIN")
        active = db.execute("SELECT value FROM config WHERE key='active'").fetchone()[0]
        head = db.execute("SELECT seq,id FROM events ORDER BY seq DESC LIMIT 1").fetchone()
        rows = db.execute(
            "SELECT state FROM current WHERE branch=? "
            "AND json_extract(CAST(state AS TEXT),'$.deleted')=0 "
            "AND json_extract(CAST(state AS TEXT),'$.content') IS NOT NULL LIMIT ?",
            (active, MAX_FILES + 1)).fetchall()
        db.rollback()
    require(len(rows) <= MAX_FILES, "initial peer inventory live-file bound exceeded")
    wanted, manifests = {}, {}
    manifest_bytes = 0

    def add(digest, kind):
        require(len(digest) == 32, "invalid digest")
        require(wanted.setdefault(digest, kind) == kind, "current object kind collision")
        require(len(wanted) <= MAX_OBJECTS, "initial peer inventory object bound exceeded")

    with read_only(state / "objects/index.sqlite", deadline) as db:
        for row in rows:
            check_deadline(deadline)
            reference = json.loads(row[0])["content"]
            require(reference["format"] == 1 and reference["algorithm"] == "sha256",
                    "unsupported content reference")
            spelling = reference["digest"]
            require(isinstance(spelling, str) and len(spelling) == 64
                    and all(c in "0123456789abcdef" for c in spelling), "invalid digest")
            digest = bytes.fromhex(spelling)
            length, kind = reference["total_len"], reference["kind"]
            require(type(length) is int and length >= 0, "invalid reference length")
            if kind == "direct":
                require(length <= CHUNK_SIZE, "invalid direct reference")
                add(digest, 1)
                continue
            require(kind == "manifest" and length > CHUNK_SIZE, "invalid manifest reference")
            add(digest, 2)
            if digest in manifests:
                require(manifests[digest] == length, "inconsistent manifest length")
                continue
            location = db.execute(
                "SELECT segment,offset,length,kind FROM objects WHERE digest=?", (digest,)).fetchone()
            require(location is not None and location[3] == 2, "source manifest missing or wrong kind")
            segment, offset, count, _ = location
            require(all(type(v) is int and v >= 0 for v in [segment, offset, count])
                    and 13 <= count <= MAX_OBJECT_BYTES, "invalid source manifest location")
            with (state / "objects/packs" / f"{segment:016x}.pack").open("rb") as stream:
                stream.seek(offset)
                data = stream.read(count)
            require(len(data) == count and hashlib.sha256(data).digest() == digest,
                    "corrupt source manifest")
            require(data[:5] == b"TKFM\x02", "invalid source manifest framing")
            require(struct.unpack_from("<Q", data, 5)[0] == length
                    and count == 13 + 32 * ((length + CHUNK_SIZE - 1) // CHUNK_SIZE),
                    "invalid source manifest length")
            for start in range(13, count, 32):
                add(data[start:start + 32], 1)
            manifests[digest] = length
            manifest_bytes += count
    check_deadline(deadline)
    fingerprint = hashlib.sha256(b"".join(d + bytes([k]) for d, k in sorted(wanted.items()))).hexdigest()
    return wanted, {
        "source_active": active,
        "source_sequence": head[0] if head else 0,
        "source_event": head[1] if head else None,
        "live_files": len(rows),
        "required_objects": len(wanted),
        "required_manifests": len(manifests),
        "source_manifest_bytes_verified": manifest_bytes,
        "required_set_sha256": fingerprint,
    }


def inventory(state, wanted, deadline=None):
    missing, wrong, rows = set(wanted), 0, 0
    with read_only(Path(state) / "objects/index.sqlite", deadline) as db:
        for digest, kind in db.execute("SELECT digest,kind FROM objects"):
            rows += 1
            if rows % 1024 == 0:
                check_deadline(deadline)
            require(rows <= MAX_OBJECTS, "initial peer receiver-index bound exceeded")
            if digest in wanted:
                if wanted[digest] == kind:
                    missing.discard(digest)
                else:
                    wrong += 1
    check_deadline(deadline)
    return {"ready": not missing and not wrong, "required_objects": len(wanted),
            "present_required_objects": len(wanted) - len(missing),
            "missing_objects": len(missing), "wrong_kind_objects": wrong,
            "missing_sample": [d.hex() for d in sorted(missing)[:8]],
            "receiver_index_objects": rows}


def wait_peer_ready(source, peer, timeout=120, observation=None):
    """One cumulative bound for source snapshot, metadata arrival and objects.

    An optional observation callback retains progress outside timed workloads.
    The initial and terminal inventories remain in the returned gate evidence.
    """
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("peer catch-up timeout must be finite and positive")
    started = time.monotonic()
    deadline = started + timeout
    wanted, result = required_objects(source, deadline)
    result.update({"timeout_seconds": timeout, "definition":
                   "current active nondeleted refs; source manifest SHA/framing verified; "
                   "peer readiness is committed-index membership/kind, not independent byte verification"})
    try:
        while result["source_event"]:
            check_deadline(deadline)
            with read_only(Path(peer) / "metadata.sqlite", deadline) as db:
                present = db.execute("SELECT 1 FROM events WHERE id=?", (result["source_event"],)).fetchone()
            if present:
                break
            time.sleep(min(.05, max(0, deadline - time.monotonic())))
        result["metadata_seconds"] = time.monotonic() - started
        result["inventory_at_metadata"] = inventory(peer, wanted, deadline)
        current = result["inventory_at_metadata"]
        while True:
            result["inventory"] = current
            result["seconds"] = time.monotonic() - started
            if observation:
                observation(result)
            if current["ready"]:
                check_deadline(deadline)
                return result
            time.sleep(min(.2, max(0, deadline - time.monotonic())))
            check_deadline(deadline)
            current = inventory(peer, wanted, deadline)
    except BaseException as error:
        result["seconds"] = time.monotonic() - started
        result["error"] = f"{type(error).__name__}: {error}"
        if observation:
            observation(result)
        raise
