"""Read-only, WAL-aware metadata inventory and comparison (Python 3.6+).

Run against the backing state directory, never the mounted filesystem. Reports
contain IDs, hashes and counts, not file contents, pairing codes or credentials.
SQLite is read in one transaction; filesystem availability is a best-effort sample.
"""
import argparse
import collections
import hashlib
import json
import pathlib
import sqlite3
import time


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     ensure_ascii=False).encode("utf-8")).hexdigest()


def links(event):
    return set(event.get("dependencies", [])) | {
        parent for change in event["changes"] for parent in change["parents"]}


def inspect(root):
    root = pathlib.Path(root).resolve()
    db = sqlite3.connect((root / "metadata.sqlite").as_uri() + "?mode=ro", uri=True)
    db.execute("PRAGMA query_only=ON")
    db.execute("BEGIN")
    try:
        config = dict(db.execute("SELECT key,value FROM config"))
        events = {key: json.loads(payload) for key, payload in db.execute(
            "SELECT id,payload FROM events")}
        pending = {key: json.loads(payload) for key, payload in db.execute(
            "SELECT id,payload FROM incoming_events")}
        errors = dict(db.execute("SELECT id,error FROM incoming_events WHERE error IS NOT NULL"))
        shared = {key: event for key, event in events.items() if event["branch"]["shared"]}
        event_hashes = {}
        for key, event in shared.items():
            normalized = dict(event)
            for field, default in [("dependencies", []), ("reviewed", []), ("object_sizes", {})]:
                normalized.setdefault(field, default)
            event_hashes[key] = digest(normalized)
        objects = {change["value"] for event in shared.values() for change in event["changes"]
                   if change["field"] == "content" and isinstance(change["value"], str)}
        sizes = dict(db.execute("SELECT hash,size FROM object_sizes"))
        missing_bytes = sorted(h for h in objects if not (root / "objects" / h).is_file())
        unknown_sizes = sorted(h for h in missing_bytes if h not in sizes)
        all_ids = set(events) | set(pending)
        missing_parents = sorted({p for e in pending.values() for p in links(e)} - all_ids)
        projections = {}
        for branch, payload in db.execute("SELECT branch,payload FROM current_state"):
            projection = json.loads(payload)
            entries = projection["entries"]
            for (entry,) in db.execute("SELECT entry FROM current_parts WHERE branch=?", (branch,)):
                entry = json.loads(entry)
                entries[entry["id"]] = entry
            projections[branch] = {
                "entries": len(entries),
                "alive_entries": sum(bool(e["alive"]) for e in entries.values()),
                "entries_digest": digest(entries),
                "pending": projection.get("pending", []),
            }
        return {
            "format": 1, "sampled_at_ms": int(time.time() * 1000),
            "repo": config["repo"], "device": config["device"], "active": config["active"],
            "journal_events": len(events), "shared_events": len(shared),
            "journal_origins": dict(collections.Counter(e["device"] for e in events.values())),
            "pending_events": len(pending), "quarantined": errors,
            "pending_origins": dict(collections.Counter(e["device"] for e in pending.values())),
            "missing_parent_ids": missing_parents,
            "shared_event_hashes": event_hashes, "pending_event_ids": sorted(pending),
            "shared_referenced_objects": len(objects), "missing_content_ids": missing_bytes,
            "unknown_size_ids": unknown_sizes,
            "outgoing_unacknowledged": db.execute(
                "SELECT count(*) FROM outbox WHERE acknowledged=0").fetchone()[0],
            "projections": projections,
            "snapshots": [dict(branch=b, cursor=c) for b, c in db.execute(
                "SELECT branch,cursor FROM startup_snapshots")],
        }
    finally:
        db.rollback()
        db.close()


def compare(left, right):
    if left["repo"] != right["repo"]:
        raise ValueError("REPOSITORY_MISMATCH")
    a, b = left["shared_event_hashes"], right["shared_event_hashes"]
    inventory = set(a) | set(b) | set(left["pending_event_ids"]) | set(right["pending_event_ids"])
    def side(report, own, other):
        missing = sorted(set(other) - set(own))
        absent = sorted(set(report["missing_parent_ids"]) - inventory)
        return {"device": report["device"], "accepted": len(own),
                "pending": report["pending_events"], "quarantined": len(report["quarantined"]),
                "missing_accepted_events": len(missing), "missing_event_sample": missing[:20],
                "missing_parents_absent_on_both": len(absent),
                "absent_parent_sample": absent[:20],
                "missing_content": len(report["missing_content_ids"]),
                "unknown_content_sizes": len(report["unknown_size_ids"]),
                "active_projection": report["projections"].get(report["active"])}
    return {"repo": left["repo"], "same_active_branch": left["active"] == right["active"],
            "accepted_metadata_equal": a == b,
            "payload_mismatches": sorted(k for k in set(a) & set(b) if a[k] != b[k]),
            "left": side(left, a, b), "right": side(right, b, a)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--state", help="backing state directory containing metadata.sqlite")
    source.add_argument("--compare", nargs=2, metavar=("LEFT_JSON", "RIGHT_JSON"))
    parser.add_argument("--output", help="write report to this file instead of stdout")
    args = parser.parse_args()
    if args.state:
        report = inspect(args.state)
    else:
        report = compare(*(json.loads(pathlib.Path(p).read_text(encoding="utf-8-sig"))
                           for p in args.compare))
    text = json.dumps(report, indent=2, sort_keys=True)
    if args.output:
        pathlib.Path(args.output).write_text(text + "\n", encoding="utf-8")
    else:
        print(text)


if __name__ == "__main__":
    main()
