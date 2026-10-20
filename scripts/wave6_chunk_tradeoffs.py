"""Deterministic fixed-chunk reuse experiment; not a TKFS timing/release gate.

Compares 16/64/256 KiB using the same input and edits. The implementation's
selected chunk size remains 64 KiB. Hashing here models full re-import, not
dirty-range finalization; identity-set reuse models physical deduplication.
"""
import argparse
import hashlib
import json
from pathlib import Path
import random


def identities(data, size):
    return [(hashlib.sha256(data[start:start + size]).hexdigest(),
             len(data[start:start + size]))
            for start in range(0, len(data), size)]


def experiment():
    data = random.Random(600).randbytes(8 * 1024 * 1024)
    patch = b"modified content" * 256  # exactly 4096 bytes
    middle = len(data) // 2
    workloads = {
        "aligned_overwrite_4k": data[:middle] + patch + data[middle + len(patch):],
        "boundary_overwrite_4k": data[:middle - 2048] + patch + data[middle + 2048:],
        "beginning_insertion_4k": patch + data,
        "middle_insertion_4k": data[:middle] + patch + data[middle:],
        "append_4k": data + patch,
        "truncate_4k": data[:-len(patch)],
        "atomic_replace_identical": data,
    }
    results = []
    for size in (16 * 1024, 64 * 1024, 256 * 1024):
        old = identities(data, size)
        known = {digest for digest, _ in old}
        for name, changed in workloads.items():
            new = identities(changed, size)
            created = {digest: length for digest, length in new if digest not in known}
            results.append({
                "chunk_size": size, "workload": name, "old_chunks": len(old),
                "new_chunks": len(new), "new_unique_chunks": len(created),
                "new_unique_data_bytes": sum(created.values()),
                "reused_chunks": sum(digest in known for digest, _ in new),
                "full_reimport_hash_bytes": len(changed),
                "manifest_bytes": "not simulated",
            })
    return {"seed": 600, "input_bytes": len(data), "input_sha256": hashlib.sha256(data).hexdigest(),
            "selected_chunk_size": 65536, "kind": "deterministic representation simulation",
            "results": results}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(experiment(), indent=2) + "\n", encoding="utf-8")
    print(args.output)
