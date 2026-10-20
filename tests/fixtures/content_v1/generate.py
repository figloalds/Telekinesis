"""Independent reference encoder for frozen WP6.0 vectors (manual regeneration)."""
import hashlib
import json
from pathlib import Path
import struct


def raw(data):
    return hashlib.sha256(data).digest()


def node(level, children):
    encoded = (b"TKFM\x01" + bytes([level])
               + struct.pack("<QH", sum(length for length, _ in children), len(children))
               + b"".join(bytes([0 if level == 0 else 1]) + struct.pack("<Q", length) + digest
                          for length, digest in children))
    return encoded, raw(b"tkfs:manifest:v1\0" + encoded)


data = {}
for name, payload in [("empty", b""), ("direct", b"abc"), ("boundary", b"x" * 65536)]:
    descriptor = b"TKFC\x01\x01\x01\x01" + struct.pack("<Q", len(payload)) + raw(payload)
    data[name] = {"descriptor_hex": descriptor.hex(), "digest": raw(payload).hex(),
                  "len": len(payload)}
leaf, leaf_hash = node(0, [(65536, raw(b"x" * 65536)), (1, raw(b"y"))])
data["tail"] = {"node_hex": leaf.hex(), "digest": leaf_hash.hex(),
                "descriptor_hex": (b"TKFC\x01\x02\x01\x01" + struct.pack("<Q", 65537)
                                   + leaf_hash).hex()}
full, full_hash = node(0, [(65536, raw(b"x" * 65536))] * 256)
tail, tail_hash = node(0, [(1, raw(b"y"))])
root, root_hash = node(1, [(65536 * 256, full_hash), (1, tail_hash)])
data["indexed"] = {"node_hex": root.hex(), "digest": root_hash.hex(),
                   "full_leaf_digest": full_hash.hex(), "tail_leaf_digest": tail_hash.hex()}
data["legacy"] = {"descriptor_hex": (b"TKFC\x01\x00\x00\x01" + struct.pack("<Q", 3)
                                   + raw(b"abc")).hex()}
Path(__file__).with_name("vectors.json").write_text(
    json.dumps(data, indent=2) + "\n", encoding="utf-8")
