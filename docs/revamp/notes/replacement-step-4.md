# Step 4: events, trees and batched seals

The replacement metadata database contains explicit entity parent versions,
current entries/heads, snapshots and idempotent seal receipts. Byte writes use
neither it nor the pack index. A delayed sealer prepares changed 64 KiB chunks,
flushes packs, then atomically commits content events and receipts with FULL
WAL synchronization. Namespace transactions use NORMAL WAL synchronization;
flush also checkpoints namespace-only changes.

Current lookup/listing use indexed in-memory entries. Snapshots encode sorted
tree objects and an event frontier. Fork creates a branch pointing at the same
snapshot; opening a fork loads cached trees plus its own entry deltas.

The Windows callback implementation now serves scratch handles and is ready
for cutover. Tests pass for dirty-chunk reuse, restart durability, receipt-based
exactly-once recovery, surviving unsealed files, and snapshot fork isolation.
