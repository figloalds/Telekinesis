# Step 2: immutable packs

The replacement object provider uses 64 MiB append-only packs, one SQLite
digest index, process-shared segment readers, and a bounded 256 MiB verified
LRU. Objects are hashed once on a cache miss. A corrupt object is removed from
the index so hydration can fetch it again. Flat manifests contain ordered
64 KiB chunk digests; direct content includes the canonical empty chunk.

Pack flush orders segment durability before the index commit. GC copies live
objects, flushes the replacement segment, commits index changes, then removes
the old segment. Pins protect complete content closures. Unit tests cover
reopen, deduplication, verified cache hits, range reads, corruption and pinned GC.

The new provider is assembled alongside the running implementation until the
runtime cutover; mounted performance will be measured at that point.
