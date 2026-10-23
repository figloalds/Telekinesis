# Wave 6 replacement: objects, snapshots and a passthrough working layer

Status: implementation in progress, 2026-10-23. Supersedes the scope of [WAVE_6_PLAN.md](WAVE_6_PLAN.md)
and the WP6.G interlude. Steps 1–6 have implementation commits; runtime cutover
and benchmark qualification are in progress. See the replacement step notes. The
[authority scope](AUTHORITY_SCOPE.md) (personal use, pairing is the admission
boundary, TLS/WSS or VPN for the channel) is unchanged.

## 1. Why

The Wave 6 write path puts the VFS on every byte. Each WinFsp write callback is
a SQLite transaction under one global journal mutex that re-reads and re-hashes
a 72 KiB patch three times and path-copies two Merkle overlay maps; each close
runs a FULL-sync checkpoint, one fsync per 64 KiB chunk file and three more
transactions; kernel caching is off. Measured on the development disk
(micro-benchmark of the primitives, not TKFS itself):

| Primitive the current path uses | Median |
|---|---:|
| Raw NTFS 64 KiB write, no fsync | 0.32 ms |
| Chunk install: write, fsync, write-through rename | 1.87 ms |
| Journal transaction per write (72 KiB blob + map nodes) | 0.87 ms |
| FULL-sync transaction (per checkpoint/close) | 0.89 ms |
| SHA-256 of the patch, three times | 0.09 ms |

Hashing is noise. The fixed per-operation costs are the bottleneck, and 64 KiB
chunks multiply them. The product model never needed any of it: a version
exists at a save boundary, not at write number 37 of an open handle.

## 2. The model

- **Object**: immutable bytes addressed by SHA-256. Three kinds: data chunk
  (64 KiB logical, last one short), file manifest (flat ordered list of chunk
  digests plus total length; files at or below 64 KiB are one direct chunk),
  and tree (sorted entries: name, entity id, kind, content ref, times, mode).
- **Snapshot**: one tree root digest plus the event frontier it was derived
  from. A snapshot is a slice of the entity graph at a point in causal time.
  Forking a snapshot is copying a digest. Nothing is copied.
- **Entity**: a stable id that survives renames. Each entity has a version DAG:
  an event creates a version with explicit parent version(s).
- **Event**: {entity, parent versions, operation (create, content, rename,
  delete, mkdir, times), new content ref, branch, author peer, clock}. The
  event journal is the only authoritative history; trees are derived and cached.
- **Conflict**: an entity version with two or more successor versions from
  different peers, or a local edit whose base version already has a remote
  successor. Conflicts never create sibling files in the directory. They are
  listed in the conflicts view; resolving selects a head, and the other head
  stays reachable as an object from that view and from history.

This is the user's model. The VFS exists to observe operations at their
boundaries and to present a snapshot as a directory. It is not a block device.

## 3. Working layer: scratch files and passthrough I/O

- Each open regular file is backed by one **scratch file** on the host
  filesystem under the store directory. On first open the scratch file is
  hydrated from the entity's current manifest: local chunks are copied in,
  remote chunks are fetched on demand by range and written in. A handle is
  bound to the version it opened on Linux. On Windows, data caching takes
  priority: existing handles may see remote updates because WinFsp shares
  cached pages by pathname (user decision, 2026-10-23). A local edit retains
  its parent version so concurrent changes still produce a conflict at seal.
- **Read, write, truncate and append** are passthrough calls on the scratch
  file's host handle. No hashing, no SQLite, no journal. The only bookkeeping on
  write is an in-memory dirty flag and dirty-range bitmap (4 KiB granularity)
  per open entity, used to skip unchanged chunks at seal.
- **Kernel caching is on.** `FileInfoTimeout` becomes a positive value with
  explicit invalidation: when a remote event changes an entity, the adapter
  calls the WinFsp/FUSE notify API for that path so cached data and attributes
  are dropped. This is the one coherence rule in the design.
- **Create, mkdir, rename, delete, set-times** are metadata operations. They
  record an event immediately in the metadata database (one short transaction,
  batched when several arrive within a few milliseconds) and update the
  in-memory projection. They do not touch bytes.
- Directory listing and lookup are served from the in-memory projection of the
  current snapshot, backed by the cached tree objects. No history replay.

## 4. Seal on close

- A writable handle's close marks the entity **pending seal**. A flush or
  fsync seals immediately and returns only after the event is committed. Otherwise
  a sealer thread seals entities whose last writable close is older than the
  write-back delay (default 1 s) or when the pending set exceeds a size budget.
- **Sealing an entity**: read the scratch file in 64 KiB chunks; for each chunk
  whose dirty bitmap intersects it, hash it and compare with the base manifest;
  unchanged chunks keep their digest. New chunks and the new manifest go into
  the current pack segment. Then one metadata transaction records the content
  event with its parent version. Seals are batched: many entities, one pack
  fsync, one metadata transaction, one WAL fsync.
- **Durability contract** matches the host filesystem: fsync/flush is the
  durability point. A crash loses at most the write-back window of closed but
  unsealed files, exactly as it would on NTFS without fsync. Scratch files that
  survive a crash are re-sealed on startup if their entity's recorded version
  is older than the scratch file's modification time; otherwise discarded.
- Editors that save by write-temp-then-rename produce one version per save: the
  temp file's seal plus the rename event, folded into one content event on the
  target entity when both land in the same seal batch.

## 5. Object store

- **Packs**: append-only segment files (64 MiB target) under `objects/packs/`.
  An object is {segment, offset, length}. The index is a SQLite table
  `objects(digest PRIMARY KEY, segment, offset, length, kind)`. Reads open the
  segment once per process and `pread` the range. A verified-chunk LRU
  (default 256 MiB) sits in front so a hot read never re-hashes.
- Verification happens once when an object is first read from a pack or
  received from a peer; a verified object is trusted from the cache afterwards.
  Corruption found on read marks the object missing and refetches from a peer.
- **Retention**: an object is live if reachable from any snapshot root the
  store keeps (current heads, retained history depth, unresolved conflict
  heads, pending seals). GC rewrites segments whose live fraction falls below a
  threshold. Nothing is deleted while a scratch file or read handle pins it.
- Manifests stay flat. A 1 GiB file is a 512 KiB manifest; that is acceptable
  for this workload. Paged manifests and content-defined chunking are deferred
  until a measured need exists.

## 6. Sync

- **Events first.** The existing feed replication carries events between
  paired peers. A peer applies an event as soon as its parents are known,
  whether or not the content objects are present. The projection shows the
  entry; opening it hydrates on demand.
- **Objects by digest.** A paired peer may request any object by digest. There
  is no per-reference authorization, admission outbox, lease generation or
  pool fencing. Requests are batched per peer, with one bounded retry policy
  and a per-peer backoff. Priority: foreground hydration first, then background
  prefetch of the current snapshot, then nothing else.
- **Conflicts** are detected on apply: an incoming event whose parent version
  already has a different successor opens a conflict for that entity, and the
  local head stays visible until resolved. Resolution is itself an event with
  both heads as parents, so every peer converges on the same choice.

## 7. What survives, what goes

Survives, with the noted changes:

| Area | Keep | Change |
|---|---|---|
| `src/model/content.rs` | `Digest`, `FileContentRef`, manifest encoding | Add tree objects; drop manifest paging |
| `src/content/local.rs` | provider interface, pins, counters | Loose files become pack segments; no per-chunk fsync |
| `src/core/{commit,inbox}.rs`, `src/orchestrator/meta_exchange.rs`, feed tables | event journal and replication | Events gain explicit parent versions; projection is a cached tree, not cone replay |
| `src/session/*`, `src/pairing_code.rs`, `src/orchestrator/catalog.rs` | pairing, TLS sessions, catalog | None required |
| `native/mount.c`, `src/mount.rs`, `src/fuse_linux.rs` | adapters | Passthrough handles, caching on, notify on remote change |
| `src/gui.rs`, `src/ui_model.rs` | GUI | Conflicts view reads entity heads |

Removed:

- `src/content/write.rs`, `src/content/write/*`, `src/content/overlay_map.rs`:
  the private write journal, overlay maps, checkpoints, continuations, adoption.
- `src/runtime/content_saves.rs`, `src/core/content_saves.rs` and the
  `content_save_*` tables: the save pipeline.
- Object admission and fencing: `content_root_outbox`, `object_root_admission`,
  `content_source_backoff`, `content_transfer_cursor`, `content_page_aliases`,
  `content_public_receipts`, lease generations and pool layouts in
  `src/orchestrator/objects.rs`, `inbound_objects.rs` and `src/core/transfer.rs`.
  Replaced by the batched digest fetch in section 6.
- `src/core/derive.rs` cone projection as the source of current state. Conflict
  derivation becomes the per-entity successor check.
- The three-database layout (core, write journal, catalog) collapses to one
  metadata database plus the pack index.
- The qualification apparatus: CCR numbering, evidence capsules, per-step
  hashes. Replaced by the gate below plus `cargo fmt`, `clippy` and a trimmed
  test suite that covers invariants, not evidence.

## 8. The benchmark gate

One script, `scripts/bench.py`, runs each workload native and mounted (no
peers, then with one peer connected and idle) and prints one table of medians
over five runs after one warm-up. It is the only performance gate.

| Workload | Target vs native |
|---|---|
| Copy 2,000 files, 1–64 KiB, into the mount | ≤ 1.3× |
| Clean build and no-op build of the .NET harness inside the mount | ≤ 1.3× clean, ≤ 1.2× no-op |
| 1 GiB sequential write, then read | ≤ 1.2× |
| 4 KiB random overwrites of a 1 GiB file, 10,000 ops | ≤ 1.5×, and ≤ one new chunk per op at seal |
| `git status` on a 50,000-file tree | ≤ 1.3× |
| Remote change visible locally after apply | ≤ 250 ms with the file cached |

Native baseline, captured 2026-10-23 on the development NVMe with the warm
OS cache (`test-evidence/bench/native.json`, medians of five runs): copy 2,000
files 1,439 ms; 1 GiB write 399 ms, read 387 ms; 10,000 random 4 KiB writes
84 ms; `git status` on 50,000 files 62 ms; clean build 4,911 ms; no-op build
532 ms; incremental build 1,522 ms. Rerun with the same script and compare with
`--compare test-evidence/bench/native.json`.

Correctness gates stay small and exact: seal exactly-once across restart,
conflict detection for concurrent edits of one version, snapshot fork isolation,
hydrate-on-demand with a missing remote chunk, pack GC never removing a pinned
object, and notify invalidation after a remote update.

## 9. Order of work

1. Benchmark script and the native baseline numbers.
2. Pack store with the digest index and LRU; port `LocalContent` reads.
3. Scratch-file working layer and passthrough adapters with caching on.
4. Seal on close with batched events; tree objects and the cached projection.
5. Per-entity conflicts and the conflicts view.
6. Digest fetch over the existing sessions; remove the admission engine.
7. Delete the removed modules and tables; trim tests; re-run the gate.

Each step lands when the gate table does not regress and its exact tests pass.
Notes per step are at most one page.
