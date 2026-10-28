# Wave 6 replacement: objects, snapshots and a passthrough working layer

Status: implementation and correctness qualification in progress, 2026-10-28. Supersedes the scope of [WAVE_6_PLAN.md](WAVE_6_PLAN.md)
and the WP6.G interlude. Steps 1–6 have implementation commits; runtime cutover
and benchmark qualification are blocked by W6-P0-MOUNT-LIFECYCLE. See the
replacement step notes. The
[authority scope](AUTHORITY_SCOPE.md) (personal use, pairing is the admission
boundary, TLS/WSS or VPN for the channel) is unchanged.

## 0. Immediate blocking demand

**W6-P0-MOUNT-LIFECYCLE remains OPEN.** The user's 2026-10-28 decision is to
proceed with correctness and remaining Wave 6 implementation, leaving performance
gates for last. This supersedes the earlier ordering that blocked all other work
on performance acceptance. It does not waive the no-self-handle, cache delivery,
durability, bounded cleanup or dependency checks, or authorize host mounts/cutover.

The no-self-handle fix is implemented ([note](notes/mount-lifecycle-fix.md),
invariants in section 3a). Repeated expanded 44-case isolated lifecycle matrices
pass with the pinned WinFsp Beta5 dependency. Full performance acceptance remains
open; the blocker retains exact candidate identities and earlier failures.
All mounted correctness and performance runs still belong in an isolated Windows
VM; folder mounts use the same driver and callbacks. Before a performance lap, read the blocker's
[performance triage](WAVE_6_MOUNT_LIFECYCLE_BLOCKER.md#performance-triage-read-the-counters-before-the-next-vm-lap):
retained callback counters show that opening an existing file copies it into a
scratch, which dominates Git/build cost, and that metadata work is secondary.
Section 3's hydrate-on-first-open is the design point to revisit first.

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

- Regular-file handles share an **in-memory draft** and pin its immutable base.
  Open and read create no host scratch or sidecar. Reads come from packs/LRU
  until a range is materialized; the first write or size change creates the
  scratch and recovery record under the store directory. Partial writes hydrate
  only touched chunks, with recovery markers before accepting edits. A handle is
  bound to the version it opened on Linux. On Windows, data caching takes
  priority: existing handles may see remote updates because WinFsp shares
  cached pages by pathname (user decision, 2026-10-23). A local edit retains
  its parent version so concurrent changes still produce a conflict at seal.
- **Read, write, truncate and append** use the shared draft's immutable ranges
  and host scratch handle. No hashing or SQLite on the byte path. After demand
  hydration, write bookkeeping is an in-memory dirty flag and dirty-range bitmap (4 KiB granularity)
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

## 3a. Windows cache and mount lifecycle invariants

These rules come from [W6-P0-MOUNT-LIFECYCLE](WAVE_6_MOUNT_LIFECYCLE_BLOCKER.md).
They hold for drive-letter and directory mounts alike.

- **No self-handles.** The server process never opens a path under its own
  mount and never issues I/O against it: no root handle, no `FlushFileBuffers`
  on the volume, no helper spawned to do it on the server's behalf. Windows
  closes a dying process's handles one at a time and synchronously; a cleanup
  that needs the dead server's own dispatcher never completes, and the process
  can neither finish exiting nor be killed. The native shim refuses Create/Open
  requests originating from the server's PID so a regression fails loudly.
- **Cached bytes reach the server through the driver.** With
  `FileInfoTimeout = INFINITE` the Windows data cache is shared per pathname and
  dirty pages stay in the cache after a handle closes. `FlushAndPurgeOnCleanup`
  makes the FSD deliver them as Write callbacks before Cleanup, which is what
  "seal on close" relies on. Its purge cost is measured against the gate in the
  VM; the fallback, if that cost fails the gate, is to seal at Close after the
  Cache Manager's own lazy writes, never a volume flush from the server.
- **Paging writes are accepted on any handle.** The Cache Manager writes back
  through the file object it initialised caching with, which may be a read-only
  handle. WinFsp checked access at Create; the adapter accepts `ConstrainedIo`
  writes on every handle and keeps the access check for ordinary writes.
- **fsync is the durability point.** WinFsp flushes a file's cache before
  posting Flush, so a per-handle flush seals with no server-side volume I/O.
  Dirty cached pages of handles still open when the daemon is killed are lost,
  as they would be on NTFS without fsync.
- **Shutdown releases the mount regardless of seal or flush failure.** The
  control `shutdown` sets `stopping` before it reports a flush error; `detach()`
  stops the dispatcher and only then returns the final seal error; the daemon
  loop runs both and reports the first failure. A failed seal is a reported
  durability failure, not a reason to leave the dispatcher running; recoverable
  scratch files stay on disk for the startup re-seal.
- **Harnesses bound every wait.** Graceful stop, diagnostic capture and the
  post-kill wait each have a deadline. A process still alive afterwards is a
  failed run with its PID, mount, log and exit state preserved next to the
  daemon log; no further mounts are started over it.

## 4. Seal on close

- The last Cleanup of a writable handle marks the entity **pending seal**. On
  Windows the driver has flushed and purged that file's cached pages into Write
  callbacks before Cleanup is delivered (section 3a), so the scratch file is
  complete at that point. A flush or
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
  survive a crash are compared with their immutable base or committed receipt
  before discard. Equal or backwards timestamps cannot prove unchanged bytes;
  different materialized bytes are re-sealed on startup.
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
| `native/mount.c`, `src/mount.rs`, `src/fuse_linux.rs` | adapters | Passthrough handles, caching on with flush-and-purge on cleanup, notify on remote change, no self-handles (section 3a) |
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

Targets as decided by the user on 2026-10-26 (WinFsp Beta5 kept as shipped;
[reassessment section 5](notes/perf-gate-reassessment.md#5-proposed-rewrite-of-the-section-8-targets)
adopted for copy, sequential write and Git). Those three workloads are bounded
by WinFsp's request model rather than by the server
([source review](notes/winfsp-source-review.md)). Their pass/fail bound is the
pinned memory-filesystem reference, run by the same script in the same guest
and session, with the same cleanup cache policy as the candidate
(`--reference`, `--cache-policy`). Their native ratio is still printed and
recorded on every run. Every other bound is unchanged.

| Workload | Target | Measured against |
|---|---|---|
| Copy 2,000 files, 1–64 KiB, into the mount | ≤ 1.3× | memory-filesystem reference |
| Clean build and no-op build of the .NET harness inside the mount | ≤ 1.3× clean, ≤ 1.2× no-op | native |
| 1 GiB sequential write | ≤ 1.2× | memory-filesystem reference |
| 1 GiB sequential read | ≤ 1.2× | native |
| 4 KiB random overwrites of a 1 GiB file, 10,000 ops | ≤ 1.5×, and ≤ one new chunk per op at seal | native |
| `git status` on a 50,000-file tree, client `core.fscache=true` | ≤ 1.3× | memory-filesystem reference |
| Remote change visible locally after apply | ≤ 250 ms with the file cached | absolute |
| Idle-peer overhead | ≤ 20% per metric | the same mount without a peer |

The superseded targets were copy ≤ 1.3×, sequential write ≤ 1.2× and Git
≤ 1.3× native, with Git run with `core.fscache` unset. Git now runs with
`core.fscache=true`, the setting Git for Windows installs by default, in the
native, reference and mounted runs alike. A report whose Git client setting,
cache policy, host, Git version or workload sizes differ from its reference
cannot pass. A run without a reference cannot pass the three reference-bound
workloads. The reference is the upstream memory filesystem at the pinned
Beta5 source with the recorded capacity adapter
([section 9 of the reassessment](notes/perf-gate-reassessment.md#9-complete-paired-policy-reference-measurement-2026-10-26));
it is a measured reference, not a theoretical lower bound.

Native baseline, captured 2026-10-23 on the development NVMe with the warm
OS cache (`test-evidence/bench/native.json`, medians of five runs): copy 2,000
files 1,439 ms; 1 GiB write 399 ms, read 387 ms; 10,000 random 4 KiB writes
84 ms; `git status` on 50,000 files 62 ms; clean build 4,911 ms; no-op build
532 ms; incremental build 1,522 ms. Rerun with the same script and compare with
`--compare test-evidence/bench/native.json`.

Correctness gates stay small and exact: seal exactly-once across restart,
conflict detection for concurrent edits of one version, snapshot fork isolation,
hydrate-on-demand with a missing remote chunk, pack GC never removing a pinned
object, notify invalidation after a remote update, and the mount lifecycle
checks of the blocker (bounded graceful and forced exit with no stranded
process, a reader opened before a writer with no access error).

## 9. Order of work

0. Preserve the [mount safety invariants](WAVE_6_MOUNT_LIFECYCLE_BLOCKER.md)
   throughout implementation; run mounted correctness checks only in an isolated VM.
1. Benchmark script and the native baseline numbers.
2. Pack store with the digest index and LRU; port `LocalContent` reads.
3. Scratch-file working layer and passthrough adapters with caching on.
4. Seal on close with batched events; tree objects and the cached projection.
5. Per-entity conflicts and the conflicts view.
6. Digest fetch over the existing sessions; remove the admission engine.
7. Delete the removed modules and tables; trim tests; finish correctness/recovery
   and mounted lifecycle qualification, then run the section 8 performance gate last.

Steps 1–6 and canonical-path removal are implemented. Per the 2026-10-28 ordering,
correctness fixes proceed with exact regression checks while final performance
acceptance stays open; cutover requires all gates, including performance, to pass.
Notes per step are at most one page.
