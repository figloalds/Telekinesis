# Chunked content foundation and future Git-backed COW workspaces

Planning update: the user selected the [concrete Wave 6 plan](../WAVE_6_PLAN.md).
It selects 64 KiB as the initial default and owns implementation scope and gates.
This document preserves the exploratory reasoning and deferred Git feature;
its references to an experimental size do not override the selected plan.

2026-10-19. Design direction from the post-Wave-5 discussion. The user wants
to bank hybrid Git workspaces for a future step and move the storage foundation
toward chunked content. This note proposes a 64 KiB starting point; it does
not freeze a format, implement a migration, or claim performance results.
Read with the [Wave 5 handoff](wave-5-handoff.md) and the
[engine/admission study](wp6-build-sync-juicefs-study.md).

## 1. Banked feature: hybrid Git workspaces

Support both cloning an existing prepared workspace and creating a lazily
populated workspace from a Git commit. Conceptually the visible directory is
a checkout-correct Git base plus private changes, deletions, untracked files
and ignored files. Forking a prepared workspace retains build outputs and
caches to make incremental builds possible; path-sensitive tools may still
invalidate them. Preserve relevant timestamps and verify actual builds.

Git continues to own commits, refs, staging, merges and rebases. TKFS owns
the directory view, COW content, snapshots, local durability and retention.
A narrow adapter reads Git data and establishes valid worktree bookkeeping.
HEAD, index and working directory remain distinct: a clone may need to retain
different staged and unstaged versions, including an unresolved index.
Git [worktrees](https://git-scm.com/docs/git-worktree) have per-worktree
administrative state as well as shared repository state. Do not blindly clone
live lockfiles or machine-specific registration paths.

Git blobs are not invariably working-directory bytes: honor checkout filters,
line endings, modes and symlinks; see
[gitattributes](https://git-scm.com/docs/gitattributes). Initially let Git
perform checkout/reset/merge through the filesystem. A HEAD change alone must
not silently replace a dirty view. A Git object ID is not a TKFS content hash.

Pin the base and the overlay snapshot before exposing the fork. Snapshotting
pending writes needs an explicit checkpoint boundary; a filesystem snapshot
does not by itself make a live build application-consistent. Retain base bytes
against Git GC or import them into a TKFS-owned store. A lazy view is only
offline-readable to the extent its content is locally retained.

Zero-copy here means no copying of unchanged payload when forking. It does
not promise zero copies through kernel/userspace, hashing or encrypted I/O.
Share namespace structure as well as content to avoid an O(file count) metadata
copy on every fork. Existing disposable startup snapshots are not automatically
suitable retained clone roots. Git integration is deferred; the content format
should not require Git or understand programming-language syntax.

## 2. Initial chunk policy

### Git-enabling foundation choices

The following seams would give the deferred Git feature a practical advantage
without implementing Git in the storage engine:

1. **Separate content identity from its local provider.** Resolve an immutable
   file version through a bounded read interface. Today its provider is TKFS
   CAS; later it may be a retained Git blob or a cached checkout conversion.
   Git object ID plus hash algorithm and repository/provider scope identifies
   a source, not a TKFS chunk digest or peer sharing authorization. Keep local
   provider locators out of portable peer descriptors. Establish canonical
   manifests/verified bytes before sharing through the TKFS object protocol.
2. **Keep writable overlays independent of the base provider.** Represent
   untouched base ranges and private dirty ranges separately. The same write
   session can later overlay Git-backed content without first copying the
   entire file. Keep the representation capable of ordered extents with lengths
   and offsets, even if the first persisted policy uses aligned 64 KiB chunks.
   This leaves room for CDC and shared subranges later. An ordinary filesystem
   write does not announce a semantic text insertion; do not claim range
   descriptors alone detect or preserve suffix reuse after an editor rewrite.
3. **Make immutable namespace roots a first-class future boundary.** Workspace
   identity, root generation and file identity must not collapse into absolute
   host paths. Share base directory structure and path-copy changed entries.
   Resolve normal lookups through current indexed state, not history replay.
   A mutable Git HEAD is not an immutable root identity; snapshot it to a
   selected commit and preserve independent index/overlay state.
4. **Maintain a trustworthy workspace change journal.** Record local and remote
   changes, deletes, renames and directory effects with resumable tokens.
   Overflow, restart or an expired token must permit a full-rescan answer.
   This can later support Git's fsmonitor integration and ordinary watcher/cache
   invalidation. Correct size/timestamps/file identity and stat invalidation
   remain necessary; never report a changed file as clean to accelerate Git.
5. **Cache verified Git-to-TKFS conversion results outside causal metadata.**
   Reuse a blob-to-manifest mapping across workspaces when the bytes are truly
   identical. Checkout-converted bytes need path/attribute/config/filter-context
   awareness; arbitrary external filters may be stateful or nondeterministic,
   so they cannot always use a persistent mapping. Git index/HEAD/object IDs
   must not become the identity or merge rules of TKFS file revisions.
6. **Make retention and hydration explicit.** A provider needs a lifetime/pin
   contract and must distinguish known content from locally readable content.
   Retain a Git source against GC/repacking or import it. Partial clone and
   external filter content can be unavailable offline. Preserve workspace,
   conflict and pending-save roots independent of mutable Git branch names.

Git pack entries can be compressed and delta encoded; they are not normally
directly mappable file byte ranges. A logical provider read may reconstruct an
entire blob before slicing/caching it. Start with supported batched access
([git cat-file](https://git-scm.com/docs/git-cat-file)) rather than implementing
a pack decoder; see [pack format](https://git-scm.com/docs/gitformat-pack).
Git's [fsmonitor support](https://git-scm.com/docs/git-update-index) provides a
future consumer of a reliable change journal; it is not implemented here.

The immediate design priority is the provider/overlay/identity boundary and
correct invalidation. A Git provider, fsmonitor adapter and persistent tree
implementation remain separately scoped work. No special Git chunk size exists
to align with: choose 64 KiB on TKFS workload evidence, not Git pack layout.

### Candidate size and source sample

Prototype fixed **64 KiB (65,536 byte)** logical chunks with variable-length
last chunks and no padding. Files up to that size use one direct content
reference; larger files use a versioned manifest. Empty files have an explicit
canonical representation. Keep size/algorithm in the format so a later policy
does not reinterpret existing objects. The single-object threshold and chunk
size are separate tuning parameters even if initially equal.

This balances changed-byte amplification and demand latency against object
counts. It is a hypothesis, not an established optimum. A 4 KiB in-place edit
contained in one full chunk requires at most 64 KiB of replacement payload
plus manifest work; a boundary-crossing edit touches multiple chunks. This
does not imply all saves are in-place: editors often replace whole files.

For text, inserting bytes near the start shifts fixed boundaries and can
invalidate the remaining chunks. Deduplication still recognizes unchanged
chunks after a full rewrite when their boundaries and bytes remain equal, but
finding them may require hashing the rewritten stream. Content-defined
chunking (CDC) is a later alternative with bounded minimum/target/maximum
sizes; it can regain reuse after insertions, at additional CPU, indexing and
incremental-update complexity. Do not promise fixed chunks give delta-sized
transfers for arbitrary source edits, or use line boundaries as storage units.

Read-only sample of this checkout at master b1b8e52: `git ls-files` for
`.rs .cs .c .h .cpp .hpp .js .ts .tsx .jsx .py .zig .go`, then filesystem lengths
of existing listed files. 188 files, 7,680,404 bytes; no build artifacts or
untracked files included. Chunk counts below count at least one per file and
are reference counts before deduplication, not measured objects on disk.

| Fixed size | Files fitting one object | Total chunk references |
|---|---:|---:|
| 16 KiB | 80 | 569 |
| 64 KiB | 143 | 248 |
| 256 KiB | 187 | 189 |

This is a small local source sample, not the user's Visual Studio workload or
a throughput benchmark. Most sampled files would remain single objects at
64 KiB. Small-file build performance therefore still depends on caching,
callback cost, metadata work and durability batching.

## 3. Representation and execution contracts

- Use a typed, versioned descriptor distinguishing legacy whole-file content
  from chunk manifests. A manifest root digest is not the SHA-256 of the
  reconstructed file. Preserve existing identities; capability negotiation,
  export/materialization and mixed-version behavior require explicit design.
  Avoid retaining a mandatory whole-file hash on every new-format edit, which
  would recreate the full-read cost; legacy export may need that work.
- Canonically encode ordered chunk references, lengths and total length;
  validate bounds, type, depth and arithmetic. Independently verify immutable
  chunks with cryptographic hashes. Serve a demanded verified range without
  requiring every other chunk of the file. Small files avoid an extra manifest
  lookup. Bound manifest size; large files eventually need indexed immutable
  manifest pages so updates do not rewrite a huge flat list. At 64 KiB, a
  1 GiB file has 16,384 references (512 KiB of SHA-256 digests alone).
- Track dirty ranges in a private writable session. Reads combine the immutable
  base and pending writes. Avoid copying the full base merely to begin a write.
  Finalization hashes changed chunks and reuses known unchanged references.
  Partial overwrite of an absent base chunk requires its untouched bytes;
  full-chunk overwrite need not fetch that chunk. Truncation, extension, holes,
  mmap, append, rename and open-handle behavior require adapter-specific tests.
- Prepare chunks outside metadata coordination, establish the promised local
  durability, then atomically commit the new file descriptor/event and durable
  admission work. Crash recovery must not expose a committed local revision
  whose required bytes were never durable. Persist pending save ownership and
  generations. Revisit Wave 5's burst-close durability contract explicitly.
- Remote namespace activation may precede content hydration. Fetch manifests
  as bounded demand-critical structure when necessary; a committed authorized
  root admits its validated descendants. Never require a complete manifest
  traversal or content download for unrelated metadata to converge. Specify
  the lane/budget for manifests without smuggling file payload onto control
  lanes or allowing a manifest backlog to starve causal metadata.
- Hash knowledge alone never authorizes a chunk transfer. Admission follows a
  valid shared reference and its verified manifest ancestry, scoped to the
  requesting peer. Deduplication does not merge permission scopes. Missing
  manifest structure, denied access, absent bytes and corruption are distinct
  states; no routine readiness wait should become an unrelated failsafe.
- Coordinate mutations by workspace/entity and affected namespace invariants;
  join installs by content identity. Generation-check late completions and
  give demand actual slots across repositories. A canceled reader must not
  cancel another reader's shared transfer. Chunk boundaries are scheduling
  opportunities, not a latency guarantee under a saturated unbounded queue.
- File versions remain atomic conflict units unless a separate explicit merge
  rule applies. Two concurrent writes to different chunks do not prove a
  semantically valid combined source or binary file.
- Pin manifests and chunks for workspaces, snapshots, conflicts, pending saves
  and promised offline availability. Local GC must distinguish disposable
  cache from retained content and cannot infer global unreachability from an
  absent peer. Cross-peer global reference counting is not the default design.

## 4. Chunk size is not request, transaction or storage-container size

Do not create one RPC, SQLite commit or durability barrier per 64 KiB chunk.
Batch requests, stream verified chunks over existing bounded sessions, and
group compatible durable installations while preserving flush ordering.
Demand should interrupt background work at bounded scheduling opportunities.

Small immutable objects can create host-filesystem overhead. Measure loose
objects first; consider locally packed storage with an index only when justified.
Physical packing must preserve independent identity, verification, reads and
GC, and must not force a full-pack download for one requested chunk. Disk packs,
network batches, manifest pages and logical COW chunks can have different sizes.

JuiceFS's [I/O model](https://juicefs.com/docs/community/internals/io_processing/)
uses separate logical chunks, written slices and physical blocks (normally
4 MiB). Its range layering and batching are useful lessons; its default block
size is not evidence that TKFS should use 4 MiB for source-oriented COW.

## 5. Implementation sequence and selection evidence

1. Specify descriptor identity, durable save/publication ordering, manifest
   admission, retention and old-peer behavior before changing event formats.
2. Prototype the small-file path and 64 KiB fixed chunks in isolation. Compare
   16/64/256 KiB on the same corpus, including real source and build outputs.
3. Add dirty-range saves, verified range reads, shared-reference forks and
   batching through the existing engine boundaries. Full namespace clones and
   Git integration remain later features, not prerequisites for this prototype.
4. Test crash points, incomplete hydration, concurrent saves, cancellation,
   truncation, delete/rename, corrupt manifests, private shared chunks and GC.
   Reuse the existing causal/security invariants and deterministic fault tests.
5. Compare small in-place writes, beginning/middle insertions, atomic editor
   replacement, append/truncate, large partial binary updates, cold demand reads
   and native versus mounted clean/incremental builds. Record bytes hashed,
   read, written and transferred; object/reference counts; CPU; transaction and
   fsync counts; metadata latency; demand queue time; clone time and disk growth.
   Evaluate CDC only against insertion/rewrite evidence that can justify it.

Keep the initial policy configurable in experiments, with representation
parameters recorded immutably per descriptor. Choose the production default
from measurements; never silently reinterpret old manifests after tuning.
