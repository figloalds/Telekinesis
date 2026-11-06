# Configurable object cache

Status: draft for the owner's approval, 2026-11-06.

## Why

`PackStore` already keeps verified objects in an always-on 256 MiB LRU per folder
(`CACHE_BYTES`, `src/content/local.rs`). It cannot be sized, and its miss path reads and
hashes while holding the one mutex that also guards the location cache, the pending
writes and `flush_locked`'s fsync and FULL commit. Bulk readers (peer serving, GC) also
push whatever they read through it.

This design makes the size a per-folder setting and fixes those two behaviours. It is a
change to an existing cache, not a new one.

### What it will and will not speed up

Measured on 2026-11-06. Raw data, scripts and the trace patch are in
`test-evidence/object-cache-rebuild-20261106/` (local, gitignored).

Clean and rebuild of `ErpSoftLeader.sln` (43 projects, stale copy, one project fails on a
missing `webui.zip` natively too) inside a real mount, in Windows Sandbox with 32 vCPUs,
NuGet and the SDK outside the mount. Median of 3 rotated laps. Laps at one size differ by
up to about 6 s, so smaller gaps are noise.

| Cache | Build after clean | Warm `--no-incremental` rebuild | Byte hits, build | Byte hits, rebuild |
|---:|---:|---:|---:|---:|
| 0 | 49.0 s | 57.8 s | 0% | 0% |
| 16 MiB | 46.3 s | 56.3 s | 51% | 55% |
| 64 MiB | 50.5 s | 55.4 s | 75% | 89% |
| 256 MiB | 47.9 s | 55.2 s | 81% | 94% |
| 1 GiB | 49.0 s | 56.7 s | 74% | 94% |

Native on the same guest: 44.1 s after clean. The build touches only 66 to 74 MiB of
distinct objects, and at 256 MiB it spends 2.2 s of summed thread time in `get` (3.5 s at
0). **So this design is not expected to make builds faster**, and clock-sweep gains nothing
here (replay: identical hit rates at 128 MiB and above).

Where it does matter, measured on the real `local.rs` with no mount:

- Re-reading a file that fits: 1.4 GiB/s on misses, 14 GiB/s on hits. A file larger than the
  cache gets no hits under any policy, which is why the size is worth exposing.
- 8 threads, misses: 1.3 GiB/s today, 10.3 GiB/s with read and hash outside the mutex. Hit
  p99 under a 90/10 hit/miss mix: 583 µs today, 63 µs with that change.
- During the real first build, hits are 1.4 µs at the median but 1.2 to 1.8 ms at p99 and up
  to 7 ms: they queue on the pack mutex. Small in total, but it is the same defect.
- A peer pull running alongside foreground reads: simulated hits at 256 MiB fall from 55%
  to 42%, or 27% with a faster pull. Not admitting bulk misses keeps them at 55%.

These are upper bounds. The mounted benefit for large files is unmeasured, and that run must
stay in the isolated VM while W6-P0 is open.

## Goals

- A per-folder cache size, default 256 MiB, changed in the GUI only while the folder is
  stopped, the same way as its mount path.
- A miss does not block other readers, and a hit does not wait on a miss or a pack flush.
- Peer serving and GC do not evict what local readers are using.
- Hit counters in `mount-stats`, so this needs no patched binary to measure again.

## Non-goals

- Faster builds (see above). Peer fetch speed: a hit saves 1.1% of end-to-end CPU, because
  chunk bytes cross each hop as JSON number arrays at 3.57x. That needs binary object bodies,
  a separate protocol change.
- Clock-sweep or a partitioned table. Revisit only if a mounted run, after this change, still
  shows hits contending.
- A pool shared between folders: workers are separate processes.
- Caching whole packs: at 256 MiB they load 150x the bytes requested, and pack records have no
  headers.
- Persisting the cache, changing kernel cache policy, or a settings screen on Android.

## Anti-goals

Any of these makes the change a failure even if it is faster:

- Returning bytes that were not verified against their digest.
- A dead object readable after GC removed it.
- Deleting the index row of a healthy object because a stale read failed.
- Any dependency on I/O or handles in the served namespace (W6-P0).

## Decisions

The owner's, 2026-11-06:

- Default near 256 MiB, not 0, because caching already exists.
- Set per folder in the GUI, only while dismounted, like the mount path.
- Cache whichever of objects or packs gives the most benefit. Measured: objects.
- Build the setting, off-lock reads and bulk non-admission; leave clock-sweep out unless a VM
  run shows it is needed.

My calls, for the owner to check:

- **Minimum 64 MiB, not a true 0.** `MAX_OBJECT_BYTES` is 64 MiB, so every object fits. At a
  true 0 a 4 KiB read of a 64 GiB file takes 22 ms, because each call re-hashes its manifest.
  Proposed in chat, no objection raised; this table's 0 row shows builds would survive, but
  large files would not.
- **Maximum 65,536 MiB**, only to catch typing mistakes.
- **Android defaults to 64 MiB**, with no UI. 256 MiB per folder is a lot for a phone.
- **A new catalog table, not a column.** `Catalog::open` requires an exact schema version and
  has no migration, so a new column would need one; a table created by the existing
  `CREATE TABLE IF NOT EXISTS` batch needs no version bump and old builds ignore it.
- **GC keeps reading every live chunk, without admitting it.** Skipping those reads looks free
  but is not: today a corrupt chunk loses its index row and fails one GC run, and the next run
  proceeds. Without the read, `collect` would hit `OBJECT_CORRUPT_DURING_GC` on every run, which
  removes no row, until a foreground read happened to heal it. This keeps today's behaviour.
- **The missing-object scan counts as peer serving.** It reads each present manifest, so it stops
  admitting them too.
- **The setting travels on the existing `configure-mount` op**, since it shares the dialog,
  the stopped-only check and the save.
- **The VM rebuild laps are a regression check before merging; the large-file run is a follow-up
  measurement** with no pass or fail, under the owner's standing instruction to leave performance
  gates for last. Clock-sweep stays out of this work whatever that run shows; it would be a new
  decision. Both use Windows Sandbox, where the baseline was taken.

## Design

### 1. The setting

- Storage: `CREATE TABLE IF NOT EXISTS folder_cache(folder TEXT PRIMARY KEY, mib INTEGER NOT
  NULL)` in the catalog's `SCHEMA`. No row means the default. Removing a folder removes its
  row. `FolderRecord` gains `object_cache_mib: Option<u32>`.
- Hub: `configure_mount` accepts an optional `object_cache_mib`. Absent leaves it unchanged. It
  goes through the existing `Phase::Stopped | Phase::Failed` check (`FOLDER_RUNNING`). Out of
  range is `INVALID_OBJECT_CACHE_SIZE`.
- Launch: `LaunchSpec` gains `object_cache_mib`, filled from the record where `mount` is
  (`folder.rs`). `worker_args` adds `--object-cache-mib N` when set; `Command::Daemon` takes it.
- Open: `Engine::open` and `Store::open` keep their signatures and use the default. Add variants
  taking a size, down to `PackStore::with_limits`, which already takes one. The worker must open
  with the size first: the `STORES` registry returns the existing store to later openers, so
  the first limit wins. `embedded.rs` passes it too.
- Default: one function, 256 MiB, or 64 MiB on `target_os = "android"`.
- GUI: a "Object cache (MiB)" field in the `dialog == "mount"` branch of `ui/app.slint`, not in
  the create dialog. It reaches that dialog only through the existing `stop-first` gate in
  `gui_adapter.rs`. Blank means the default. The mount-only save label becomes "Save".

### 2. Reads outside the mutex

The LRU moves out of `State` into its own mutex, keeping today's algorithm. Lock order: `State`
then cache, and neither is held during I/O.

`get`:

1. Cache lock: on a hit, update recency and return.
2. `State` lock: find the location and the reader, and note `gc_generation`. Release.
3. Read and hash with no lock held.
4. On success, cache lock: admit only if `gc_generation` is unchanged. Return the bytes.
5. On failure, `State` lock: if the digest still has the same location, treat it as corrupt as
   today, but delete with `WHERE digest=? AND segment=? AND offset=?`. If it moved, retry once
   from step 2.

`collect` increments `gc_generation` under the cache lock when it sweeps dead objects, so a read
that began earlier cannot put one back. An in-flight reader keeps its `Arc<File>`, so a segment
removed meanwhile still reads correctly; its digest is checked as usual.

`read_range` asks for each chunk once. Today `ensure` calls `get` to test presence and the copy
loop calls `get` again, hashing twice when the object is not cached. It keeps the fetched `Arc`s
instead. The behaviours to preserve: missing chunks go to the fetcher together as one batch, and
a corrupt chunk is re-fetched rather than failing the read.

`verified_reads` becomes atomic. Existing tests that count it must keep passing.

### 3. Bulk readers

Add `PackStore::get_bulk`: a hit returns without updating recency, and a miss is read and verified
but not admitted.

- `Service::fetch` in `orchestrator/sync.rs` uses it.
- The missing-object scan in the same file uses it to read manifests, through a variant of
  `LocalContent::chunks` that never falls through to the fetcher. It already skips manifests that
  are absent or unreadable, and that stays.
- `Store::live_objects` uses it for every kind, so a corrupt chunk is still found and removed
  exactly as today.
- Everything else stays on `get`: `read_range`, seals, tree loading and previews.

### 4. Counters

`mount-stats` gains an `object_cache` row beside `namespace_cache`: limit, bytes used, hits, misses
and bulk reads. Counters are relaxed atomics.

## Testing

Host tests, no mount:

- The setting round-trips through the catalog, refuses a running folder, rejects out-of-range
  values, survives restart, and reaches `worker_args`. An existing catalog opens unchanged.
- A failed read after GC moved the object retries and succeeds, and the row stays. A real
  corruption at the same location still removes it and re-fetches.
- A read in flight across `collect` does not make a dead object readable.
- `read_range` verifies each chunk once per call, and still batches its fetch and self-heals.
- `get_bulk` leaves the cache's contents and order unchanged, for `fetch`, the missing-object scan
  and GC.
- A corrupt live chunk fails one GC run and loses its row, and the next run proceeds, as today.
- Many threads doing hits, misses, puts, flushes and GC together, checking every byte.

In Windows Sandbox only, bounded as `AGENTS.md` requires:

- Before merging, repeat the rebuild laps at the default with an unpatched binary. Accept if wall
  time is within the noise above, the build gives the same result as the baseline, every stop is
  graceful, and the new counters are within 10 points of the measured hit rates at 256 MiB: 67% of
  requests on the first build and 95% on the rebuild. Exact equality is not expected, because two
  threads missing one digest now both count as misses.
- As a follow-up, the first mounted measurement of the large-file case: re-read a file that fits,
  one that does not, and read during a concurrent peer pull. This shows whether step 2 pays off
  through a mount. It needs a small new harness; `guest.ps1` in the evidence folder only covers the
  rebuild laps.

Neither run closes W6-P0.

## Left to the builder

Names and module layout; whether the cache becomes its own type; the exact GUI wording and
validation messages; whether `manage` also exposes the setting on the command line.

## Separate leads this work surfaced

Not in scope, recorded because they are larger than anything above:

- During a warm rebuild `SetBasicInfo` costs 31.6 ms per call against 1.8 ms after a clean: 175 s
  of summed thread time, against 7.4 s for reads. Setting a modified time takes `seal_guard()`
  (`set_metadata` in `engine.rs`). The guard is a suspect, not a proven cause.
- Seals hash and discard 12 GiB of deduplicated output per build, about 5 s of CPU.
- GC hashes every live chunk while holding `Mutex<Store>`. Removing that needs a decision on what
  `collect` does with a corrupt chunk it is relocating, including how a read error differs from a
  digest mismatch, since the segment is deleted right after.
- Binary bodies for object transfer (see Non-goals).
