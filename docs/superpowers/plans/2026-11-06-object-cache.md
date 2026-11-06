# Configurable Object Cache Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `PackStore`'s object cache a per-folder setting (default 256 MiB, changed only while stopped), stop reads and hashes from holding the pack mutex, and stop peer serving and GC from evicting what local readers use.

**Architecture:** The LRU moves out of `State` into its own mutex, keeping today's algorithm. Misses read and hash with no lock held, guarded by a generation counter and a location re-check. The size travels the same route as the mount path: catalog, hub, `LaunchSpec`, worker flag, store open.

**Tech Stack:** Rust 2024, std threads and blocking I/O, rusqlite, clap, Slint. No new dependencies.

**Spec:** `docs/superpowers/specs/2026-11-06-object-cache-design.md`. Executors read both; the spec holds the evidence and the reasons.

## Preconditions

**Do not start until the Android work has landed.** The owner set this order on 2026-11-06. Then:

- Branch from wherever Android landed, in a worktree at `C:/Users/felyp/Desktop/Projetos/Telekinesis-wt/object-cache` with `CARGO_TARGET_DIR=C:/tkfs-target/object-cache`.
- This plan names symbols, not line numbers, because Android is still moving those files. Confirm they exist:
  `rg -n "CACHE_BYTES|fn ensure|fn read_range|fn live_objects|fn configure_mount|fn worker_args|struct LaunchSpec|mount-stats" src`
- Search for readers added since 2026-11-06: `rg -n "packs\.get\(|\.chunks\(" src android/native/src`. Any new one that serves peers or walks all objects follows Task 4's rule; any other stays on `get`. Any new `.ensure(` caller moves to `held` in Task 3.

## Global Constraints

- Default 256 MiB; 64 MiB on `target_os = "android"`. Range 64 to 65,536 MiB, enforced by the hub and the worker flag. Out of range is `INVALID_OBJECT_CACHE_SIZE`.
- `with_limits` still accepts any limit, including 0. The minimum is a rule on the setting, not on the cache.
- `CREATE TABLE IF NOT EXISTS folder_cache(folder TEXT PRIMARY KEY, mib INTEGER NOT NULL)`. `SCHEMA_VERSION` stays 1. No row means the default.
- Only `Phase::Stopped | Phase::Failed` may change the size; anything else is `FOLDER_RUNNING`.
- Lock order is `State`, then cache. Nothing takes `State` while holding the cache lock. Neither is held during a `get`'s read and hash.
- Never return bytes not verified against their digest. A dead object must not be readable once `collect` returns. A stale read failure must never delete an index row. No I/O or handles in the served namespace.
- Out of scope: clock-sweep, whole-pack caching, persistence, a pool shared between folders, admitting on `put`, and the separate leads at the end of the spec.
- **No mounts on the host.** Host tests stay headless. Mounted runs happen only in Windows Sandbox, every step bounded; a cleanup timeout or stranded process fails the run and stops further mounts (`AGENTS.md`, W6-P0).

## Review Focus

1. An object exactly at, or just over, the limit: at the limit it fits and evicts the rest; over it, it is not admitted and evicts nothing. Test in Task 1.
2. Two threads missing one digest at once: its bytes are counted once and `bytes_used` never drifts. Tests in Tasks 1 and 2.
3. A file that repeats an absent chunk inside one read: the fetcher is asked once per digest and the bytes come back in the right places. Test in Task 3.
4. Typed sizes such as blank, `" 128 "`, `63`, `65537`, `abc`, `-1`, `128.5`, `1e3`: each is accepted or refused clearly, and a refusal changes nothing, including the mount chosen in the same save. Tests in Tasks 5 and 7.
5. An existing install: a catalog from today's build opens with its folders intact and at the default, and a removed folder leaves no row. Tests in Task 5.

---

### Task 1: Object cache under its own lock, with counters

**Files:**
- Modify: `src/content/local.rs`
- Modify: `src/orchestrator/control.rs`

**Interfaces:**
- Produces, in `local.rs`:
  ```rust
  struct ObjectCache { entries: BTreeMap<Digest, Cached>, order: BTreeSet<(u64, Digest)>,
                       len: usize, stamp: u64, limit: usize, generation: u64 }
  impl ObjectCache {
      fn hit(&mut self, digest: &Digest) -> Option<Arc<Vec<u8>>>;   // updates recency
      fn peek(&self, digest: &Digest) -> Option<Arc<Vec<u8>>>;      // leaves order alone
      fn admit(&mut self, digest: Digest, bytes: &Arc<Vec<u8>>, seen_generation: u64) -> bool;
      fn sweep(&mut self, live: &BTreeSet<Digest>);                 // drops non-live, generation += 1
  }
  #[derive(Clone, Debug, Serialize)]
  pub struct CacheStats { pub limit: usize, pub bytes_used: usize,
                          pub hits: u64, pub misses: u64, pub bulk_reads: u64 }
  impl PackStore { pub fn cache_stats(&self) -> CacheStats; }
  ```
- `admit` changes nothing and returns false when `seen_generation` differs, the digest is present, or `bytes.len() > limit`. Otherwise it evicts oldest-first until the object fits.

- [ ] **Step 1: Write the failing tests** in `local.rs`'s test module

```rust
fn cached(n: usize) -> Arc<Vec<u8>> { Arc::new(vec![n as u8; n]) }

#[test] fn admit_counts_a_digest_once() { /* limit 16: admit d twice;
    second returns false; cache.len == 4 */ }
#[test] fn an_object_at_the_limit_fits_and_one_over_it_evicts_nothing() { /* limit 8:
    admit a(4); admit b(8) true, a gone, len 8; admit c(9) false, b still there, len 8 */ }
#[test] fn a_stale_generation_is_not_admitted() { /* admit(d, b, generation + 1) false, len 0 */ }
#[test] fn a_sweep_drops_dead_objects_and_moves_the_generation() { /* live {a}: b gone,
    a stays, len == a.len(), generation + 1 */ }
#[test] fn peek_leaves_eviction_order_alone() { /* limit 8, a(4), b(4):
    peek(a) then admit c(4) evicts a; repeat with hit(a): evicts b */ }
#[test] fn a_second_open_keeps_the_first_limit() { /* with_limits(root, 1024, SEGMENT_BYTES)
    then PackStore::open(root) while the first is alive: cache_stats().limit == 1024 */ }
#[test] fn counters_count_hits_and_misses() { /* put, flush, get, get: misses 1, hits 1,
    bulk_reads 0, bytes_used == len */ }
```

And in `control.rs`, `#[cfg(windows)] fn mount_stats_reports_the_object_cache`: `execute(&engine, &settings, "mount-stats", &json!({}))` has a row with `operation == "object_cache"` and `cache.limit == 256 * 1024 * 1024`.

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test --lib content::local`
Expected: does not compile, `ObjectCache` is not defined.

- [ ] **Step 3: Implement**

`State` loses `cache`, `order`, `cache_len`, `stamp` and `verified_reads`. `Inner` loses `cache_limit` and gains `cache: Mutex<ObjectCache>` plus relaxed `AtomicU64`s `verified_reads`, `hits`, `misses`, `bulk_reads`.

`get` tries `cache.hit` first, taking only the cache lock. A miss still reads under `State` in this task, and notes `generation` under `State` before reading.

`collect` calls `sweep(&live)` once, under `State` then the cache lock, right after extending `live` with the pins and before any index change. Remove the sweep at its end: `State` is held throughout, so no new reader finds a location meanwhile, and error paths are now covered too.

The `mount-stats` arm in `control.rs` pushes `{"operation":"object_cache","cache":<cache_stats>}` onto `crate::mount::stats()`.

- [ ] **Step 4: Run the tests**

Run: `cargo test --lib content::local` then `cargo test --lib`
Expected: PASS, including every existing GC, location-cache and `verified_reads` test, unchanged.

- [ ] **Step 5: Commit**

```bash
git add src/content/local.rs src/orchestrator/control.rs
git commit -m "✨Give the object cache its own lock and hit counters"
```

### Task 2: Read and hash outside the mutex

**Files:**
- Modify: `src/content/local.rs`

**Interfaces:**
- Consumes: `ObjectCache`, `Inner`'s counters (Task 1).
- Produces:
  ```rust
  /// Steps 2, 3 and 5 of the spec's `get`: the verified bytes and the generation noted before reading.
  fn read_verified(&self, digest: Digest, before_read: &mut dyn FnMut()) -> Result<(Arc<Vec<u8>>, u64)>;
  fn get_with(&self, digest: Digest, before_read: impl FnMut()) -> Result<Arc<Vec<u8>>>; // `get` passes a no-op
  ```
- `before_read` runs with no lock held. It is a test seam, in the style of `Service::have_with`.

- [ ] **Step 1: Write the failing tests**

Bounded: anything that could deadlock runs on a thread and reports over a channel with `recv_timeout`, so a regression fails instead of hanging.

```rust
#[test] fn a_read_in_progress_lets_hits_and_puts_through() {
    // limit 1 MiB. hot: put, flush, get. cold: put, flush.
    // store.get_with(cold, || { spawn a thread doing store.get(hot), store.put(Chunk, b"x"),
    //     store.flush(), sending the result; rx.recv_timeout(10 s).expect("blocked by a read") })
    // Returned bytes equal cold's.
}
#[test] fn a_failed_read_of_a_moved_object_retries_and_keeps_its_row() {
    // with_limits(root, 0, 16): keep, dead, roll, as cached_locations_follow_gc_relocation....
    // Open a write handle on keep's segment first (default share mode allows deletion).
    // store.get_with(keep, || { store.collect(&BTreeSet::from([keep]), 1.0).unwrap();
    //     write b"!" through the handle at keep's old offset; })
    assert_eq!(bytes.as_slice(), b"keep me!");
    assert!(store.contains(keep)?);
    // Drop, PackStore::open again: get(keep) still returns the bytes.
}
#[test] fn a_read_in_flight_across_collect_does_not_cache_a_dead_object() {
    // with_limits(root, 1024, 8): dead, roll.
    // let got = store.get_with(dead, || { store.collect(&BTreeSet::new(), 1.0).unwrap(); })?;
    assert_eq!(got.as_slice(), b"dead me!");            // verified, already in flight
    assert_eq!(store.cache_stats().bytes_used, 0);
    assert!(!store.contains(dead)?);
    assert!(store.get(dead).unwrap_err().to_string().contains("OBJECT_MISSING"));
}
#[test] fn hits_misses_puts_flushes_and_gc_together_return_only_verified_bytes() {
    // limit 256 KiB, segment 64 KiB, objects 1 to 4 KiB derived from an index.
    // 8 threads, fixed iteration count: get a live or churn digest and assert
    // Digest::bytes(&got) == digest; put churn objects; flush.
    // One more thread loops collect(&live, 1.0).
    // Only churn digests may error, and only with OBJECT_MISSING.
    // Main thread joins through recv_timeout(120 s).
    // Afterwards every live digest reads back, bytes_used <= limit, and bytes_used equals
    // the sum of the cache's entries.
}
```

`flat_manifest_ranges_and_corruption` already pins that real corruption removes the row and that a re-put heals it. It must keep passing unchanged.

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test --lib content::local`
Expected: does not compile, `get_with` is not defined.

- [ ] **Step 3: Implement `read_verified`**

```text
attempts = 0
loop:
  lock State: location (else OBJECT_MISSING), reader (a first-time File::open stays under
    this lock: one per segment, rare), then generation via the cache lock. Release.
  before_read()
  read and hash with no lock
  ok: count a verified read; return (bytes, generation)
  failed: lock State; look the location up again
    none:          OBJECT_MISSING
    same location: clear the location caches, DELETE ... WHERE digest=? AND segment=?
                   AND offset=?, remove from pending, bail OBJECT_CORRUPT (as today)
    moved:         attempts += 1; if attempts == 2 bail "OBJECT_MOVED: {digest}", deleting
                   nothing; else go round again
```

`Location` derives `PartialEq`. `get_with` tries `cache.hit` (counting a hit), calls `read_verified` (counting a miss), then `admit`s with the returned generation. Hits and misses are counted here, not in `read_verified`, because Task 4's `get_bulk` shares it.

- [ ] **Step 4: Run the tests**

Run: `cargo test --lib content::local` then `cargo test --lib`
Expected: PASS. Run the stress test 20 times to catch flakes:
`cargo test --lib hits_misses_puts_flushes -- --test-threads=1`, repeated.

- [ ] **Step 5: Commit**

```bash
git add src/content/local.rs
git commit -m "✨Read and hash objects outside the pack lock"
```

### Task 3: `read_range` verifies each object once

**Files:**
- Modify: `src/content/local.rs`

**Interfaces:**
- Consumes: `get` (Task 2).
- Produces:
  ```rust
  impl LocalContent {
      /// The bytes of `objects`, in order. Absent or corrupt ones go to the fetcher together,
      /// each digest once, then are read again. No fetcher: OBJECT_MISSING.
      fn held(&self, objects: &[(Digest, Kind)]) -> Result<Vec<Arc<Vec<u8>>>>;
  }
  #[cfg(test)] impl PackStore { pub(crate) fn corrupt_on_disk(&self, digest: Digest) -> Result<()>; }
  ```
- `LocalContent::ensure` is removed. `object` and `read_range` use `held`.

- [ ] **Step 1: Write the failing tests**

A `StoringFetcher` test helper holds a `PackStore`, a map of good bytes, and every request it receives; `fetch` records the request and `put`s the bytes.

```rust
#[test] fn read_range_verifies_each_object_once_per_call() {
    // with_limits(home, 0, SEGMENT_BYTES), then LocalContent::open(home) (same store).
    // import 2 full chunks + 9 distinct bytes; flush.
    // read_range(&reference, CHUNK_SIZE - 1, &mut [0; 2]) crosses chunks 0 and 1.
    assert_eq!(packs.verified_reads() - before, 3); // manifest + 2 chunks; 5 today
}
#[test] fn a_repeated_absent_chunk_is_fetched_once_and_reads_back() {
    // Manifest [a, b, a] with a and b absent; StoringFetcher holds them.
    // Read all 3 chunks.
    assert_eq!(*asked, vec![vec![a, b]]);
    // Output equals a's bytes + b's bytes + a's bytes.
}
#[test] fn a_corrupt_chunk_is_fetched_again_within_the_same_read() {
    // Import, flush, corrupt_on_disk(chunk); StoringFetcher holds the good bytes.
    // read_range succeeds with the right bytes; asked contains chunk.
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test --lib content::local`
Expected: does not compile (`corrupt_on_disk`), or `verified_reads` is 5.

- [ ] **Step 3: Implement `held` and move `read_range`'s copy loop onto its result**

The copy loop indexes the `Vec` by chunk position; `INVALID_CHUNK_LENGTH` stays. Refactor `flat_manifest_ranges_and_corruption` to use `corrupt_on_disk`.

- [ ] **Step 4: Run the tests**

Run: `cargo test --lib`
Expected: PASS, including `truncate_keeps_lazy_immutable_ranges_unhydrated` and the `hint_range` tests.

- [ ] **Step 5: Commit**

```bash
git add src/content/local.rs
git commit -m "✨Verify each object once per ranged read"
```

### Task 4: Bulk readers do not fill the cache

**Files:**
- Modify: `src/content/local.rs`, `src/orchestrator/sync.rs`, `src/core.rs`
- Test: `src/invariants.rs`

**Interfaces:**
- Consumes: `read_verified`, `ObjectCache::peek`, `corrupt_on_disk`.
- Produces:
  ```rust
  impl PackStore {
      /// A hit leaves recency alone; a miss is verified but not admitted. Counts bulk_reads only.
      pub fn get_bulk(&self, digest: Digest) -> Result<Arc<Vec<u8>>>;
  }
  impl LocalContent {
      /// `chunks` through `get_bulk`. Never asks the fetcher: absent is an error.
      pub fn chunks_bulk(&self, reference: &FileContentRef) -> Result<Vec<Digest>>;
  }
  ```

- [ ] **Step 1: Write the failing tests**

```rust
// local.rs
#[test] fn get_bulk_serves_without_touching_the_cache() {
    // A miss returns the bytes: bytes_used 0, bulk_reads 1, hits 0, misses 0.
    // limit 8: get(a), get(b) (4 bytes each), get_bulk(a), get(c): a was not refreshed, so
    // a is the one evicted (the next get(a) adds a miss).
}
#[test] fn chunks_bulk_never_asks_the_fetcher() {
    // Absent manifest with a RecordingFetcher: Err, fetches == 0.
}
// sync.rs, using stored_objects and fetch_reply
#[test] fn serving_a_fetch_leaves_the_object_cache_alone() {
    // stored_objects(&engine, 4, 1024, 0), note cache_stats, fetch_reply: all objects served.
    // bytes_used, hits and misses are unchanged; bulk_reads rose by 4.
}
#[test] fn the_missing_scan_leaves_the_object_cache_alone() {
    // Fixture of objects_missing_lists_unfetched_content_refs_then_demand_wakes_waiters: target
    // holds a present manifest; note cache_stats, call "objects.missing".
    // bytes_used and misses are unchanged. (Compare, do not assume 0: opening an engine may read trees.)
}
// invariants.rs
#[test] fn gc_leaves_the_object_cache_alone_and_still_finds_a_corrupt_chunk() {
    // Engine with a sealed multi-chunk file; note bytes_used; engine.collect(): unchanged.
    // corrupt_on_disk(chunk): first collect is Err containing OBJECT_CORRUPT, contains(chunk) false.
    // Second collect is Ok.
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test --lib -- get_bulk chunks_bulk leaves_the_object_cache_alone`
Expected: does not compile, `get_bulk` is not defined.

- [ ] **Step 3: Implement and move the three callers**

- `Service::fetch`: `content.packs.get_bulk(digest)`.
- The missing scan in `sync.rs`: `content.chunks(reference)` becomes `content.chunks_bulk(reference)`. Its `let Ok(..) else continue` stays.
- `Store::live_objects`: `self.content.packs.get_bulk(digest)`, for every kind.

- [ ] **Step 4: Run the tests**

Run: `cargo test --lib`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/content/local.rs src/orchestrator/sync.rs src/core.rs src/invariants.rs
git commit -m "✨Keep peer serving and GC out of the object cache"
```

### Task 5: The setting in the catalog and the hub

**Files:**
- Modify: `src/orchestrator/catalog.rs`, `src/orchestrator/hub.rs`, `src/orchestrator/folder.rs` (test literal only), `src/content/local.rs` (constants)

**Interfaces:**
- Produces:
  ```rust
  // local.rs
  pub const MIN_OBJECT_CACHE_MIB: u32 = 64;
  pub const MAX_OBJECT_CACHE_MIB: u32 = 65_536;
  // catalog.rs
  pub struct FolderRecord { /* ... */ #[serde(default)] pub object_cache_mib: Option<u32> }
  impl Catalog { pub fn set_object_cache(&self, id: &str, mib: Option<u32>) -> Result<()>; } // None deletes the row
  // hub.rs
  /// Absent: None (unchanged). Null: Some(None) (back to the default). Number: Some(Some(n)).
  fn object_cache_request(payload: &Value) -> Result<Option<Option<u32>>>;
  ```

- [ ] **Step 1: Write the failing tests**

```rust
// catalog.rs
#[test] fn the_object_cache_size_round_trips_and_survives_reopen() {
    // set 128, resolve Some(128); drop, Catalog::open, still Some(128);
    // set None: resolve None, and folder_cache has 0 rows.
}
#[test] fn a_catalog_without_the_cache_table_opens_unchanged() {
    // create_folder, DROP TABLE folder_cache, drop, Catalog::open:
    // the folder is identical, object_cache_mib None, folder_cache exists again.
    assert_eq!(SCHEMA_VERSION, 1);
}
#[test] fn removing_a_folder_removes_its_cache_row() {}

// hub.rs, using daemon()
#[test] fn configure_mount_stores_the_cache_size() {
    // Absent leaves it; 128 sets it; null resets it. The returned record shows each.
}
#[test] fn a_bad_cache_size_is_refused_before_anything_is_written() {
    // For 63, 65537, -1, 128.5, "128", true, each with a new mount in the same payload:
    // the error starts with INVALID_OBJECT_CACHE_SIZE and execute("list") is unchanged.
}
#[test] fn a_folder_that_is_not_stopped_keeps_its_cache_size() {
    // Daemon over NeverReady; start it (phase Starting):
    // configure-mount with 128 is FOLDER_RUNNING; the record stays None.
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test --lib catalog` then `cargo test --lib orchestrator::hub`
Expected: does not compile, `set_object_cache` is not defined.

- [ ] **Step 3: Implement**

- Catalog: add the table to `SCHEMA`. `all_folders` uses `LEFT JOIN folder_cache c ON c.folder = f.id ORDER BY f.rowid`. `remove_folder` deletes the row. `create_folder` and the test literals in `folder.rs` and `hub.rs` set `object_cache_mib: None`.
- Hub: `object_cache_request` must use `payload.get("object_cache_mib")`, since indexing a `Value` cannot tell absent from null. In `configure_mount`, parse with it next to `mount_path`, before any lock or write. Keep the phase check, and write the mount then the size. The error becomes `FOLDER_RUNNING: stop {} before changing its mount or object cache`.
- Refusal text: `INVALID_OBJECT_CACHE_SIZE: object_cache_mib must be a whole number from 64 to 65536`, built from the constants.

- [ ] **Step 4: Run the tests**

Run: `cargo test --lib`
Expected: PASS, including `mounts_are_stored_and_a_taken_mount_is_refused_before_any_catalog_write`.

- [ ] **Step 5: Commit**

```bash
git add src/orchestrator/catalog.rs src/orchestrator/hub.rs src/orchestrator/folder.rs src/content/local.rs
git commit -m "✨Store a per-folder object cache size"
```

### Task 6: The size reaches the worker

**Files:**
- Modify: `src/content/local.rs`, `src/core.rs`, `src/runtime/engine.rs`, `src/orchestrator/process.rs`, `src/orchestrator/folder.rs`, `src/orchestrator/embedded.rs`, `src/orchestrator/manage.rs`, `src/orchestrator/control.rs`, `src/main.rs`
- Test: `tests/orchestrator.rs`

**Interfaces:**
- Consumes: `FolderRecord.object_cache_mib`, the constants (Task 5), `cache_stats` (Task 1).
- Produces:
  ```rust
  pub fn default_object_cache_mib() -> u32;                    // 64 on target_os = "android", else 256
  pub fn object_cache_bytes(mib: Option<u32>) -> usize;        // mib or the default, in bytes
  impl LocalContent { pub fn open_with_cache(root: &Path, cache_bytes: usize) -> Result<Self>; }
  impl Store  { pub fn open_with_cache(root: &Path, repo: Option<&str>, cache_bytes: usize) -> Result<Self>; }
  impl Engine { pub fn open_with_cache(root: &Path, repo: Option<&str>, cache_bytes: usize) -> Result<Arc<Self>>; }
  pub struct LaunchSpec { /* ... */ pub object_cache_mib: Option<u32> }
  ```
- `CACHE_BYTES` is removed. The existing `open`s keep their signatures and use `object_cache_bytes(None)`.
- Worker status gains `"object_cache_mib"`, the limit in MiB. It works on every platform, unlike `mount-stats`.

- [ ] **Step 1: Write the failing tests**

```rust
// local.rs
#[test] fn the_default_size_follows_the_platform() {
    assert_eq!(default_object_cache_mib(), if cfg!(target_os = "android") { 64 } else { 256 });
    assert_eq!(object_cache_bytes(Some(128)), 128 << 20);
}
// process.rs
#[test] fn worker_args_carry_the_cache_size_only_when_set() {
    // None: no flag. Some(128): "--object-cache-mib", "128".
}
// engine.rs
#[test] fn open_with_cache_sets_the_limit() { /* 128 << 20 == cache_stats().limit */ }
// embedded.rs, with its existing spawn helper
#[test] fn an_embedded_worker_uses_its_spec_size() {
    // Some(128): the engine kept in shared.engines has limit 128 << 20.
}
// tests/orchestrator.rs
#[test] fn a_stopped_folder_takes_a_cache_size_for_its_next_start() -> Result<()> {
    // Headless, no mount. create "Project", wait Running:
    // inspect's worker.object_cache_mib == 256.
    // configure-mount Project --object-cache-mib 128 is refused with FOLDER_RUNNING.
    // stop; configure-mount Project --object-cache-mib 128; start; wait Running:
    // worker.object_cache_mib == 128, record.object_cache_mib == 128.
    // configure-mount --object-cache-mib 63: clap refuses it.
}
```

Extend `crash_loop_trips_breaker_and_retry_clears_it`: once the folder is `Failed` and before `retry`, `configure-mount <folder> --object-cache-mib 128` succeeds.

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test --lib -- worker_args open_with_cache the_default_size embedded_worker_uses` then `cargo test --test orchestrator`
Expected: does not compile.

- [ ] **Step 3: Implement**

- `folder.rs` and the `embedded.rs` test literal fill `object_cache_mib` from the record or `None`.
- `ThreadLauncher::spawn` calls `Engine::open_with_cache(&spec.state, None, object_cache_bytes(spec.object_cache_mib))`.
- `Command::Daemon` and `C::ConfigureMount` gain `#[arg(long, value_parser = clap::value_parser!(u32).range(64..=65536))] object_cache_mib: Option<u32>`. `main.rs` opens the daemon's engine with it.
- `manage.rs` adds `"object_cache_mib"` to the payload only when the flag is given, so omitting it leaves the size alone. There is no command-line reset to the default.
- `manage configure-mount` without `--mount` already means headless, so using the new flag alone would clear a folder's mount. Keep that meaning, and say so in the flag's help: "Give --mount as well to keep a mount point; without it the folder runs headless."

- [ ] **Step 4: Run the tests**

Run: `cargo test --lib` then `cargo test --test orchestrator --test docs_cli`
Expected: PASS. These workers are headless; none mounts.

- [ ] **Step 5: Commit**

```bash
git add src tests/orchestrator.rs
git commit -m "✨Open each folder's store with its object cache size"
```

### Task 7: The GUI field and the docs

**Files:**
- Modify: `src/desktop.rs`, `src/gui.rs`, `src/gui_adapter.rs`, `ui/app.slint`, `README.md`, `docs/ARCHITECTURE.md`

**Interfaces:**
- Consumes: `record.object_cache_mib`, `default_object_cache_mib()`; the hub's range check (Task 5).
- Produces:
  ```rust
  /// Blank: None, the default. A whole number: Some(n); the hub checks the range.
  /// Anything else: INVALID_OBJECT_CACHE_SIZE.
  pub fn object_cache_field(text: &str) -> Result<Option<u32>>; // desktop.rs
  ```
  ```slint
  in-out property <string> object-cache-mib;
  in property <string> default-object-cache-mib;
  callback configure-mount(string /*state*/, string /*mount*/, bool /*headless*/, string /*object cache MiB*/);
  ```

- [ ] **Step 1: Write the failing test** in `desktop.rs`

```rust
#[test] fn the_cache_field_reads_blank_and_whole_numbers_only() {
    assert_eq!(object_cache_field("").unwrap(), None);
    assert_eq!(object_cache_field("   ").unwrap(), None);
    assert_eq!(object_cache_field(" 128 ").unwrap(), Some(128));
    assert_eq!(object_cache_field("63").unwrap(), Some(63)); // the hub refuses it
    for bad in ["abc", "-1", "128.5", "1e3", "99999999999"] {
        assert!(object_cache_field(bad).unwrap_err().to_string().starts_with("INVALID_OBJECT_CACHE_SIZE"));
    }
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test --lib desktop`
Expected: does not compile, `object_cache_field` is not defined.

- [ ] **Step 3: Implement**

- `gui.rs`, `"configure-mount"`: the third arg (`c`) goes through `object_cache_field` before anything is sent, then into the payload as `"object_cache_mib"`. The GUI always sends it, so blank resets to the default. The non-headless done message becomes `"Saved; start the folder to apply it"`.
- `gui_adapter.rs`: on `configure-mount` set `object-cache-mib` from `entry["record"]["object_cache_mib"]`, blank when null; `on_configure_mount` takes the new argument and passes it as the third arg; set `default-object-cache-mib` where `set_default_mount_directory` is set.
- `ui/app.slint`: in the `create / mount path` branch, `if root.dialog == "mount":` a Label "Object cache (MiB)" and a Field with placeholder `root.default-object-cache-mib + " (default)"`, enabled by `root.idle`, also when Headless. Not in the create dialog. `"Save mount point"` becomes `"Save"`. Pass the field to `root.configure-mount`.
- `README.md`: extend the **Change mount point** row (the object cache, in the same dialog, folder stopped), and add `INVALID_OBJECT_CACHE_SIZE` to the error table.
- `docs/ARCHITECTURE.md`: "a bounded 256 MiB verified LRU" becomes a per-folder verified LRU, 256 MiB by default, that peer serving and GC do not fill.

- [ ] **Step 4: Verify**

Run: `cargo test --lib desktop` then `cargo build --bin tkfs`
Expected: PASS, and Slint compiles the bindings.

Run the app with a scratch config (`tkfs -f <scratch>\orchestrator.toml gui`) and a **Headless** folder, so nothing mounts, and check:

- While running, **Change mount point** gives the stop-first dialog.
- While stopped, the field shows blank with "256 (default)". Saving `128` and reopening shows `128`.
- Saving `63` or `abc` shows the refusal and changes nothing. Saving blank resets it.
- After start, `tkfs manage inspect <folder>` shows `worker.object_cache_mib`.

- [ ] **Step 5: Commit**

```bash
git add src/desktop.rs src/gui.rs src/gui_adapter.rs ui/app.slint README.md docs/ARCHITECTURE.md
git commit -m "✨Let the desktop app set a folder's object cache size"
```

### Task 8: Full gates and the regression laps

**Files:**
- Create (gitignored): `test-runs/object-cache-regression/`, copied from `test-evidence/object-cache-rebuild-20261106/scripts/`

**Interfaces:**
- Consumes: `--object-cache-mib`, the `object_cache` row in `mount-stats`.

- [ ] **Step 1: Run the host gates**

```bash
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt --check
wsl.exe -e bash -lc 'cd /mnt/c/Users/felyp/Desktop/Projetos/Telekinesis-wt/object-cache && CARGO_TARGET_DIR=$HOME/tkfs-target/object-cache cargo check --all-targets'
cargo test --manifest-path android/native/Cargo.toml
python scripts/build_android.py
```
Expected: all pass.

- [ ] **Step 2: Run the Android end-to-end suite**, since phones now default to 64 MiB and read through the changed `read_range`

Run: `python scripts/android_e2e.py --scenario all`
Expected: PASS, including the large-file scenarios.

- [ ] **Step 3: Adapt the lap harness**

In the copied `guest.ps1`:

- Pass `--object-cache-mib $Mib` to the daemon, replacing the `TKFS_TRACE_*` variables.
- Take a `stats-clean` snapshot after `clean`.
- Take the binary from a `-Bin` parameter, so one session can compare two.

Setup notes from the first run:

- Start the WinFsp driver with `sc start` from `wsb exec --run-as System` after the MSI, because a login token does not demand-load it (`MOUNT_FAILED: c000000e`).
- Copy only the folder's state directory, so no catalog or peer list exists in the guest, and disable guest networking.
- Under PowerShell 5.1 with `$ErrorActionPreference='Stop'`, send native stderr through `cmd /c "... 2>&1"`.

- [ ] **Step 4: Run rotated laps in one Sandbox session**: the merge base's binary and this branch's, 3 laps each, at 256 MiB

Comparing inside one session means Android's changes cannot skew the baseline. Bound every step; stop at the first cleanup failure.

Expected, against the spec's acceptance:

- Branch median within 6 s of base, for both the build after clean and the warm rebuild.
- The same build result as base. On the 2026-11-06 store copy that is exactly one `CS1566` for `impxml\webui.zip`.
- Every daemon stop graceful.
- The `object_cache` deltas within 10 points of 67% of requests on the first build and 95% on the rebuild.

- [ ] **Step 5: Record the result**

Copy the reports and `mount-stats` into `test-evidence/object-cache-regression-<date>/`. Report any miss with its numbers, not as a pass. This does not close W6-P0.

### Task 9: Follow-up measurement of large files through a mount

The spec's follow-up, after merging. It has no pass or fail, and it does not reopen clock-sweep by itself: that is a new decision for the owner.

**Files:**
- Create (gitignored): `test-runs/object-cache-large-file/`

**Interfaces:**
- Consumes: the Task 8 harness, the merge base and branch binaries.

- [ ] **Step 1: Add three bounded scenarios to the harness**, each run for both binaries in rotated order, in Sandbox

- Re-read, through the mount, a file that fits in the cache: 128 MiB at 256 MiB.
- Re-read a file that does not fit: 512 MiB at 256 MiB.
- Read the 128 MiB file while a second orchestrator in the guest, paired over `127.0.0.1` with an empty folder, pulls the whole store.

Close each file between reads, so the kernel cache is purged and reads reach user mode.

- [ ] **Step 2: Report to the owner**

Save a table to `test-evidence/object-cache-large-file-<date>/results.txt`: throughput, read latency p50 and p99, hit rate, and the `read` callback total per scenario and binary. State whether off-lock reads paid off through a mount, and whether hits still contend.
