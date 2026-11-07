//! Append-only packs. No per-object flush, rename, or separate loose files.
use crate::model::content::{CHUNK_SIZE, ContentKind, Digest, FileContentRef};
use crate::util::sqlite::CachedConnection;
use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, Weak},
};

pub const SEGMENT_BYTES: u64 = 64 * 1024 * 1024;
pub const CACHE_BYTES: usize = 256 * 1024 * 1024;
const MAX_OBJECT_BYTES: usize = 64 * 1024 * 1024;
// Fixed-size records only, separate from the verified object-byte cache.
const LOCATION_CACHE_ENTRIES: usize = 131_072;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum Kind {
    Chunk = 1,
    Manifest = 2,
    Tree = 3,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Manifest {
    pub len: u64,
    pub chunks: Vec<Digest>,
}
impl Manifest {
    pub fn encode(&self) -> Result<Vec<u8>> {
        ensure!(
            self.len > CHUNK_SIZE && self.chunks.len() as u64 == self.len.div_ceil(CHUNK_SIZE),
            "INVALID_MANIFEST"
        );
        let mut bytes = b"TKFM\x02".to_vec();
        bytes.extend_from_slice(&self.len.to_le_bytes());
        for chunk in &self.chunks {
            bytes.extend_from_slice(&chunk.0);
        }
        ensure!(bytes.len() <= MAX_OBJECT_BYTES, "MANIFEST_TOO_LARGE");
        Ok(bytes)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let (len, chunks) = Self::encoded_chunks(bytes)?;
        Ok(Self {
            len,
            chunks: chunks
                .chunks_exact(32)
                .map(|b| Digest(b.try_into().unwrap()))
                .collect(),
        })
    }
    fn encoded_chunks(bytes: &[u8]) -> Result<(u64, &[u8])> {
        ensure!(
            bytes.len() >= 13
                && &bytes[..5] == b"TKFM\x02"
                && (bytes.len() - 13).is_multiple_of(32),
            "INVALID_MANIFEST"
        );
        let len = u64::from_le_bytes(bytes[5..13].try_into()?);
        ensure!(
            len > CHUNK_SIZE && (bytes.len() - 13) as u64 / 32 == len.div_ceil(CHUNK_SIZE),
            "INVALID_MANIFEST"
        );
        Ok((len, &bytes[13..]))
    }
}

/// Which old segments a rewrite takes.
#[derive(Clone, Copy)]
enum Rewriting<'a> {
    /// Those with less than this share of their bytes live.
    BelowShare(f64),
    /// Those that would lose one of these.
    Losing(&'a BTreeSet<Digest>),
}
#[derive(Clone, Copy)]
struct Location {
    segment: u64,
    offset: u64,
    len: usize,
    kind: Kind,
}
struct Cached {
    bytes: Arc<Vec<u8>>,
    stamp: u64,
}
struct State {
    db: Connection,
    segment: u64,
    writer: File,
    offset: u64,
    pending: BTreeMap<Digest, Location>,
    locations: BTreeMap<Digest, Location>,
    location_order: VecDeque<Digest>,
    readers: BTreeMap<u64, Arc<File>>,
    cache: BTreeMap<Digest, Cached>,
    order: BTreeSet<(u64, Digest)>,
    cache_len: usize,
    stamp: u64,
    pins: BTreeMap<Digest, usize>,
    verified_reads: u64,
    /// How many more appends go through whole. The one after stops halfway, as on a full volume.
    #[cfg(test)]
    appends_left: Option<usize>,
}
struct Inner {
    root: PathBuf,
    state: Mutex<State>,
    cache_limit: usize,
    segment_limit: u64,
    /// Bumped before any object is removed, so an answer about what is here can tell it was
    /// given before a removal. See [`PackStore::generation`].
    generation: std::sync::atomic::AtomicU64,
}
#[derive(Clone)]
pub struct PackStore {
    inner: Arc<Inner>,
}
static STORES: Mutex<BTreeMap<PathBuf, Weak<Inner>>> = Mutex::new(BTreeMap::new());
fn guard<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
pub struct Pin {
    store: PackStore,
    digests: Vec<Digest>,
}
impl Drop for Pin {
    fn drop(&mut self) {
        let mut state = guard(&self.store.inner.state);
        for digest in &self.digests {
            if let Some(count) = state.pins.get_mut(digest) {
                *count -= 1;
                if *count == 0 {
                    state.pins.remove(digest);
                }
            }
        }
    }
}
fn segment_path(root: &Path, segment: u64) -> PathBuf {
    root.join(format!("{segment:016x}.pack"))
}
fn append_file(root: &Path, segment: u64) -> Result<File> {
    Ok(OpenOptions::new()
        .create(true)
        .append(true)
        .read(true)
        .open(segment_path(root, segment))?)
}
fn kind(value: u8) -> Result<Kind> {
    match value {
        1 => Ok(Kind::Chunk),
        2 => Ok(Kind::Manifest),
        3 => Ok(Kind::Tree),
        _ => anyhow::bail!("INVALID_OBJECT_KIND"),
    }
}

impl PackStore {
    pub fn open(store: &Path) -> Result<Self> {
        Self::with_limits(store, CACHE_BYTES, SEGMENT_BYTES)
    }
    pub fn with_limits(store: &Path, cache_limit: usize, segment_limit: u64) -> Result<Self> {
        fs::create_dir_all(store.join("objects/packs"))?;
        let root = fs::canonicalize(store.join("objects/packs"))?;
        let mut stores = guard(&STORES);
        if let Some(inner) = stores.get(&root).and_then(Weak::upgrade) {
            return Ok(Self { inner });
        }
        let db = Connection::open(root.parent().unwrap().join("index.sqlite"))?;
        db.set_prepared_statement_cache_capacity(32);
        db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
          CREATE TABLE IF NOT EXISTS objects(digest BLOB PRIMARY KEY CHECK(length(digest)=32), segment INTEGER NOT NULL, offset INTEGER NOT NULL, length INTEGER NOT NULL, kind INTEGER NOT NULL);")?;
        // Never reuse a segment number after GC or a crash with an unindexed tail.
        let mut segment = 0;
        for entry in fs::read_dir(&root)? {
            let path = entry?.path();
            if path.extension().is_some_and(|e| e == "pack")
                && let Some(n) = path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .and_then(|s| u64::from_str_radix(s, 16).ok())
            {
                segment = segment.max(n + 1);
            }
        }
        let writer = append_file(&root, segment)?;
        let inner = Arc::new(Inner {
            root: root.clone(),
            cache_limit,
            segment_limit,
            generation: std::sync::atomic::AtomicU64::new(0),
            state: Mutex::new(State {
                db,
                segment,
                writer,
                offset: 0,
                pending: BTreeMap::new(),
                locations: BTreeMap::new(),
                location_order: VecDeque::new(),
                readers: BTreeMap::new(),
                cache: BTreeMap::new(),
                order: BTreeSet::new(),
                cache_len: 0,
                stamp: 0,
                pins: BTreeMap::new(),
                verified_reads: 0,
                #[cfg(test)]
                appends_left: None,
            }),
        });
        stores.insert(root, Arc::downgrade(&inner));
        Ok(Self { inner })
    }
    fn location(state: &mut State, digest: Digest) -> Result<Option<Location>> {
        if let Some(location) = state.pending.get(&digest) {
            return Ok(Some(*location));
        }
        if let Some(location) = state.locations.get(&digest) {
            return Ok(Some(*location));
        }
        let row: Option<(u64, u64, usize, u8)> = state
            .db
            .query_cached(
                "SELECT segment,offset,length,kind FROM objects WHERE digest=?1",
                [&digest.0[..]],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .optional()?;
        let Some((segment, offset, len, k)) = row else {
            return Ok(None);
        };
        let location = Location {
            segment,
            offset,
            len,
            kind: kind(k)?,
        };
        // Positive entries only: a missing digest can arrive at any time.
        // FIFO bounds memory without mutating the order on every presence check.
        if state.locations.len() == LOCATION_CACHE_ENTRIES {
            let oldest = state.location_order.pop_front().unwrap();
            state.locations.remove(&oldest);
        }
        state.locations.insert(digest, location);
        state.location_order.push_back(digest);
        Ok(Some(location))
    }
    pub fn put(&self, kind: Kind, bytes: &[u8]) -> Result<Digest> {
        self.put_with_status(kind, bytes).map(|(digest, _)| digest)
    }
    /// Hash and validate the supplied bytes once; report insertion under the
    /// same pack lock as deduplication. Callers never supply an unchecked hash.
    pub(crate) fn put_with_status(&self, kind: Kind, bytes: &[u8]) -> Result<(Digest, bool)> {
        ensure!(
            bytes.len() <= MAX_OBJECT_BYTES
                && (kind != Kind::Chunk || bytes.len() <= CHUNK_SIZE as usize),
            "OBJECT_TOO_LARGE"
        );
        let digest = Digest::bytes(bytes);
        let mut state = guard(&self.inner.state);
        if let Some(old) = Self::location(&mut state, digest)? {
            ensure!(old.kind == kind, "OBJECT_KIND_COLLISION");
            return Ok((digest, false));
        }
        if state.offset > 0 && state.offset + bytes.len() as u64 > self.inner.segment_limit {
            Self::flush_locked(&mut state)?;
            self.roll_locked(&mut state)?;
        }
        let offset = self.append(&mut state, bytes)?;
        let location = Location {
            segment: state.segment,
            offset,
            len: bytes.len(),
            kind,
        };
        state.pending.insert(digest, location);
        Ok((digest, true))
    }
    /// Opens the next segment for writing. If that fails the open one stays open.
    fn roll_locked(&self, state: &mut State) -> Result<()> {
        state.writer.sync_all()?;
        let writer = append_file(&self.inner.root, state.segment + 1)?;
        state.segment += 1;
        state.writer = writer;
        state.offset = 0;
        Ok(())
    }
    /// Adds `bytes` to the open segment and says where. The file opens to append, so what follows
    /// a write that stopped short, as on a full volume, would land past where the index says. That
    /// write is taken back.
    fn append(&self, state: &mut State, bytes: &[u8]) -> Result<u64> {
        let offset = state.offset;
        if let Err(error) = Self::write_out(state, bytes) {
            self.cut_open_segment(state, offset);
            return Err(error.into());
        }
        state.offset += bytes.len() as u64;
        Ok(offset)
    }
    /// Cuts the open segment back to `offset`, past which the index names nothing. If it cannot be
    /// cut, what comes next goes after what is there. An append handle cannot cut on every platform.
    fn cut_open_segment(&self, state: &mut State, offset: u64) {
        let cut = OpenOptions::new()
            .write(true)
            .open(segment_path(&self.inner.root, state.segment))
            .and_then(|file| file.set_len(offset));
        state.offset = match cut {
            Ok(()) => offset,
            Err(_) => state
                .writer
                .metadata()
                .map_or(state.offset, |metadata| metadata.len()),
        };
    }
    fn write_out(state: &mut State, bytes: &[u8]) -> std::io::Result<()> {
        #[cfg(test)]
        if let Some(left) = &mut state.appends_left {
            if *left == 0 {
                state.writer.write_all(&bytes[..bytes.len() / 2])?;
                return Err(std::io::ErrorKind::StorageFull.into());
            }
            *left -= 1;
        }
        state.writer.write_all(bytes)
    }
    /// See [`State::appends_left`].
    #[cfg(test)]
    pub(crate) fn fail_appends_after(&self, appends: Option<usize>) {
        guard(&self.inner.state).appends_left = appends;
    }
    /// What the object is, if it is here.
    pub fn kind(&self, digest: Digest) -> Result<Option<Kind>> {
        Ok(Self::location(&mut guard(&self.inner.state), digest)?.map(|location| location.kind))
    }
    /// Closes the open segment if any of `digests` is in it, since a collect passes over the open
    /// one. Returns the number for [`Self::collect_named`]: whatever is written from now on is in
    /// that segment or a later one.
    pub fn close_segment_holding(&self, digests: &BTreeSet<Digest>) -> Result<u64> {
        let mut state = guard(&self.inner.state);
        Self::flush_locked(&mut state)?;
        for digest in digests {
            if Self::location(&mut state, *digest)?
                .is_some_and(|location| location.segment == state.segment)
            {
                self.roll_locked(&mut state)?;
                break;
            }
        }
        Ok(state.segment)
    }
    fn flush_locked(state: &mut State) -> Result<()> {
        if state.pending.is_empty() {
            return Ok(());
        }
        state.writer.sync_all()?;
        let tx = state.db.transaction()?;
        for (digest, loc) in &state.pending {
            tx.execute_cached(
                "INSERT OR IGNORE INTO objects VALUES(?1,?2,?3,?4,?5)",
                params![
                    &digest.0[..],
                    loc.segment,
                    loc.offset,
                    loc.len,
                    loc.kind as u8
                ],
            )?;
        }
        tx.commit()?;
        state.pending.clear();
        Ok(())
    }
    pub fn flush(&self) -> Result<()> {
        Self::flush_locked(&mut guard(&self.inner.state))
    }
    pub fn get(&self, digest: Digest) -> Result<Arc<Vec<u8>>> {
        let mut state = guard(&self.inner.state);
        state.stamp += 1;
        let stamp = state.stamp;
        if let Some(cached) = state.cache.remove(&digest) {
            state.order.remove(&(cached.stamp, digest));
            let bytes = cached.bytes.clone();
            state.cache.insert(
                digest,
                Cached {
                    bytes: bytes.clone(),
                    stamp,
                },
            );
            state.order.insert((stamp, digest));
            return Ok(bytes);
        }
        let location = Self::location(&mut state, digest)?.context("OBJECT_MISSING")?;
        ensure!(location.len <= MAX_OBJECT_BYTES, "INVALID_PACK_INDEX");
        let file = if let Some(file) = state.readers.get(&location.segment) {
            file.clone()
        } else {
            let file = Arc::new(File::open(segment_path(
                &self.inner.root,
                location.segment,
            ))?);
            state.readers.insert(location.segment, file.clone());
            file
        };
        let mut bytes = vec![0; location.len];
        let read = read_exact_at(&file, &mut bytes, location.offset);
        if read.is_err() || Digest::bytes(&bytes) != digest {
            state.locations.clear();
            state.location_order.clear();
            state
                .db
                .execute_cached("DELETE FROM objects WHERE digest=?1", [&digest.0[..]])?;
            state.pending.remove(&digest);
            self.bump_generation();
            anyhow::bail!("OBJECT_CORRUPT: {digest}");
        }
        state.verified_reads += 1;
        let bytes = Arc::new(bytes);
        if bytes.len() <= self.inner.cache_limit {
            while state.cache_len + bytes.len() > self.inner.cache_limit {
                let Some((age, old)) = state.order.pop_first() else {
                    break;
                };
                let cached = state.cache.remove(&old).unwrap();
                debug_assert_eq!(age, cached.stamp);
                state.cache_len -= cached.bytes.len();
            }
            state.cache_len += bytes.len();
            state.cache.insert(
                digest,
                Cached {
                    bytes: bytes.clone(),
                    stamp,
                },
            );
            state.order.insert((stamp, digest));
        }
        Ok(bytes)
    }
    pub fn pin(&self, digests: impl IntoIterator<Item = Digest>) -> Pin {
        let digests: Vec<_> = digests.into_iter().collect();
        let mut state = guard(&self.inner.state);
        for digest in &digests {
            *state.pins.entry(*digest).or_default() += 1;
        }
        Pin {
            store: self.clone(),
            digests,
        }
    }
    /// How many bytes the object takes in the packs, if it is here.
    pub fn object_len(&self, digest: Digest) -> Result<Option<u64>> {
        Ok(Self::location(&mut guard(&self.inner.state), digest)?
            .map(|location| location.len as u64))
    }
    /// What every segment takes on disk. A segment a rewrite removes as this looks counts for nothing.
    pub fn disk_bytes(&self) -> Result<u64> {
        let mut total = 0;
        for entry in fs::read_dir(&self.inner.root)? {
            let entry = entry?;
            if entry.path().extension().is_some_and(|e| e == "pack") {
                total += entry.metadata().map_or(0, |metadata| metadata.len());
            }
        }
        Ok(total)
    }
    /// A number that changes whenever objects may have been removed (`collect`, or a corrupt object
    /// dropped). An answer to "is this here" that was true under one generation holds only while it
    /// stays. Adding objects does not change it.
    pub fn generation(&self) -> u64 {
        self.inner
            .generation
            .load(std::sync::atomic::Ordering::SeqCst)
    }
    fn bump_generation(&self) {
        self.inner
            .generation
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
    pub fn contains(&self, digest: Digest) -> Result<bool> {
        Ok(Self::location(&mut guard(&self.inner.state), digest)?.is_some())
    }
    pub fn verified_reads(&self) -> u64 {
        guard(&self.inner.state).verified_reads
    }
    pub fn keys(&self) -> Result<Vec<(Digest, Kind)>> {
        let mut state = guard(&self.inner.state);
        Self::flush_locked(&mut state)?;
        let mut stmt = state.db.prepare_cached("SELECT digest,kind FROM objects")?;
        let rows = stmt
            .query_map([], |r| Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, u8>(1)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        rows.into_iter()
            .map(|(d, k)| Ok((Digest(d.as_slice().try_into()?), kind(k)?)))
            .collect()
    }
    /// Caller supplies transitive snapshot reachability; open handles pin their
    /// entire content closure. Index commit precedes removal of old segments.
    pub fn collect(&self, live: &BTreeSet<Digest>, threshold: f64) -> Result<usize> {
        ensure!((0.0..=1.0).contains(&threshold), "INVALID_GC_THRESHOLD");
        self.rewrite(live, u64::MAX, Rewriting::BelowShare(threshold))
    }
    /// Removes `named`, keeping what `live` or a pin names. Only the segments below `before` that
    /// lose one of them are rewritten, so the cost goes by what was asked for and not by how much
    /// of the store is dead. A segment with nothing to keep is removed with no copying, and what
    /// is dead in one being rewritten is not copied. What is dead elsewhere waits for a collect.
    /// `before` comes from [`Self::close_segment_holding`] before `live` was worked out, so what
    /// was written since stays, whether or not `live` names it.
    pub fn collect_named(
        &self,
        named: &BTreeSet<Digest>,
        live: &BTreeSet<Digest>,
        before: u64,
    ) -> Result<usize> {
        self.rewrite(live, before, Rewriting::Losing(named))
    }
    /// Rewrites the segments below `before` that `which` takes. An error leaves every object
    /// readable. One segment that fails does not stop the others, and the first error is returned
    /// once they are done.
    fn rewrite(&self, live: &BTreeSet<Digest>, before: u64, which: Rewriting) -> Result<usize> {
        let mut state = guard(&self.inner.state);
        Self::flush_locked(&mut state)?;
        // Before anything is removed, and whether or not this ends in an error: what was said of
        // what is here before this is no longer to be believed.
        self.bump_generation();
        // GC can relocate live entries or delete dead ones. Invalidate before
        // any index mutation so error/partial-completion paths are safe too.
        state.locations.clear();
        state.location_order.clear();
        let mut live = live.clone();
        live.extend(state.pins.keys());
        let before = before.min(state.segment);
        let removed = self.rewrite_segments(&mut state, &live, which, before);
        // Cached dead objects must not turn a miss into a successful read, even if the rewrite
        // stopped partway. One still in the packs is only read from there the next time.
        let dead: Vec<_> = state
            .cache
            .keys()
            .filter(|d| !live.contains(d))
            .copied()
            .collect();
        for digest in dead {
            let old = state.cache.remove(&digest).unwrap();
            state.cache_len -= old.bytes.len();
            state.order.remove(&(old.stamp, digest));
        }
        removed
    }
    fn rewrite_segments(
        &self,
        state: &mut State,
        live: &BTreeSet<Digest>,
        which: Rewriting,
        before: u64,
    ) -> Result<usize> {
        let rows = {
            let mut stmt = state.db.prepare_cached(
                "SELECT digest,segment,offset,length,kind FROM objects ORDER BY segment,offset",
            )?;
            stmt.query_map([], |r| {
                Ok((
                    r.get::<_, Vec<u8>>(0)?,
                    r.get::<_, u64>(1)?,
                    r.get::<_, u64>(2)?,
                    r.get::<_, usize>(3)?,
                    r.get::<_, u8>(4)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?
        };
        let mut segments: BTreeMap<u64, Vec<(Digest, Location)>> = BTreeMap::new();
        for (d, segment, offset, len, k) in rows {
            segments.entry(segment).or_default().push((
                Digest(d.as_slice().try_into()?),
                Location {
                    segment,
                    offset,
                    len,
                    kind: kind(k)?,
                },
            ));
        }
        let mut rewrites = Vec::new();
        for (segment, entries) in segments {
            // Rewriting can open new segments, so this is not the open one by then: `before` is.
            if segment >= before {
                continue;
            }
            let total = entries.iter().map(|(_, l)| l.len.max(1)).sum::<usize>();
            let kept = entries
                .iter()
                .filter(|(d, _)| live.contains(d))
                .map(|(_, l)| l.len.max(1))
                .sum::<usize>();
            let taken = match which {
                Rewriting::BelowShare(threshold) => (kept as f64 / total as f64) < threshold,
                Rewriting::Losing(named) => entries
                    .iter()
                    .any(|(d, _)| named.contains(d) && !live.contains(d)),
            };
            if taken {
                rewrites.push((kept, segment, entries));
            }
        }
        // Least to copy first: with little room left, what needs none is freed whatever comes of
        // the rest, and it makes room for them.
        rewrites.sort_by_key(|(kept, segment, _)| (*kept, *segment));
        let mut removed = 0;
        let mut failure = None;
        for (_, segment, entries) in rewrites {
            match self.rewrite_segment(state, segment, &entries, live) {
                Ok(count) => removed += count,
                Err(error) => {
                    failure.get_or_insert(error);
                }
            }
        }
        failure.map_or(Ok(removed), Err)
    }
    /// Moves the live objects of one old segment to the open one and removes it. The old segment
    /// and the index stay as they are until the commit, so no failure loses anything. One before
    /// the commit takes back what was copied, to leave the room it took. One of the commit leaves
    /// the copies where they went: it may have gone through for all this can tell, and then the
    /// index names them. They take room until their segment is rewritten.
    fn rewrite_segment(
        &self,
        state: &mut State,
        segment: u64,
        entries: &[(Digest, Location)],
        live: &BTreeSet<Digest>,
    ) -> Result<usize> {
        let old = File::open(segment_path(&self.inner.root, segment))?;
        let keeping: Vec<&(Digest, Location)> = entries
            .iter()
            .filter(|(digest, _)| live.contains(digest))
            .collect();
        // In one piece: this keeps segments to their size, so freeing later never has to copy
        // more than one at a time, and leaves one place to take back from.
        let copying = keeping.iter().map(|(_, l)| l.len as u64).sum::<u64>();
        if copying > 0 && state.offset > 0 && state.offset + copying > self.inner.segment_limit {
            self.roll_locked(state)?;
        }
        let start = state.offset;
        let copied = (|| -> Result<Vec<(Digest, u64, Location)>> {
            let mut updates = Vec::with_capacity(keeping.len());
            for (digest, loc) in &keeping {
                let mut bytes = vec![0; loc.len];
                read_exact_at(&old, &mut bytes, loc.offset)?;
                ensure!(Digest::bytes(&bytes) == *digest, "OBJECT_CORRUPT_DURING_GC");
                updates.push((*digest, self.append(state, &bytes)?, *loc));
            }
            state.writer.sync_all()?;
            Ok(updates)
        })();
        let updates = match copied {
            Ok(updates) => updates,
            Err(error) => {
                self.cut_open_segment(state, start);
                return Err(error);
            }
        };
        let new_segment = state.segment;
        let committed = (|| -> Result<rusqlite::Result<()>> {
            let tx = state.db.transaction()?;
            tx.execute_cached("DELETE FROM objects WHERE segment=?1", [segment])?;
            for (digest, offset, loc) in updates {
                tx.execute_cached(
                    "INSERT INTO objects VALUES(?1,?2,?3,?4,?5)",
                    params![&digest.0[..], new_segment, offset, loc.len, loc.kind as u8],
                )?;
            }
            Ok(tx.commit())
        })();
        match committed {
            Ok(commit) => commit?,
            Err(error) => {
                self.cut_open_segment(state, start);
                return Err(error);
            }
        }
        state.readers.remove(&segment);
        drop(old);
        fs::remove_file(segment_path(&self.inner.root, segment))?;
        Ok(entries.len() - keeping.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn packs_reopen_deduplicate_and_cache_verified_reads() -> Result<()> {
        let root = tempfile::tempdir()?;
        let digest;
        {
            let store = PackStore::open(root.path())?;
            digest = store.put(Kind::Chunk, b"immutable bytes")?;
            assert_eq!(digest, store.put(Kind::Chunk, b"immutable bytes")?);
            store.flush()?;
            assert_eq!(&**store.get(digest)?, b"immutable bytes");
            store.get(digest)?;
            assert_eq!(store.verified_reads(), 1);
        }
        assert_eq!(
            &**PackStore::open(root.path())?.get(digest)?,
            b"immutable bytes"
        );
        Ok(())
    }
    #[test]
    fn counted_puts_report_one_insert_and_keep_kind_and_size_validation() -> Result<()> {
        let root = tempfile::tempdir()?;
        let store = PackStore::open(root.path())?;
        let bytes = Arc::new(vec![19; CHUNK_SIZE as usize]);
        let expected = Digest::bytes(&bytes);
        let start = Arc::new(std::sync::Barrier::new(9));
        let workers = (0..8)
            .map(|_| {
                let store = store.clone();
                let bytes = bytes.clone();
                let start = start.clone();
                std::thread::spawn(move || {
                    start.wait();
                    store.put_with_status(Kind::Chunk, &bytes)
                })
            })
            .collect::<Vec<_>>();
        start.wait();
        let mut inserted = 0;
        for worker in workers {
            let (digest, new) = worker.join().unwrap()?;
            assert_eq!(digest, expected);
            inserted += usize::from(new);
        }
        assert_eq!(inserted, 1);
        assert!(
            store
                .put_with_status(Kind::Tree, &bytes)
                .unwrap_err()
                .to_string()
                .contains("OBJECT_KIND_COLLISION")
        );
        store.flush()?;
        drop(store);
        let store = PackStore::open(root.path())?;
        assert_eq!(
            store.put_with_status(Kind::Chunk, &bytes)?,
            (expected, false)
        );
        assert!(
            store
                .put_with_status(Kind::Tree, &bytes)
                .unwrap_err()
                .to_string()
                .contains("OBJECT_KIND_COLLISION")
        );
        assert!(
            store
                .put_with_status(Kind::Chunk, &vec![0; CHUNK_SIZE as usize + 1])
                .unwrap_err()
                .to_string()
                .contains("OBJECT_TOO_LARGE")
        );
        assert_eq!(store.keys()?, vec![(expected, Kind::Chunk)]);
        assert_eq!(store.get(expected)?.as_slice(), bytes.as_slice());
        Ok(())
    }
    #[test]
    fn range_reads_preserve_cross_chunk_partial_eof_and_empty_boundaries() -> Result<()> {
        let home = tempfile::tempdir()?;
        let content = LocalContent::open(home.path())?;
        for len in [17, CHUNK_SIZE as usize, 3 * CHUNK_SIZE as usize + 17] {
            let bytes = (0..len)
                .map(|i| (i.wrapping_mul(17) % 251) as u8)
                .collect::<Vec<_>>();
            let reference = content.import(&bytes)?;
            for start in [0, 3, len / 2, len - 1, len] {
                for count in [0, 1, 31, CHUNK_SIZE as usize + 7] {
                    let mut got = vec![0; count.min(len - start)];
                    content.read_range(&reference, start as u64, &mut got)?;
                    assert_eq!(got, bytes[start..start + got.len()]);
                }
            }
            assert!(
                content
                    .read_range(&reference, len as u64, &mut [0])
                    .is_err()
            );
            assert!(
                content
                    .read_range(&reference, len as u64 + 1, &mut [])
                    .is_err()
            );
        }
        Ok(())
    }
    #[test]
    fn small_range_read_still_validates_complete_manifest_framing() -> Result<()> {
        let home = tempfile::tempdir()?;
        let content = LocalContent::open(home.path())?;
        let digest = content
            .packs
            .put(Kind::Chunk, &vec![7; CHUNK_SIZE as usize])?;
        let valid = Manifest {
            len: CHUNK_SIZE * 2,
            chunks: vec![digest; 2],
        }
        .encode()?;
        for kind in 0..3 {
            let mut malformed = valid.clone();
            match kind {
                0 => malformed.push(0),
                1 => {
                    malformed.truncate(malformed.len() - 32);
                }
                _ => malformed[5..13].copy_from_slice(&(CHUNK_SIZE * 2 - 1).to_le_bytes()),
            }
            let reference = FileContentRef::new(
                ContentKind::Manifest,
                content.packs.put(Kind::Manifest, &malformed)?,
                CHUNK_SIZE * 2,
            )?;
            assert!(content.read_range(&reference, 0, &mut [0]).is_err());
            assert!(
                content
                    .read_range(&reference, reference.total_len, &mut [])
                    .is_err()
            );
        }
        Ok(())
    }
    /// Records what it is asked to hint. A hint must wait for nothing, so it
    /// also counts fetches.
    #[derive(Default)]
    struct RecordingFetcher {
        hinted: Mutex<Vec<(Digest, Kind)>>,
        fetches: Mutex<usize>,
    }
    impl ObjectFetcher for RecordingFetcher {
        fn fetch(&self, _objects: &[(Digest, Kind)]) -> Result<()> {
            *guard(&self.fetches) += 1;
            anyhow::bail!("OBJECT_MISSING")
        }
        fn hint(&self, objects: &[(Digest, Kind)]) {
            guard(&self.hinted).extend_from_slice(objects);
        }
    }
    #[test]
    fn hint_range_names_the_missing_chunks_of_a_range_in_order() -> Result<()> {
        let home = tempfile::tempdir()?;
        let content = LocalContent::open(home.path())?;
        let fetcher = Arc::new(RecordingFetcher::default());
        content.set_fetcher(fetcher.clone());
        let chunk = |n: u8| vec![n; CHUNK_SIZE as usize];
        let held = content.packs.put(Kind::Chunk, &chunk(1))?;
        // Absent, and one of them twice: a file may repeat a chunk.
        let absent = [2, 3, 4].map(|n| Digest::bytes(&chunk(n)));
        let chunks = vec![held, absent[0], absent[1], absent[0], absent[2]];
        let len = 4 * CHUNK_SIZE + 9;
        let manifest = Manifest { len, chunks }.encode()?;
        let reference = FileContentRef::new(
            ContentKind::Manifest,
            content.packs.put(Kind::Manifest, &manifest)?,
            len,
        )?;
        let want = |digests: &[Digest]| {
            digests
                .iter()
                .map(|digest| (*digest, Kind::Chunk))
                .collect::<Vec<_>>()
        };
        // From inside the held chunk to the start of the third absent one.
        assert_eq!(
            content.hint_range(&reference, 7, 2 * CHUNK_SIZE)?,
            want(&absent[..2])
        );
        assert_eq!(*guard(&fetcher.hinted), want(&absent[..2]));
        // Past the end is cut to the end, and a repeated chunk is named once.
        assert_eq!(
            content.hint_range(&reference, CHUNK_SIZE, 100 * CHUNK_SIZE)?,
            want(&absent)
        );
        guard(&fetcher.hinted).clear();
        for (offset, count) in [(0, 0), (len, CHUNK_SIZE), (len + 1, 1)] {
            assert!(content.hint_range(&reference, offset, count)?.is_empty());
        }
        assert!(guard(&fetcher.hinted).is_empty());
        Ok(())
    }
    #[test]
    fn hint_range_of_an_absent_manifest_hints_nothing_and_does_not_wait() -> Result<()> {
        let home = tempfile::tempdir()?;
        let content = LocalContent::open(home.path())?;
        let fetcher = Arc::new(RecordingFetcher::default());
        content.set_fetcher(fetcher.clone());
        let reference = FileContentRef::new(
            ContentKind::Manifest,
            Digest::bytes(b"not here"),
            2 * CHUNK_SIZE,
        )?;
        assert!(content.hint_range(&reference, 0, CHUNK_SIZE)?.is_empty());
        assert_eq!(*guard(&fetcher.fetches), 0);
        assert!(guard(&fetcher.hinted).is_empty());
        Ok(())
    }
    #[test]
    fn gc_never_removes_a_pinned_object() -> Result<()> {
        let root = tempfile::tempdir()?;
        let store = PackStore::with_limits(root.path(), 1024, 8)?;
        let keep = store.put(Kind::Chunk, b"keep me!")?;
        let dead = store.put(Kind::Chunk, b"dead me!")?;
        let _roll = store.put(Kind::Chunk, b"roll me!")?;
        let pin = store.pin([keep]);
        assert_eq!(store.collect(&BTreeSet::new(), 1.0)?, 1);
        assert_eq!(&**store.get(keep)?, b"keep me!");
        assert!(!store.contains(dead)?);
        drop(pin);
        store.put(Kind::Chunk, b"roll again")?;
        store.collect(&BTreeSet::new(), 1.0)?;
        assert!(!store.contains(keep)?);
        Ok(())
    }
    #[test]
    fn cached_locations_follow_gc_relocation_and_allow_missing_objects_to_arrive() -> Result<()> {
        let root = tempfile::tempdir()?;
        let store = PackStore::with_limits(root.path(), 0, 16)?;
        let keep = store.put(Kind::Chunk, b"keep me!")?;
        let dead = store.put(Kind::Chunk, b"dead me!")?;
        store.put(Kind::Chunk, b"roll me!")?;
        assert!(store.contains(keep)? && store.contains(dead)?);
        let original = guard(&store.inner.state).locations[&keep].segment;
        assert_eq!(store.collect(&BTreeSet::from([keep]), 1.0)?, 1);
        assert!(!store.contains(dead)?);
        assert_eq!(store.get(keep)?.as_slice(), b"keep me!");
        assert_ne!(guard(&store.inner.state).locations[&keep].segment, original);
        assert!(store.put(Kind::Tree, b"keep me!").is_err());
        let arriving = Digest::bytes(b"arriving");
        assert!(!store.contains(arriving)?);
        assert_eq!(
            store.put_with_status(Kind::Chunk, b"arriving")?,
            (arriving, true)
        );
        assert!(store.contains(arriving)?);
        store.flush()?;
        drop(store);
        let reopened = PackStore::open(root.path())?;
        assert!(!reopened.contains(dead)?);
        assert_eq!(reopened.get(keep)?.as_slice(), b"keep me!");
        assert_eq!(reopened.get(arriving)?.as_slice(), b"arriving");
        Ok(())
    }
    /// What a segment file holds, in bytes.
    fn segment_len(store: &PackStore, segment: u64) -> Result<u64> {
        Ok(fs::metadata(segment_path(&store.inner.root, segment))?.len())
    }
    fn open_segment(store: &PackStore) -> u64 {
        guard(&store.inner.state).segment
    }
    #[test]
    fn a_failed_rewrite_leaves_every_object_readable_and_the_store_usable() -> Result<()> {
        let root = tempfile::tempdir()?;
        let store = PackStore::with_limits(root.path(), 0, 24)?;
        let one = store.put(Kind::Chunk, b"live one")?;
        let two = store.put(Kind::Chunk, b"live two")?;
        let dead = store.put(Kind::Chunk, b"dead one")?;
        store.put(Kind::Chunk, b"rolls on")?;
        let live = BTreeSet::from([one, two]);
        let open = open_segment(&store);
        let len = segment_len(&store, open)?;
        // The first copy goes through, and the second stops halfway.
        store.fail_appends_after(Some(1));
        assert!(store.collect(&live, 1.0).is_err());
        store.fail_appends_after(None);
        assert_eq!(store.get(one)?.as_slice(), b"live one");
        assert_eq!(store.get(two)?.as_slice(), b"live two");
        assert_eq!(store.get(dead)?.as_slice(), b"dead one", "half-deleted");
        assert_eq!(segment_len(&store, open)?, len, "what it copied stayed");
        // What comes next is where the index says.
        let after = store.put(Kind::Chunk, b"afterward")?;
        store.flush()?;
        assert_eq!(store.get(after)?.as_slice(), b"afterward");
        // And it can be tried again.
        assert_eq!(store.collect(&live, 1.0)?, 1);
        assert!(!store.contains(dead)?);
        assert_eq!(store.get(one)?.as_slice(), b"live one");
        assert_eq!(store.get(two)?.as_slice(), b"live two");
        drop(store);
        let reopened = PackStore::open(root.path())?;
        assert_eq!(reopened.get(one)?.as_slice(), b"live one");
        assert_eq!(reopened.get(two)?.as_slice(), b"live two");
        assert_eq!(reopened.get(after)?.as_slice(), b"afterward");
        Ok(())
    }
    #[test]
    fn a_put_that_stops_halfway_shifts_nothing_written_after_it() -> Result<()> {
        let root = tempfile::tempdir()?;
        let store = PackStore::with_limits(root.path(), 0, 1024)?;
        let first = store.put(Kind::Chunk, b"first")?;
        store.fail_appends_after(Some(0));
        assert!(store.put(Kind::Chunk, b"cut short").is_err());
        store.fail_appends_after(None);
        assert!(!store.contains(Digest::bytes(b"cut short"))?);
        let next = store.put(Kind::Chunk, b"next")?;
        let again = store.put(Kind::Chunk, b"cut short")?;
        store.flush()?;
        assert_eq!(store.get(first)?.as_slice(), b"first");
        assert_eq!(store.get(next)?.as_slice(), b"next");
        assert_eq!(store.get(again)?.as_slice(), b"cut short");
        Ok(())
    }
    #[test]
    fn a_segment_that_will_not_open_leaves_the_last_one_open() -> Result<()> {
        let root = tempfile::tempdir()?;
        let store = PackStore::with_limits(root.path(), 0, 16)?;
        let first = store.put(Kind::Chunk, b"twelve bytes")?;
        let open = open_segment(&store);
        let next = segment_path(&store.inner.root, open + 1);
        fs::create_dir(&next)?;
        assert!(store.put(Kind::Chunk, b"too long").is_err());
        fs::remove_dir(&next)?;
        assert_eq!(open_segment(&store), open);
        // This one still fits where it was going to.
        let small = store.put(Kind::Chunk, b"tiny")?;
        store.flush()?;
        assert_eq!(store.get(first)?.as_slice(), b"twelve bytes");
        assert_eq!(store.get(small)?.as_slice(), b"tiny");
        Ok(())
    }
    /// With little room, what takes no copying is freed before anything that does can fail.
    #[test]
    fn a_failed_rewrite_still_frees_a_segment_with_nothing_to_copy() -> Result<()> {
        let root = tempfile::tempdir()?;
        let store = PackStore::with_limits(root.path(), 1024, 16)?;
        let live = store.put(Kind::Chunk, b"live one")?;
        let dead_with_it = store.put(Kind::Chunk, b"dead one")?;
        let dead = store.put(Kind::Chunk, b"all dead")?;
        store.put(Kind::Chunk, b"all gone")?;
        store.put(Kind::Chunk, b"rolls on")?;
        store.flush()?;
        // In the cache, which must not answer for what the packs lost.
        store.get(dead)?;
        store.fail_appends_after(Some(0));
        assert!(store.collect(&BTreeSet::from([live]), 1.0).is_err());
        store.fail_appends_after(None);
        assert!(!store.contains(dead)?);
        assert!(store.get(dead).is_err(), "the cache still answered");
        assert_eq!(store.get(live)?.as_slice(), b"live one");
        assert!(store.contains(dead_with_it)?);
        Ok(())
    }
    #[test]
    fn a_closed_segment_is_collected_and_what_comes_after_it_stays() -> Result<()> {
        let root = tempfile::tempdir()?;
        let store = PackStore::with_limits(root.path(), 0, 16)?;
        let named = store.put(Kind::Chunk, b"named!!!")?;
        let kept = store.put(Kind::Chunk, b"kept!!!!")?;
        let open = open_segment(&store);
        let before = store.close_segment_holding(&BTreeSet::from([named]))?;
        assert!(before > open, "the segment that holds it stayed open");
        // These come once `live` has been worked out, and it does not name them. One is past a
        // segment that closed by itself.
        let arriving = store.put(Kind::Chunk, b"arrives!")?;
        let also = store.put(Kind::Chunk, b"arrives2")?;
        let last = store.put(Kind::Chunk, b"arrives3")?;
        assert!(open_segment(&store) > before);
        let named = BTreeSet::from([named]);
        assert_eq!(
            store.collect_named(&named, &BTreeSet::from([kept]), before)?,
            1
        );
        assert!(!store.contains(*named.first().unwrap())?);
        assert_eq!(store.get(kept)?.as_slice(), b"kept!!!!");
        assert_eq!(store.get(arriving)?.as_slice(), b"arrives!");
        assert_eq!(store.get(also)?.as_slice(), b"arrives2");
        assert_eq!(store.get(last)?.as_slice(), b"arrives3");
        Ok(())
    }
    /// Freeing one small file must not copy most of the store. A segment that loses nothing named
    /// is not looked at, whatever is dead in it, and that waits for a collect.
    #[test]
    fn collect_named_touches_only_the_segments_that_lose_one() -> Result<()> {
        let root = tempfile::tempdir()?;
        let store = PackStore::with_limits(root.path(), 0, 24)?;
        // All dead, and nothing in it named.
        let others = [b"other 1!", b"other 2!", b"other 3!"]
            .map(|bytes| store.put(Kind::Chunk, bytes).unwrap());
        // One named, one dead that is not, one live.
        let named = store.put(Kind::Chunk, b"named 1!")?;
        let dead = store.put(Kind::Chunk, b"dead 1!!")?;
        let live = store.put(Kind::Chunk, b"live 1!!")?;
        // Nothing but what is named.
        let alone = [b"named 2!", b"named 3!", b"named 4!"]
            .map(|bytes| store.put(Kind::Chunk, bytes).unwrap());
        // One named that a handle pins, and the rest live: nothing here can go.
        let pinned = store.put(Kind::Chunk, b"pinned!!")?;
        let beside = [b"beside1!", b"beside2!"].map(|bytes| store.put(Kind::Chunk, bytes).unwrap());
        let open_object = store.put(Kind::Chunk, b"in open!")?;
        store.flush()?;
        let _pin = store.pin([pinned]);
        let open = open_segment(&store);
        assert_eq!(open, 4);

        let mut names = BTreeSet::from([named, pinned]);
        names.extend(alone);
        let before = store.close_segment_holding(&names)?;
        assert_eq!(before, open, "nothing named is in the open segment");
        let mut keep = BTreeSet::from([live, open_object]);
        keep.extend(beside);
        assert_eq!(store.collect_named(&names, &keep, before)?, 5);

        for digest in others {
            assert!(store.contains(digest)?, "a dead object nobody named went");
        }
        assert_eq!(segment_len(&store, 0)?, 24);
        // Not copied: it would be taking room to keep what no one wants.
        assert!(!store.contains(named)? && !store.contains(dead)?);
        assert!(!segment_path(&store.inner.root, 1).exists());
        assert_eq!(store.get(live)?.as_slice(), b"live 1!!");
        for digest in alone {
            assert!(!store.contains(digest)?);
        }
        assert!(!segment_path(&store.inner.root, 2).exists());
        assert_eq!(segment_len(&store, 3)?, 24, "nothing there could go");
        assert_eq!(store.get(pinned)?.as_slice(), b"pinned!!");
        // All that was copied is the one live object, after what the open segment held.
        assert_eq!(segment_len(&store, open)?, 16);
        assert_eq!(store.get(open_object)?.as_slice(), b"in open!");
        Ok(())
    }
    /// Overwrites the start of an object where it lies, as bit rot does.
    fn corrupt(store: &PackStore, digest: Digest) -> Result<()> {
        let location = PackStore::location(&mut guard(&store.inner.state), digest)?.unwrap();
        let file = OpenOptions::new()
            .write(true)
            .open(segment_path(&store.inner.root, location.segment))?;
        #[cfg(windows)]
        {
            use std::os::windows::fs::FileExt;
            file.seek_write(b"!", location.offset)?;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            file.write_at(b"!", location.offset)?;
        }
        Ok(())
    }
    /// A corrupt object in one segment says nothing of the others, which still go.
    #[test]
    fn a_segment_that_fails_does_not_stop_the_rest() -> Result<()> {
        let root = tempfile::tempdir()?;
        let store = PackStore::with_limits(root.path(), 0, 16)?;
        // As much to copy as the next, and older, so it comes first.
        let spoiled = store.put(Kind::Chunk, b"spoiled!")?;
        let dead_beside = store.put(Kind::Chunk, b"dead one")?;
        let fine = store.put(Kind::Chunk, b"fine one")?;
        let dead = store.put(Kind::Chunk, b"dead two")?;
        store.put(Kind::Chunk, b"rolls on")?;
        store.flush()?;
        corrupt(&store, spoiled)?;
        let failed = store
            .collect(&BTreeSet::from([spoiled, fine]), 1.0)
            .unwrap_err();
        assert!(
            failed.to_string().contains("OBJECT_CORRUPT_DURING_GC"),
            "{failed:#}"
        );
        assert!(!store.contains(dead)?, "the failure stopped the rest");
        assert_eq!(store.get(fine)?.as_slice(), b"fine one");
        assert!(store.contains(dead_beside)?);
        Ok(())
    }
    /// Three objects in a closed segment, of which the first two are to be kept.
    fn a_segment_to_rewrite(store: &PackStore) -> Result<[Digest; 3]> {
        let one = store.put(Kind::Chunk, b"live one")?;
        let two = store.put(Kind::Chunk, b"live two")?;
        let dead = store.put(Kind::Chunk, b"dead one")?;
        store.put(Kind::Chunk, b"rolls on")?;
        store.flush()?;
        Ok([one, two, dead])
    }
    #[test]
    fn a_failure_before_the_commit_takes_the_copies_back() -> Result<()> {
        let root = tempfile::tempdir()?;
        let store = PackStore::with_limits(root.path(), 0, 24)?;
        let [one, two, dead] = a_segment_to_rewrite(&store)?;
        let open = open_segment(&store);
        let len = segment_len(&store, open)?;
        guard(&store.inner.state).db.execute_batch(
            "CREATE TRIGGER no_room BEFORE INSERT ON objects
             BEGIN SELECT RAISE(ABORT,'no room'); END;",
        )?;
        assert!(store.collect(&BTreeSet::from([one, two]), 1.0).is_err());
        assert_eq!(segment_len(&store, open)?, len, "what it copied stayed");
        guard(&store.inner.state)
            .db
            .execute_batch("DROP TRIGGER no_room;")?;
        assert_eq!(store.get(one)?.as_slice(), b"live one");
        assert_eq!(store.get(two)?.as_slice(), b"live two");
        assert_eq!(store.get(dead)?.as_slice(), b"dead one");
        let after = store.put(Kind::Chunk, b"afterward")?;
        store.flush()?;
        assert_eq!(store.get(after)?.as_slice(), b"afterward");
        Ok(())
    }
    /// A commit that fails may have gone through, for all this can tell. Cutting the copies off
    /// could leave the index naming bytes that are gone, so they stay, at the cost of their room.
    #[test]
    fn a_failed_commit_leaves_the_copies_where_they_went() -> Result<()> {
        let root = tempfile::tempdir()?;
        let store = PackStore::with_limits(root.path(), 0, 24)?;
        let [one, two, dead] = a_segment_to_rewrite(&store)?;
        let open = open_segment(&store);
        let len = segment_len(&store, open)?;
        // A deferred constraint is looked at only by the commit.
        guard(&store.inner.state).db.execute_batch(
            "PRAGMA foreign_keys=ON;
             CREATE TABLE parent(id INTEGER PRIMARY KEY);
             CREATE TABLE child(id INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED);
             CREATE TRIGGER late AFTER INSERT ON objects
             BEGIN INSERT INTO child VALUES(1); END;",
        )?;
        assert!(store.collect(&BTreeSet::from([one, two]), 1.0).is_err());
        assert_eq!(segment_len(&store, open)?, len + 16, "the copies were cut");
        guard(&store.inner.state)
            .db
            .execute_batch("DROP TRIGGER late;")?;
        assert_eq!(store.get(one)?.as_slice(), b"live one");
        assert_eq!(store.get(two)?.as_slice(), b"live two");
        assert_eq!(store.get(dead)?.as_slice(), b"dead one");
        // What comes next goes after them, where the index says.
        let after = store.put(Kind::Chunk, b"afterward")?;
        store.flush()?;
        assert_eq!(store.get(after)?.as_slice(), b"afterward");
        Ok(())
    }
    #[test]
    fn a_segment_is_closed_only_for_what_it_holds() -> Result<()> {
        let root = tempfile::tempdir()?;
        let store = PackStore::with_limits(root.path(), 0, 8)?;
        let old = store.put(Kind::Chunk, b"in a old")?;
        let held = store.put(Kind::Chunk, b"in it!!!")?;
        let open = open_segment(&store);
        for elsewhere in [BTreeSet::new(), BTreeSet::from([old])] {
            assert_eq!(store.close_segment_holding(&elsewhere)?, open);
        }
        let absent = BTreeSet::from([Digest::bytes(b"not here")]);
        assert_eq!(store.close_segment_holding(&absent)?, open);
        assert_eq!(open_segment(&store), open);
        assert_eq!(
            store.close_segment_holding(&BTreeSet::from([held]))?,
            open + 1
        );
        // Pending and durable: a restart still has it.
        drop(store);
        assert_eq!(
            PackStore::open(root.path())?.get(held)?.as_slice(),
            b"in it!!!"
        );
        Ok(())
    }
    /// What is rewritten stays in segments of the usual size, so that freeing later never has to
    /// copy more than one at a time. That can open a segment on the way, and what was open before
    /// it, with copies already in it, must not be taken for one to rewrite.
    #[test]
    fn a_rewrite_keeps_to_the_segment_limit_and_loses_nothing() -> Result<()> {
        let root = tempfile::tempdir()?;
        let store = PackStore::with_limits(root.path(), 0, 32)?;
        let mut live = BTreeSet::new();
        let mut dead = Vec::new();
        let mut kept = Vec::new();
        for segment in 0..4u8 {
            for object in 0..3u8 {
                let bytes = [b'a' + segment, b'0' + object, 1, 2, 3, 4, 5, 6];
                let digest = store.put(Kind::Chunk, &bytes)?;
                live.insert(digest);
                kept.push((digest, bytes));
            }
            dead.push(store.put(Kind::Chunk, &[b'a' + segment, b'x', 1, 2, 3, 4, 5, 6])?);
        }
        let open_object = store.put(Kind::Chunk, b"in open!")?;
        let open = open_segment(&store);
        assert_eq!(store.collect(&live, 1.0)?, dead.len());
        assert!(open_segment(&store) > open, "nothing rolled");
        for digest in &dead {
            assert!(!store.contains(*digest)?);
        }
        // Its object was not named live, and the open segment is not collected.
        assert!(store.contains(open_object)?);
        let check = |store: &PackStore| -> Result<()> {
            for (digest, bytes) in &kept {
                assert_eq!(store.get(*digest)?.as_slice(), bytes);
            }
            assert_eq!(store.get(open_object)?.as_slice(), b"in open!");
            Ok(())
        };
        check(&store)?;
        for segment in open..=open_segment(&store) {
            assert!(segment_len(&store, segment)? <= 32, "segment {segment}");
        }
        store.flush()?;
        drop(store);
        check(&PackStore::open(root.path())?)
    }
    #[test]
    fn location_cache_evicts_without_losing_indexed_objects() -> Result<()> {
        let root = tempfile::tempdir()?;
        let store = PackStore::open(root.path())?;
        let digests = (0..LOCATION_CACHE_ENTRIES + 3)
            .map(|n| Digest::bytes(&(n as u64).to_le_bytes()))
            .collect::<Vec<_>>();
        // Index-only fixture: contains checks index presence, not file bytes.
        {
            let mut state = guard(&store.inner.state);
            let tx = state.db.transaction()?;
            for digest in &digests {
                tx.execute_cached("INSERT INTO objects VALUES(?1,0,0,8,1)", [&digest.0[..]])?;
            }
            tx.commit()?;
        }
        for digest in &digests {
            assert!(store.contains(*digest)?);
        }
        {
            let state = guard(&store.inner.state);
            assert_eq!(state.locations.len(), LOCATION_CACHE_ENTRIES);
            assert_eq!(state.location_order.len(), LOCATION_CACHE_ENTRIES);
            assert!(!state.locations.contains_key(&digests[0]));
        }
        assert!(store.contains(digests[0])?);
        assert_eq!(
            guard(&store.inner.state).locations.len(),
            LOCATION_CACHE_ENTRIES
        );
        Ok(())
    }
    #[cfg(windows)]
    #[test]
    fn location_cache_follows_committed_gc_even_when_old_segment_deletion_fails() -> Result<()> {
        use std::os::windows::fs::OpenOptionsExt;
        let root = tempfile::tempdir()?;
        let store = PackStore::with_limits(root.path(), 0, 16)?;
        let keep = store.put(Kind::Chunk, b"keep me!")?;
        let dead = store.put(Kind::Chunk, b"dead me!")?;
        store.put(Kind::Chunk, b"roll me!")?;
        assert!(store.contains(keep)? && store.contains(dead)?);
        let original = guard(&store.inner.state).locations[&keep].segment;
        // Permit reads/writes but prevent deletion after the index commits.
        let held = OpenOptions::new()
            .read(true)
            .share_mode(3)
            .open(segment_path(&store.inner.root, original))?;
        assert!(store.collect(&BTreeSet::from([keep]), 1.0).is_err());
        assert!(!store.contains(dead)?);
        assert_eq!(store.get(keep)?.as_slice(), b"keep me!");
        assert_ne!(guard(&store.inner.state).locations[&keep].segment, original);
        assert_eq!(
            store.put_with_status(Kind::Chunk, b"dead me!")?,
            (dead, true)
        );
        store.flush()?;
        drop(held);
        drop(store);
        let reopened = PackStore::open(root.path())?;
        assert_eq!(reopened.get(keep)?.as_slice(), b"keep me!");
        assert_eq!(reopened.get(dead)?.as_slice(), b"dead me!");
        Ok(())
    }
    #[test]
    fn flat_manifest_ranges_and_corruption() -> Result<()> {
        let root = tempfile::tempdir()?;
        let content = LocalContent::open(root.path())?;
        let mut bytes = vec![5; CHUNK_SIZE as usize];
        bytes.extend_from_slice(b"tail");
        let reference = content.import(&bytes)?;
        content.packs.flush()?;
        let mut range = [0; 7];
        content.read_range(&reference, CHUNK_SIZE - 3, &mut range)?;
        assert_eq!(&range, b"\x05\x05\x05tail");
        let digest = content.packs.put(Kind::Chunk, b"corrupt this")?;
        content.packs.flush()?;
        assert!(content.packs.contains(digest)?);
        let location =
            PackStore::location(&mut guard(&content.packs.inner.state), digest)?.unwrap();
        let file = OpenOptions::new()
            .write(true)
            .open(segment_path(&content.packs.inner.root, location.segment))?;
        #[cfg(windows)]
        {
            use std::os::windows::fs::FileExt;
            file.seek_write(b"!", location.offset)?;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            file.write_at(b"!", location.offset)?;
        }
        assert!(content.packs.get(digest).is_err());
        assert!(!content.packs.contains(digest)?);
        assert_eq!(
            content
                .packs
                .put_with_status(Kind::Chunk, b"corrupt this")?,
            (digest, true)
        );
        content.packs.flush()?;
        assert_eq!(content.packs.get(digest)?.as_slice(), b"corrupt this");
        Ok(())
    }
}

pub(crate) fn read_exact_at(
    file: &File,
    mut out: &mut [u8],
    mut offset: u64,
) -> std::io::Result<()> {
    while !out.is_empty() {
        #[cfg(windows)]
        let n = {
            use std::os::windows::fs::FileExt;
            file.seek_read(out, offset)?
        };
        #[cfg(unix)]
        let n = {
            use std::os::unix::fs::FileExt;
            file.read_at(out, offset)?
        };
        if n == 0 {
            return Err(std::io::ErrorKind::UnexpectedEof.into());
        }
        out = &mut out[n..];
        offset += n as u64;
    }
    Ok(())
}

/// Immutable range provider used to hydrate scratch files. Missing chunks can
/// be fetched independently without downloading the rest of the file.
pub trait ObjectFetcher: Send + Sync {
    fn fetch(&self, objects: &[(Digest, Kind)]) -> Result<()>;
    /// Objects a reader is expected to want soon. Nothing waits for them.
    fn hint(&self, _objects: &[(Digest, Kind)]) {}
    /// A reader takes back what it hinted.
    fn unhint(&self, _objects: &[(Digest, Kind)]) {}
}
#[derive(Clone)]
pub struct LocalContent {
    pub packs: PackStore,
    fetcher: Arc<Mutex<Option<Arc<dyn ObjectFetcher>>>>,
}
impl LocalContent {
    pub fn open(root: &Path) -> Result<Self> {
        Ok(Self {
            packs: PackStore::open(root)?,
            fetcher: Arc::new(Mutex::new(None)),
        })
    }
    pub fn set_fetcher(&self, fetcher: Arc<dyn ObjectFetcher>) {
        *guard(&self.fetcher) = Some(fetcher);
    }
    pub fn ensure(&self, objects: &[(Digest, Kind)]) -> Result<()> {
        let missing = objects
            .iter()
            .filter_map(|(d, k)| match self.packs.get(*d) {
                Ok(_) => None,
                Err(_) => Some((*d, *k)),
            })
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            let fetcher = guard(&self.fetcher).clone().context("OBJECT_MISSING")?;
            fetcher.fetch(&missing)?;
        }
        Ok(())
    }
    fn object(&self, digest: Digest, kind: Kind) -> Result<Arc<Vec<u8>>> {
        if let Ok(bytes) = self.packs.get(digest) {
            return Ok(bytes);
        }
        self.ensure(&[(digest, kind)])?;
        self.packs.get(digest)
    }
    pub fn chunks(&self, reference: &FileContentRef) -> Result<Vec<Digest>> {
        reference.validate()?;
        if reference.kind == ContentKind::Direct {
            return Ok(vec![reference.digest]);
        }
        let manifest = Manifest::decode(&self.object(reference.digest, Kind::Manifest)?)?;
        ensure!(
            manifest.len == reference.total_len,
            "INVALID_MANIFEST_LENGTH"
        );
        Ok(manifest.chunks)
    }
    /// [`Self::chunks`] without asking anyone: `None` when the manifest is not here. It
    /// goes to no fetcher, so it is safe wherever a wait for the network is not.
    pub fn chunks_if_local(&self, reference: &FileContentRef) -> Result<Option<Vec<Digest>>> {
        reference.validate()?;
        if reference.kind == ContentKind::Direct {
            return Ok(Some(vec![reference.digest]));
        }
        if !self.packs.contains(reference.digest)? {
            return Ok(None);
        }
        let manifest = Manifest::decode(&self.packs.get(reference.digest)?)?;
        ensure!(
            manifest.len == reference.total_len,
            "INVALID_MANIFEST_LENGTH"
        );
        Ok(Some(manifest.chunks))
    }
    fn range_chunks(
        &self,
        reference: &FileContentRef,
        range: std::ops::Range<usize>,
    ) -> Result<Vec<Digest>> {
        reference.validate()?;
        if reference.kind == ContentKind::Direct {
            return Ok(if range.is_empty() {
                Vec::new()
            } else {
                vec![reference.digest]
            });
        }
        // The pack verifies/caches the complete immutable object. Validate its
        // complete framing and descriptor length, but decode only the requested
        // digests: a small read of a large file must not allocate/copy its whole
        // flat manifest on every paging callback.
        let bytes = self.object(reference.digest, Kind::Manifest)?;
        let (len, encoded) = Manifest::encoded_chunks(&bytes)?;
        ensure!(len == reference.total_len, "INVALID_MANIFEST_LENGTH");
        Ok(encoded[range.start * 32..range.end * 32]
            .chunks_exact(32)
            .map(|b| Digest(b.try_into().unwrap()))
            .collect())
    }
    pub fn read_range(
        &self,
        reference: &FileContentRef,
        offset: u64,
        out: &mut [u8],
    ) -> Result<()> {
        ensure!(
            offset
                .checked_add(out.len() as u64)
                .is_some_and(|n| n <= reference.total_len),
            "READ_PAST_EOF"
        );
        let first = (offset / CHUNK_SIZE) as usize;
        let end = if out.is_empty() {
            first
        } else {
            ((offset + out.len() as u64 - 1) / CHUNK_SIZE + 1) as usize
        };
        let chunks = self.range_chunks(reference, first..end)?;
        if !out.is_empty() {
            let wanted = chunks
                .iter()
                .map(|digest| (*digest, Kind::Chunk))
                .collect::<Vec<_>>();
            self.ensure(&wanted)?;
        }
        let mut done = 0;
        while done < out.len() {
            let pos = offset + done as u64;
            let bytes = self
                .packs
                .get(chunks[(pos / CHUNK_SIZE) as usize - first])?;
            let expected =
                (reference.total_len - pos / CHUNK_SIZE * CHUNK_SIZE).min(CHUNK_SIZE) as usize;
            ensure!(bytes.len() == expected, "INVALID_CHUNK_LENGTH");
            let start = (pos % CHUNK_SIZE) as usize;
            let count = (out.len() - done).min(bytes.len() - start);
            out[done..done + count].copy_from_slice(&bytes[start..start + count]);
            done += count;
        }
        Ok(())
    }
    /// Hints the chunks of the range, cut to the file, that are not here yet: what
    /// a reader is expected to want next, nearest first. Returns them, for
    /// [`Self::unhint`]. Nothing waits for this: with the manifest not here there
    /// is nothing to name.
    pub fn hint_range(
        &self,
        reference: &FileContentRef,
        offset: u64,
        len: u64,
    ) -> Result<Vec<(Digest, Kind)>> {
        let end = offset.saturating_add(len).min(reference.total_len);
        let Some(fetcher) = guard(&self.fetcher).clone() else {
            return Ok(Vec::new());
        };
        if offset >= end
            || (reference.kind == ContentKind::Manifest
                && !self.packs.contains(reference.digest)?)
        {
            return Ok(Vec::new());
        }
        let range = (offset / CHUNK_SIZE) as usize..end.div_ceil(CHUNK_SIZE) as usize;
        let mut seen = BTreeSet::new();
        let mut missing = Vec::new();
        for digest in self.range_chunks(reference, range)? {
            if seen.insert(digest) && !self.packs.contains(digest)? {
                missing.push((digest, Kind::Chunk));
            }
        }
        fetcher.hint(&missing);
        Ok(missing)
    }
    /// Asks for these objects, as [`Self::hint_range`] does, with nothing waiting for them.
    pub fn hint(&self, objects: &[(Digest, Kind)]) {
        if let Some(fetcher) = guard(&self.fetcher).clone() {
            fetcher.hint(objects);
        }
    }
    /// Takes back what [`Self::hint_range`] returned.
    pub fn unhint(&self, objects: &[(Digest, Kind)]) {
        if objects.is_empty() {
            return;
        }
        if let Some(fetcher) = guard(&self.fetcher).clone() {
            fetcher.unhint(objects);
        }
    }
    pub fn pin(&self, reference: &FileContentRef) -> Result<Pin> {
        let root_pin = self.packs.pin([reference.digest]);
        let mut chunks = self.chunks(reference)?;
        chunks.push(reference.digest);
        let pin = self.packs.pin(chunks);
        drop(root_pin);
        Ok(pin)
    }
    /// [`Self::pin`] for a draft that shows `visible` bytes of `reference`. One that shows
    /// none needs nothing of it, so this asks for nothing: its manifest may never have been
    /// here.
    pub fn pin_visible(&self, reference: &FileContentRef, visible: u64) -> Result<Pin> {
        if visible == 0 {
            return Ok(self.packs.pin([reference.digest]));
        }
        self.pin(reference)
    }
    pub fn import(&self, bytes: &[u8]) -> Result<FileContentRef> {
        let chunks = if bytes.is_empty() {
            vec![self.packs.put(Kind::Chunk, &[])?]
        } else {
            bytes
                .chunks(CHUNK_SIZE as usize)
                .map(|chunk| self.packs.put(Kind::Chunk, chunk))
                .collect::<Result<Vec<_>>>()?
        };
        let (kind, digest) = if bytes.len() as u64 <= CHUNK_SIZE {
            (ContentKind::Direct, chunks[0])
        } else {
            (
                ContentKind::Manifest,
                self.packs.put(
                    Kind::Manifest,
                    &Manifest {
                        len: bytes.len() as u64,
                        chunks,
                    }
                    .encode()?,
                )?,
            )
        };
        FileContentRef::new(kind, digest, bytes.len() as u64)
    }
}
