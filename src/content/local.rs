//! Append-only packs. No per-object flush, rename, or separate loose files.
use crate::model::content::{CHUNK_SIZE, ContentKind, Digest, FileContentRef};
use crate::util::sqlite::CachedConnection;
use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, Weak},
};

pub const SEGMENT_BYTES: u64 = 64 * 1024 * 1024;
pub const CACHE_BYTES: usize = 256 * 1024 * 1024;
const MAX_OBJECT_BYTES: usize = 64 * 1024 * 1024;

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
    readers: BTreeMap<u64, Arc<File>>,
    cache: BTreeMap<Digest, Cached>,
    order: BTreeSet<(u64, Digest)>,
    cache_len: usize,
    stamp: u64,
    pins: BTreeMap<Digest, usize>,
    verified_reads: u64,
}
struct Inner {
    root: PathBuf,
    state: Mutex<State>,
    cache_limit: usize,
    segment_limit: u64,
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
            state: Mutex::new(State {
                db,
                segment,
                writer,
                offset: 0,
                pending: BTreeMap::new(),
                readers: BTreeMap::new(),
                cache: BTreeMap::new(),
                order: BTreeSet::new(),
                cache_len: 0,
                stamp: 0,
                pins: BTreeMap::new(),
                verified_reads: 0,
            }),
        });
        stores.insert(root, Arc::downgrade(&inner));
        Ok(Self { inner })
    }
    fn location(state: &State, digest: Digest) -> Result<Option<Location>> {
        if let Some(location) = state.pending.get(&digest) {
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
        row.map(|(segment, offset, len, k)| {
            Ok(Location {
                segment,
                offset,
                len,
                kind: kind(k)?,
            })
        })
        .transpose()
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
        if let Some(old) = Self::location(&state, digest)? {
            ensure!(old.kind == kind, "OBJECT_KIND_COLLISION");
            return Ok((digest, false));
        }
        if state.offset > 0 && state.offset + bytes.len() as u64 > self.inner.segment_limit {
            Self::flush_locked(&mut state)?;
            state.segment += 1;
            state.writer = append_file(&self.inner.root, state.segment)?;
            state.offset = 0;
        }
        let location = Location {
            segment: state.segment,
            offset: state.offset,
            len: bytes.len(),
            kind,
        };
        state.writer.write_all(bytes)?;
        state.offset += bytes.len() as u64;
        state.pending.insert(digest, location);
        Ok((digest, true))
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
        let location = Self::location(&state, digest)?.context("OBJECT_MISSING")?;
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
            state
                .db
                .execute_cached("DELETE FROM objects WHERE digest=?1", [&digest.0[..]])?;
            state.pending.remove(&digest);
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
    pub fn contains(&self, digest: Digest) -> Result<bool> {
        Ok(Self::location(&guard(&self.inner.state), digest)?.is_some())
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
        let mut state = guard(&self.inner.state);
        Self::flush_locked(&mut state)?;
        let mut live = live.clone();
        live.extend(state.pins.keys());
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
        let mut removed = 0;
        for (segment, entries) in segments {
            if segment == state.segment {
                continue;
            }
            let total = entries.iter().map(|(_, l)| l.len.max(1)).sum::<usize>();
            let kept = entries
                .iter()
                .filter(|(d, _)| live.contains(d))
                .map(|(_, l)| l.len.max(1))
                .sum::<usize>();
            if kept as f64 / total as f64 >= threshold {
                continue;
            }
            let old = File::open(segment_path(&self.inner.root, segment))?;
            let mut updates = Vec::new();
            for (digest, loc) in &entries {
                if !live.contains(digest) {
                    continue;
                }
                let mut bytes = vec![0; loc.len];
                read_exact_at(&old, &mut bytes, loc.offset)?;
                ensure!(Digest::bytes(&bytes) == *digest, "OBJECT_CORRUPT_DURING_GC");
                let offset = state.offset;
                state.writer.write_all(&bytes)?;
                state.offset += bytes.len() as u64;
                updates.push((*digest, offset, *loc));
            }
            state.writer.sync_all()?;
            let new_segment = state.segment;
            let tx = state.db.transaction()?;
            tx.execute_cached("DELETE FROM objects WHERE segment=?1", [segment])?;
            for (digest, offset, loc) in updates {
                tx.execute_cached(
                    "INSERT INTO objects VALUES(?1,?2,?3,?4,?5)",
                    params![&digest.0[..], new_segment, offset, loc.len, loc.kind as u8],
                )?;
            }
            tx.commit()?;
            removed += entries.iter().filter(|(d, _)| !live.contains(d)).count();
            state.readers.remove(&segment);
            drop(old);
            fs::remove_file(segment_path(&self.inner.root, segment))?;
        }
        // Cached dead objects must not turn a miss into a successful read.
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
        Ok(removed)
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
        let location = PackStore::location(&guard(&content.packs.inner.state), digest)?.unwrap();
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
    pub fn pin(&self, reference: &FileContentRef) -> Result<Pin> {
        let root_pin = self.packs.pin([reference.digest]);
        let mut chunks = self.chunks(reference)?;
        chunks.push(reference.digest);
        let pin = self.packs.pin(chunks);
        drop(root_pin);
        Ok(pin)
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
