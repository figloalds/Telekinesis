//! Content-addressed objects: verified reads and installs, immutable sizes,
//! local availability, the shared (sendable) inventory and per-object locks.
//! Also owns the startup object check (phase 4 of `Store::open`).
use super::*;
use std::{
    fs::OpenOptions,
    io::{Seek, Write},
    sync::{Arc, Mutex, Weak},
};

/// Object-module cache (owned by WP1.8; empty in Wave 0).
#[derive(Default)]
pub(crate) struct Cache {}

/// Startup verification of referenced, locally present objects.
/// `Full` re-hashes every object; `Changed` trusts objects whose recorded
/// markers still match; `Lazy` skips hashing. Wave 0: all behave like `Full`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum VerifyMode {
    #[default]
    Full,
    Changed,
    Lazy,
}

/// Schema hook for object side tables (WP1.8). Wave 0: nothing to create.
pub(super) fn ensure_schema(_store: &mut Store) -> Result<()> {
    Ok(())
}

/// Phase 4 of `Store::open`: reference/size bookkeeping and verification of
/// stored bytes; corrupt content is quarantined, the journal is kept.
pub(super) fn startup_object_check(store: &mut Store, verify: VerifyMode) -> Result<()> {
    // Wave 0: `Changed` and `Lazy` still verify like `Full` (WP1.8 honours markers).
    let _ = verify;
    // Missing content is a supported state. Verify existing bytes once during
    // recovery, quarantine corrupt content, and keep the semantic journal.
    let legacy = store
        .db
        .query_row(
            "SELECT value FROM config WHERE key='availability_v1'",
            [],
            |r| r.get::<_, String>(0),
        )
        .optional()?
        .is_none();
    let events = store.events()?;
    let shared: BTreeSet<_> = events
        .iter()
        .filter(|e| e.branch.shared)
        .flat_map(Event::objects)
        .collect();
    let objects: BTreeSet<_> = events.iter().flat_map(Event::objects).collect();
    for event in &events {
        for object in event.objects() {
            store.db.execute(
                "INSERT OR IGNORE INTO object_references VALUES(?)",
                [object],
            )?;
        }
        for (object, size) in &event.object_sizes {
            store.remember_object_size(object, *size)?;
        }
    }
    for object in objects {
        crate::startup::progress("verifying available objects");
        let path = store.root.join("objects").join(&object);
        if path.is_file() {
            if let Err(error) = store.verify_object(&object) {
                fs::rename(&path, path.with_extension(format!("corrupt-{}", id())))?;
                store.db.execute("INSERT INTO object_errors VALUES(?,?,?) ON CONFLICT(hash) DO UPDATE SET error=excluded.error,at=excluded.at", params![object, format!("{error:#}"), time()])?;
            } else {
                store.remember_object_size(&object, fs::metadata(&path)?.len())?;
                if legacy && shared.contains(&object) {
                    store.db.execute(
                        "INSERT OR IGNORE INTO published_objects VALUES(?)",
                        [&object],
                    )?;
                }
            }
        }
    }
    store.db.execute(
        "INSERT OR IGNORE INTO config VALUES('availability_v1','1')",
        [],
    )?;
    Ok(())
}
impl Store {
    pub fn read_object(&self, object: &str) -> Result<Vec<u8>> {
        ensure!(
            self.object_size(object)? <= MAX_FILE as u64,
            "OBJECT_REQUIRES_STREAMING"
        );
        let mut file = self.open_object(object)?;
        ensure!(
            file.metadata()?.len() <= MAX_FILE as u64,
            "OBJECT_REQUIRES_STREAMING"
        );
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        Ok(bytes)
    }
    pub fn object_size(&self, object: &str) -> Result<u64> {
        self.object_size_if_known(object)?
            .with_context(|| format!("CONTENT_SIZE_UNKNOWN: {object}"))
    }
    /// Legacy journals may reference unavailable bytes without a size descriptor.
    /// Preserve that distinction rather than blocking metadata or inventing a size.
    pub fn object_size_if_known(&self, object: &str) -> Result<Option<u64>> {
        valid_object(object)?;
        if let Some(size) = self
            .db
            .query_row(
                "SELECT size FROM object_sizes WHERE hash=?",
                [object],
                |r| r.get(0),
            )
            .optional()?
        {
            return Ok(Some(size));
        }
        match fs::metadata(self.root.join("objects").join(object)) {
            Ok(metadata) => Ok(Some(metadata.len())),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(object_error(error)),
        }
    }
    pub fn remember_object_size(&self, object: &str, size: u64) -> Result<()> {
        valid_object(object)?;
        ensure!(size <= MAX_CONTENT, "FILE_TOO_LARGE");
        let old: Option<u64> = self
            .db
            .query_row(
                "SELECT size FROM object_sizes WHERE hash=?",
                [object],
                |r| r.get(0),
            )
            .optional()?;
        ensure!(old.is_none_or(|old| old == size), "OBJECT_SIZE_CHANGED");
        self.db.execute(
            "INSERT OR IGNORE INTO object_sizes VALUES(?,?)",
            params![object, size],
        )?;
        Ok(())
    }
    pub fn object_available(&self, object: &str) -> bool {
        valid_object(object).is_ok()
            && fs::metadata(self.root.join("objects").join(object)).is_ok_and(|m| {
                m.is_file() && self.object_size(object).is_ok_and(|size| size == m.len())
            })
    }
    pub fn object_state(&self, object: &str) -> Result<Value> {
        let available = self.object_available(object);
        let size = self.object_size_if_known(object)?;
        let partial = fs::metadata(self.root.join("incoming-objects").join(object))
            .ok()
            .map(|m| m.len());
        let requested: bool = self.db.query_row(
            "SELECT EXISTS(SELECT 1 FROM object_requests WHERE hash=?)",
            [object],
            |r| r.get(0),
        )?;
        let error: Option<String> = self
            .db
            .query_row(
                "SELECT error FROM object_errors WHERE hash=?",
                [object],
                |r| r.get(0),
            )
            .optional()?;
        let state = if available {
            "available"
        } else if error.is_some() {
            "retrying"
        } else if size.is_some() && partial == size {
            "verifying"
        } else if partial.is_some() {
            "downloading"
        } else if requested {
            "queued"
        } else {
            "missing"
        };
        Ok(
            json!({"object":object,"size":size,"size_known":size.is_some(),"state":state,"downloaded":partial.unwrap_or(0),"error":if available { None } else { error }}),
        )
    }
    pub fn open_object(&self, object: &str) -> Result<fs::File> {
        valid_object(object)?;
        let mut file =
            fs::File::open(self.root.join("objects").join(object)).map_err(object_error)?;
        ensure!(file.metadata()?.len() <= MAX_CONTENT, "FILE_TOO_LARGE");
        ensure!(
            stream_hash(&mut file)? == object,
            "CORRUPT_OBJECT: {object}"
        );
        file.rewind()?;
        Ok(file)
    }
    pub fn verify_object(&self, object: &str) -> Result<()> {
        self.open_object(object).map(|_| ())
    }
    pub fn put_stream(&self, source: &mut impl Read) -> Result<String> {
        let temp = self.root.join("objects").join(format!("{}.part", id()));
        let result = (|| {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temp)?;
            let mut digest = Sha256::new();
            let mut buffer = [0; 64 * 1024];
            let mut size = 0u64;
            loop {
                let count = source.read(&mut buffer)?;
                if count == 0 {
                    break;
                }
                size += count as u64;
                ensure!(size <= MAX_CONTENT, "FILE_TOO_LARGE");
                digest.update(&buffer[..count]);
                file.write_all(&buffer[..count])?;
            }
            file.sync_all()?;
            drop(file);
            fault("object_flushed");
            let object = hex::encode(digest.finalize());
            let dest = self.root.join("objects").join(&object);
            if dest.exists() {
                self.verify_object(&object)?;
                fs::remove_file(&temp)?;
            } else {
                install_object(&temp, &dest)?;
            }
            fault("object_installed");
            Ok(object)
        })();
        if result.is_err() {
            let _ = fs::remove_file(temp);
        }
        result
    }
    pub fn put_object(&self, bytes: &[u8]) -> Result<String> {
        ensure!(bytes.len() <= MAX_FILE, "FILE_TOO_LARGE");
        let object = hash(bytes);
        let dest = self.root.join("objects").join(&object);
        if dest.exists() {
            self.verify_object(&object)?;
            return Ok(object);
        }
        let temp = self.root.join("objects").join(format!("{}.part", id()));
        let mut f = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
        drop(f);
        fault("object_flushed");
        // MoveFileExW WRITE_THROUGH below supplies the Windows rename barrier.
        install_object(&temp, &dest)?;
        fault("object_installed");
        Ok(object)
    }
    pub fn shared_object_inventory(&self) -> Result<BTreeSet<String>> {
        let candidates = self
            .db
            .prepare(
                "SELECT hash FROM published_objects UNION SELECT hash FROM received_shared_objects",
            )?
            .query_map([], |r| r.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(candidates
            .into_iter()
            .filter(|h| self.object_available(h))
            .collect())
    }
}

/// Per-object mutual exclusion for concurrent writers of one spool/install
/// (invariant 19). Entries are dropped once no holder remains.
#[derive(Default)]
pub struct ObjectLocks(Mutex<BTreeMap<String, Weak<Mutex<()>>>>);
impl ObjectLocks {
    pub fn for_object(&self, object: &str) -> Arc<Mutex<()>> {
        let mut locks = self.0.lock().unwrap();
        locks.retain(|_, lock| lock.strong_count() > 0);
        if let Some(lock) = locks.get(object).and_then(Weak::upgrade) {
            return lock;
        }
        let lock = Arc::new(Mutex::new(()));
        locks.insert(object.to_owned(), Arc::downgrade(&lock));
        lock
    }
}

#[cfg(windows)]
pub(crate) fn install_object(temp: &Path, dest: &Path) -> Result<()> {
    use std::os::windows::ffi::OsStrExt;
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn MoveFileExW(old: *const u16, new: *const u16, flags: u32) -> i32;
    }
    let old: Vec<_> = temp.as_os_str().encode_wide().chain(Some(0)).collect();
    let new: Vec<_> = dest.as_os_str().encode_wide().chain(Some(0)).collect();
    if unsafe { MoveFileExW(old.as_ptr(), new.as_ptr(), 8) } == 0 {
        bail!(std::io::Error::last_os_error());
    }
    Ok(())
}
#[cfg(not(windows))]
pub(crate) fn install_object(temp: &Path, dest: &Path) -> Result<()> {
    fs::rename(temp, dest)?;
    fs::File::open(dest.parent().unwrap())?.sync_all()?;
    Ok(())
}
