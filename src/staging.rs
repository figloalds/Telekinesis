//! Bounded-memory random-access staging. Failed saves keep their spool on disk.
//! Native COW sessions expose cached length here; their byte I/O and durable
//! save paths are owned by runtime::content_writes outside Engine.
//!
//! WP5.2 (CCR-WP4.INT-5, the staging part):
//! * [`Staged::snapshot`] is a stable view of a session's bytes for a save
//!   that stores them without the engine lock (`runtime::engine`): a copy of
//!   the in-memory bytes (at most [`MEMORY_LIMIT`]), or a second handle to the
//!   staging file, which the engine keeps from being written until the save
//!   ends (writers of that file wait off the lock), or the object a clean
//!   reference already is.
//! * [`writable_copy`] makes the private writable copy of a large object
//!   (`Reference` above [`MEMORY_LIMIT`]) that its first write needs, off the
//!   engine lock: it copies and verifies in one pass, and the engine installs
//!   it only if the session still references the same object
//!   ([`Staged::install_writable`]). The first write then no longer copies the
//!   whole file under the lock.
use crate::content::{ContentProvider, PinnedContent};
use crate::core::{IO_CHUNK, MAX_CONTENT, Store, id};
use crate::model::content::FileContentRef;
use anyhow::{Result, ensure};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{Cursor, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
};

#[derive(Clone)]
pub(crate) struct ContentBinding {
    pub provider: Arc<dyn ContentProvider>,
    pub pin: PinnedContent,
}
/// An immutable descriptor selection. Binding/pin acquisition occurs outside
/// the engine mutex; clones keep an in-flight selection alive across refresh.
pub struct ContentSelection {
    reference: FileContentRef,
    size: usize,
    binding: OnceLock<ContentBinding>,
}
impl ContentSelection {
    pub fn reference(&self) -> &FileContentRef {
        &self.reference
    }
    pub(crate) fn binding(&self) -> Option<ContentBinding> {
        self.binding.get().cloned()
    }
    pub(crate) fn bind(&self, binding: ContentBinding) -> Option<ContentBinding> {
        self.binding.set(binding).err()
    }
}

/// Sessions up to this size stage in memory; larger ones in a staging file.
pub const MEMORY_LIMIT: usize = 256 * 1024;

/// A stable view of a session's staged bytes ([`Staged::snapshot`]).
pub enum StageSnapshot {
    /// A copy of in-memory bytes.
    Bytes(Vec<u8>),
    /// A second handle to the staging file and its length. Read only with
    /// positional reads ([`read_file_at`]): the handle shares its file
    /// position with the session's.
    File { file: File, size: usize },
    /// The bytes are already the object `object` (an unmodified reference).
    Object { object: String, size: usize },
}

impl StageSnapshot {
    pub fn len(&self) -> usize {
        match self {
            Self::Bytes(bytes) => bytes.len(),
            Self::File { size, .. } | Self::Object { size, .. } => *size,
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Reads `bytes.len()` bytes at `offset` of `file` (positional; never moves
/// a position another handle relies on). `STAGING_TRUNCATED` at end of file.
pub fn read_file_at(file: &File, offset: u64, bytes: &mut [u8]) -> Result<()> {
    #[cfg(unix)]
    use std::os::unix::fs::FileExt;
    #[cfg(windows)]
    use std::os::windows::fs::FileExt;
    let mut read = 0;
    while read < bytes.len() {
        #[cfg(windows)]
        let n = file.seek_read(&mut bytes[read..], offset + read as u64)?;
        #[cfg(unix)]
        let n = file.read_at(&mut bytes[read..], offset + read as u64)?;
        ensure!(n > 0, "STAGING_TRUNCATED");
        read += n;
    }
    Ok(())
}

/// A new staging file under `root/staging` (not retained).
fn new_stage(root: &Path) -> Result<(File, PathBuf)> {
    let folder = root.join("staging");
    fs::create_dir_all(&folder)?;
    let path = folder.join(format!("{}.stage", id()));
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&path)?;
    Ok((file, path))
}

/// The writable copy of the object at `path` (`object`, `size` bytes) as a
/// staging file, verified while it is copied (one read of the object). Runs
/// without the engine lock (module docs). The copy is removed again when the
/// result is dropped without being installed.
pub fn writable_copy(root: &Path, path: &Path, object: &str, size: usize) -> Result<Staged> {
    let mut source =
        File::open(path).map_err(|e| anyhow::anyhow!("CONTENT_UNAVAILABLE: {object}: {e}"))?;
    ensure!(
        source.metadata()?.len() == size as u64,
        "CORRUPT_OBJECT: {object}"
    );
    let (file, path) = new_stage(root)?;
    // Removed on drop unless installed.
    let mut staged = Staged::Disk {
        file,
        path,
        size,
        retained: false,
    };
    let Staged::Disk { file, .. } = &mut staged else {
        unreachable!()
    };
    let mut digest = Sha256::new();
    let mut buffer = vec![0; IO_CHUNK];
    let mut copied = 0usize;
    loop {
        let n = source.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        copied += n;
        ensure!(copied <= size, "CORRUPT_OBJECT: {object}");
        digest.update(&buffer[..n]);
        file.write_all(&buffer[..n])?;
    }
    ensure!(
        copied == size && hex::encode(digest.finalize()) == object,
        "CORRUPT_OBJECT: {object}"
    );
    Ok(staged)
}
pub enum Staged {
    Cow(Arc<crate::runtime::content_saves::SaveFile>),
    Content(Arc<ContentSelection>),
    Memory(Vec<u8>),
    Reference {
        path: PathBuf,
        object: String,
        size: usize,
        verified: OnceLock<File>,
    },
    Disk {
        file: File,
        path: PathBuf,
        size: usize,
        retained: bool,
    },
}
impl Staged {
    pub(crate) fn cow_file(&self) -> Option<Arc<crate::runtime::content_saves::SaveFile>> {
        match self {
            Self::Cow(file) => Some(file.clone()),
            _ => None,
        }
    }
    pub fn from_entry(store: &Store, entry: &crate::core::Entry) -> Result<Self> {
        if let Some(reference) = &entry.content_ref {
            reference.validate()?;
            return Ok(Self::Content(Arc::new(ContentSelection {
                reference: reference.clone(),
                size: usize::try_from(reference.total_len)
                    .map_err(|_| anyhow::anyhow!("FILE_TOO_LARGE"))?,
                binding: OnceLock::new(),
            })));
        }
        Self::from_object(store, entry.content.as_deref())
    }
    pub fn content_selection(&self) -> Option<Arc<ContentSelection>> {
        match self {
            Self::Content(selection) => Some(selection.clone()),
            _ => None,
        }
    }
    pub fn memory(bytes: Vec<u8>) -> Self {
        Self::Memory(bytes)
    }
    pub fn from_object(store: &Store, object: Option<&str>) -> Result<Self> {
        let Some(object) = object else {
            return Ok(Self::memory(vec![]));
        };
        Ok(Self::Reference {
            path: store.root.join("objects").join(object),
            object: object.to_owned(),
            size: store.object_size(object)? as usize,
            verified: OnceLock::new(),
        })
    }
    /// `(path, object, size)` of a reference whose bytes were not verified
    /// yet (`runtime::prepare_content` verifies them off the engine lock).
    /// An empty one has no bytes to verify (every new file's session is one).
    pub fn reference(&self) -> Option<(&Path, &str, usize)> {
        if let Self::Reference {
            path,
            object,
            size,
            verified,
        } = self
            && verified.get().is_none()
            && *size > 0
        {
            return Some((path, object, *size));
        }
        None
    }
    pub fn prime(&self, file: File) {
        if let Self::Reference { verified, .. } = self {
            let _ = verified.set(file);
        }
    }
    fn verified_file(&self) -> Result<&File> {
        let Self::Reference {
            path,
            object,
            size,
            verified,
        } = self
        else {
            unreachable!()
        };
        if verified.get().is_none() {
            let mut file = File::open(path)
                .map_err(|e| anyhow::anyhow!("CONTENT_UNAVAILABLE: {object}: {e}"))?;
            ensure!(
                file.metadata()?.len() == *size as u64
                    && crate::core::stream_hash(&mut file)? == *object,
                "CORRUPT_OBJECT"
            );
            let _ = verified.set(file);
        }
        Ok(verified.get().unwrap())
    }
    /// A stable view of these bytes for an off-lock save (module docs). For a
    /// staging file the caller must keep the file from being written until
    /// the snapshot is dropped.
    pub fn snapshot(&self) -> Result<StageSnapshot> {
        Ok(match self {
            Self::Content(_) | Self::Cow(_) => {
                anyhow::bail!("PEER_NEEDS_UPGRADE: content write callbacks are not enabled")
            }
            Self::Memory(bytes) => StageSnapshot::Bytes(bytes.clone()),
            Self::Disk { file, size, .. } => StageSnapshot::File {
                file: file.try_clone()?,
                size: *size,
            },
            Self::Reference { object, size, .. } => StageSnapshot::Object {
                object: object.clone(),
                size: *size,
            },
        })
    }
    /// Whether the bytes are a staging file (a save then holds writers back,
    /// [`Staged::snapshot`]).
    pub fn is_file(&self) -> bool {
        matches!(self, Self::Disk { .. })
    }
    /// `(path, object, size)` of a reference whose first write would copy it
    /// whole into a staging file (larger than [`MEMORY_LIMIT`]): the copy
    /// [`writable_copy`] makes off the engine lock.
    pub fn large_reference(&self) -> Option<(PathBuf, String, usize)> {
        match self {
            Self::Reference {
                path, object, size, ..
            } if *size > MEMORY_LIMIT => Some((path.clone(), object.clone(), *size)),
            _ => None,
        }
    }
    /// Replaces a reference to `object` (`size` bytes) with its writable copy
    /// ([`writable_copy`]); `false`, and the copy is dropped (removed), when
    /// these bytes are no longer that reference.
    pub fn install_writable(&mut self, object: &str, size: usize, copy: Staged) -> bool {
        let same =
            matches!(self, Self::Reference { object: o, size: s, .. } if o == object && *s == size);
        if same && copy.is_file() && copy.len() == size {
            *self = copy;
            return true;
        }
        false
    }
    pub fn len(&self) -> usize {
        match self {
            Self::Cow(file) => usize::try_from(file.visible_len()).unwrap_or(usize::MAX),
            Self::Content(selection) => selection.size,
            Self::Memory(b) => b.len(),
            Self::Reference { size, .. } => *size,
            Self::Disk { size, .. } => *size,
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn make_writable(&mut self, root: &Path) -> Result<()> {
        if matches!(self, Self::Reference { .. }) {
            if self.len() <= MEMORY_LIMIT {
                let mut bytes = Vec::with_capacity(self.len());
                if !self.is_empty() {
                    let mut source = self.verified_file()?.try_clone()?;
                    source.rewind()?;
                    source.read_to_end(&mut bytes)?;
                }
                *self = Self::Memory(bytes);
            } else {
                self.spool(root)?;
            }
        }
        Ok(())
    }
    fn spool(&mut self, root: &Path) -> Result<()> {
        if matches!(self, Self::Reference { .. }) {
            let mut source = self.verified_file()?.try_clone()?;
            source.rewind()?;
            let mut staged = Self::Memory(vec![]);
            staged.spool(root)?;
            if let Self::Disk { file, size, .. } = &mut staged {
                *size = std::io::copy(&mut source, file)? as usize;
            }
            *self = staged;
        }
        if let Self::Memory(bytes) = self {
            let (mut file, path) = new_stage(root)?;
            file.write_all(bytes)?;
            *self = Self::Disk {
                file,
                path,
                size: bytes.len(),
                retained: false,
            };
        }
        Ok(())
    }
    pub fn resize(&mut self, root: &Path, size: usize) -> Result<()> {
        ensure!(
            !matches!(self, Self::Content(_) | Self::Cow(_)),
            "PEER_NEEDS_UPGRADE: content write callbacks are not enabled"
        );
        ensure!(size as u64 <= MAX_CONTENT, "FILE_TOO_LARGE");
        if size == 0 && matches!(self, Self::Reference { .. }) {
            *self = Self::Memory(vec![]);
        }
        if size > MEMORY_LIMIT {
            self.spool(root)?;
        } else {
            self.make_writable(root)?;
        }
        match self {
            Self::Memory(bytes) => bytes.resize(size, 0),
            Self::Reference { .. } | Self::Content(_) | Self::Cow(_) => unreachable!(),
            Self::Disk {
                file, size: length, ..
            } => {
                file.set_len(size as u64)?;
                *length = size;
            }
        }
        Ok(())
    }
    pub fn write_at(&mut self, root: &Path, offset: usize, bytes: &[u8]) -> Result<()> {
        ensure!(
            !matches!(self, Self::Content(_) | Self::Cow(_)),
            "PEER_NEEDS_UPGRADE: content write callbacks are not enabled"
        );
        self.make_writable(root)?;
        let end = offset
            .checked_add(bytes.len())
            .ok_or_else(|| anyhow::anyhow!("FILE_TOO_LARGE"))?;
        if end > self.len() {
            self.resize(root, end)?;
        }
        match self {
            Self::Memory(buffer) => buffer[offset..end].copy_from_slice(bytes),
            Self::Reference { .. } | Self::Content(_) | Self::Cow(_) => unreachable!(),
            Self::Disk { file, .. } => {
                file.seek(SeekFrom::Start(offset as u64))?;
                file.write_all(bytes)?;
            }
        }
        Ok(())
    }
    pub fn read_at(&self, offset: usize, bytes: &mut [u8]) -> Result<usize> {
        ensure!(
            !matches!(self, Self::Content(_) | Self::Cow(_)),
            "CONTENT_UNAVAILABLE: descriptor read requires the off-lock provider path"
        );
        let count = bytes.len().min(self.len().saturating_sub(offset));
        if count == 0 {
            return Ok(0);
        }
        if let Self::Memory(buffer) = self {
            bytes[..count].copy_from_slice(&buffer[offset..offset + count]);
        } else {
            let file = match self {
                Self::Disk { file, .. } => file,
                Self::Reference { .. } => self.verified_file()?,
                _ => unreachable!(),
            };
            #[cfg(unix)]
            use std::os::unix::fs::FileExt;
            #[cfg(windows)]
            use std::os::windows::fs::FileExt;
            let mut read = 0;
            while read < count {
                #[cfg(windows)]
                let n = file.seek_read(&mut bytes[read..count], (offset + read) as u64)?;
                #[cfg(unix)]
                let n = file.read_at(&mut bytes[read..count], (offset + read) as u64)?;
                ensure!(n > 0, "STAGING_TRUNCATED");
                read += n;
            }
        }
        Ok(count)
    }
    pub fn reader(&mut self) -> Result<Box<dyn Read + '_>> {
        Ok(match self {
            Self::Content(_) | Self::Cow(_) => anyhow::bail!(
                "PEER_NEEDS_UPGRADE: content materialization callbacks are not enabled"
            ),
            Self::Reference { .. } => {
                let mut file = self.verified_file()?.try_clone()?;
                file.rewind()?;
                Box::new(file)
            }
            Self::Memory(bytes) => Box::new(Cursor::new(bytes.as_slice())),
            Self::Disk { file, .. } => {
                file.rewind()?;
                Box::new(file)
            }
        })
    }
    pub fn retain(&mut self, root: &Path) -> Result<String> {
        ensure!(
            !matches!(self, Self::Content(_) | Self::Cow(_)),
            "PEER_NEEDS_UPGRADE: content write callbacks are not enabled"
        );
        self.spool(root)?;
        let Self::Disk {
            file,
            path,
            retained,
            ..
        } = self
        else {
            unreachable!()
        };
        file.sync_all()?;
        // The caller commits a `pending_saves` row naming this file next; make
        // its directory entry durable first, or a power loss can keep the row
        // and lose the file (CCR-WP0.INT-3). Unix only, like the object
        // install in `core::objects` (std cannot open a directory on Windows).
        #[cfg(unix)]
        if let Some(folder) = path.parent() {
            File::open(folder)?.sync_all()?;
        }
        *retained = true;
        Ok(path.file_name().unwrap().to_string_lossy().into_owned())
    }
    pub fn recover(root: &Path, name: &str) -> Result<Self> {
        ensure!(
            name.ends_with(".stage")
                && uuid::Uuid::parse_str(name.trim_end_matches(".stage")).is_ok(),
            "INVALID_RECOVERY_PATH"
        );
        let path = root.join("staging").join(name);
        let file = OpenOptions::new().read(true).write(true).open(&path)?;
        let size = file.metadata()?.len();
        ensure!(size <= MAX_CONTENT, "FILE_TOO_LARGE");
        Ok(Self::Disk {
            file,
            path,
            size: size as usize,
            retained: true,
        })
    }
    pub fn release(&mut self) {
        if let Self::Disk { retained, .. } = self {
            *retained = false;
        }
    }
}
impl Drop for Staged {
    fn drop(&mut self) {
        if let Self::Disk {
            path,
            retained: false,
            ..
        } = self
        {
            let _ = fs::remove_file(path);
        }
    }
}
impl std::fmt::Debug for Staged {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Staged({} bytes)", self.len())
    }
}
impl<T: AsRef<[u8]>> PartialEq<T> for Staged {
    fn eq(&self, rhs: &T) -> bool {
        let expected = rhs.as_ref();
        if self.len() != expected.len() {
            return false;
        }
        let mut buffer = vec![0; IO_CHUNK];
        for (i, bytes) in expected.chunks(IO_CHUNK).enumerate() {
            if self
                .read_at(i * IO_CHUNK, &mut buffer[..bytes.len()])
                .is_err()
                || &buffer[..bytes.len()] != bytes
            {
                return false;
            }
        }
        true
    }
}
