//! Current immutable selections and native COW session handles.
//! Mutable memory/disk staging remains only while the standalone deferred-save
//! route is removed. Regular file bases require a typed descriptor; there is no
//! whole-file CAS reference, verification cache or writable-copy conversion.
use crate::content::{ContentProvider, PinnedContent};
use crate::core::{IO_CHUNK, MAX_CONTENT, id};
use crate::model::content::FileContentRef;
use anyhow::{Context, Result, ensure};
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
}

impl StageSnapshot {
    pub fn len(&self) -> usize {
        match self {
            Self::Bytes(bytes) => bytes.len(),
            Self::File { size, .. } => *size,
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

pub enum Staged {
    Cow(Arc<crate::runtime::content_saves::SaveFile>),
    Content(Arc<ContentSelection>),
    Memory(Vec<u8>),
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
    pub fn from_entry(entry: &crate::core::Entry) -> Result<Self> {
        if entry.kind == "directory" {
            return Ok(Self::Memory(Vec::new()));
        }
        ensure!(entry.kind == "file", "INVALID_KIND");
        let reference = entry
            .content_ref
            .as_ref()
            .context("MISSING_CONTENT_DESCRIPTOR")?;
        reference.validate()?;
        Ok(Self::Content(Arc::new(ContentSelection {
            reference: reference.clone(),
            size: usize::try_from(reference.total_len).context("FILE_TOO_LARGE")?,
            binding: OnceLock::new(),
        })))
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
    /// A stable view of these bytes for an off-lock save (module docs). For a
    /// staging file the caller must keep the file from being written until
    /// the snapshot is dropped.
    pub fn snapshot(&self) -> Result<StageSnapshot> {
        Ok(match self {
            Self::Content(_) | Self::Cow(_) => {
                anyhow::bail!("CONTENT_WRITE_REQUIRES_PRIVATE_SESSION")
            }
            Self::Memory(bytes) => StageSnapshot::Bytes(bytes.clone()),
            Self::Disk { file, size, .. } => StageSnapshot::File {
                file: file.try_clone()?,
                size: *size,
            },
        })
    }
    /// Whether the bytes are a staging file (a save then holds writers back,
    /// [`Staged::snapshot`]).
    pub fn is_file(&self) -> bool {
        matches!(self, Self::Disk { .. })
    }
    pub fn len(&self) -> usize {
        match self {
            Self::Cow(file) => usize::try_from(file.visible_len()).unwrap_or(usize::MAX),
            Self::Content(selection) => selection.size,
            Self::Memory(b) => b.len(),
            Self::Disk { size, .. } => *size,
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn spool(&mut self, root: &Path) -> Result<()> {
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
            "CONTENT_WRITE_REQUIRES_PRIVATE_SESSION"
        );
        ensure!(size as u64 <= MAX_CONTENT, "FILE_TOO_LARGE");
        if size > MEMORY_LIMIT {
            self.spool(root)?;
        }
        match self {
            Self::Memory(bytes) => bytes.resize(size, 0),
            Self::Content(_) | Self::Cow(_) => unreachable!(),
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
            "CONTENT_WRITE_REQUIRES_PRIVATE_SESSION"
        );
        let end = offset
            .checked_add(bytes.len())
            .ok_or_else(|| anyhow::anyhow!("FILE_TOO_LARGE"))?;
        if end > self.len() {
            self.resize(root, end)?;
        }
        match self {
            Self::Memory(buffer) => buffer[offset..end].copy_from_slice(bytes),
            Self::Content(_) | Self::Cow(_) => unreachable!(),
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
                "CONTENT_UNAVAILABLE: descriptor read requires the off-lock provider path"
            ),
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
            "CONTENT_WRITE_REQUIRES_PRIVATE_SESSION"
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
