//! Bounded-memory random-access staging. Failed saves keep their spool on disk.
use crate::core::{IO_CHUNK, MAX_CONTENT, Store, id};
use anyhow::{Result, ensure};
use std::{
    fs::{self, File, OpenOptions},
    io::{Cursor, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::OnceLock,
};

const MEMORY_LIMIT: usize = 256 * 1024;
pub enum Staged {
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
    pub fn reference(&self) -> Option<(&Path, &str, usize)> {
        if let Self::Reference {
            path,
            object,
            size,
            verified,
        } = self
            && verified.get().is_none()
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
    pub fn len(&self) -> usize {
        match self {
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
            let folder = root.join("staging");
            fs::create_dir_all(&folder)?;
            let path = folder.join(format!("{}.stage", id()));
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&path)?;
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
            Self::Reference { .. } => unreachable!(),
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
        self.make_writable(root)?;
        let end = offset
            .checked_add(bytes.len())
            .ok_or_else(|| anyhow::anyhow!("FILE_TOO_LARGE"))?;
        if end > self.len() {
            self.resize(root, end)?;
        }
        match self {
            Self::Memory(buffer) => buffer[offset..end].copy_from_slice(bytes),
            Self::Reference { .. } => unreachable!(),
            Self::Disk { file, .. } => {
                file.seek(SeekFrom::Start(offset as u64))?;
                file.write_all(bytes)?;
            }
        }
        Ok(())
    }
    pub fn read_at(&self, offset: usize, bytes: &mut [u8]) -> Result<usize> {
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
