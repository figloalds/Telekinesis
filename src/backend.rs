//! Optional immutable object backends. The directory adapter is a LOCAL test
//! bucket, not a remote-service durability/authentication qualification.
use crate::core::{MAX_FILE, fault, hash, install_object};
use anyhow::{Context, Result, ensure};
use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};
pub trait ObjectBackend: Send + Sync {
    fn get_verified(&self, object: &str) -> Result<Vec<u8>>;
    fn put_verified(&self, bytes: &[u8]) -> Result<String>;
}
pub struct DirectoryBackend {
    root: PathBuf,
}
impl DirectoryBackend {
    pub fn new(root: &Path) -> Result<Self> {
        fs::create_dir_all(root)?;
        Ok(Self {
            root: root.to_owned(),
        })
    }
}
impl ObjectBackend for DirectoryBackend {
    fn get_verified(&self, object: &str) -> Result<Vec<u8>> {
        ensure!(
            object.len() == 64 && object.bytes().all(|c| c.is_ascii_hexdigit()),
            "INVALID_OBJECT_ID"
        );
        let bytes = fs::read(self.root.join(object)).context("OFFLINE_OBJECT_MISSING")?;
        ensure!(hash(&bytes) == object, "CORRUPT_OBJECT: {object}");
        Ok(bytes)
    }
    fn put_verified(&self, bytes: &[u8]) -> Result<String> {
        ensure!(bytes.len() <= MAX_FILE, "FILE_TOO_LARGE");
        let object = hash(bytes);
        let dest = self.root.join(&object);
        if dest.exists() {
            self.get_verified(&object)?;
            return Ok(object);
        }
        let temp = self.root.join(format!("{}.part", crate::core::id()));
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        fault("object_flushed");
        install_object(&temp, &dest)?;
        fault("object_installed");
        Ok(object)
    }
}
