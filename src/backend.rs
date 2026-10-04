//! Optional immutable object backends. The directory adapter is a LOCAL test
//! bucket, not a remote-service durability/authentication qualification.
use crate::core::{MAX_CONTENT, MAX_FILE, fault, hash, install_object, stream_hash};
use anyhow::{Context, Result, ensure};
use std::{
    fs::{self, OpenOptions},
    io::{Read, Seek, Write},
    path::{Path, PathBuf},
};
pub trait ObjectBackend: Send + Sync {
    fn get_verified(&self, object: &str) -> Result<Vec<u8>>;
    fn put_verified(&self, bytes: &[u8]) -> Result<String>;
    fn put_stream(&self, source: &mut dyn Read) -> Result<String> {
        let mut bytes = Vec::new();
        source.take(MAX_FILE as u64 + 1).read_to_end(&mut bytes)?;
        ensure!(bytes.len() <= MAX_FILE, "BACKEND_REQUIRES_STREAMING");
        self.put_verified(&bytes)
    }
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
        ensure!(
            fs::metadata(self.root.join(object))?.len() <= MAX_FILE as u64,
            "OBJECT_REQUIRES_STREAMING"
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
    fn put_stream(&self, source: &mut dyn Read) -> Result<String> {
        let temp = self.root.join(format!("{}.part", crate::core::id()));
        let result = (|| {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&temp)?;
            let size = std::io::copy(&mut source.take(MAX_CONTENT + 1), &mut file)?;
            ensure!(size <= MAX_CONTENT, "FILE_TOO_LARGE");
            file.sync_all()?;
            file.rewind()?;
            let object = stream_hash(&mut file)?;
            drop(file);
            let dest = self.root.join(&object);
            if dest.exists() {
                ensure!(
                    stream_hash(&mut fs::File::open(&dest)?)? == object,
                    "CORRUPT_OBJECT"
                );
                fs::remove_file(&temp)?;
            } else {
                install_object(&temp, &dest)?;
            }
            Ok(object)
        })();
        if result.is_err() {
            let _ = fs::remove_file(temp);
        }
        result
    }
}
