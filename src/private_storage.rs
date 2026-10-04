//! Fail-closed pairing paths. No existing ownership/ACL is silently repaired.
#[cfg(target_os = "linux")]
use anyhow::Context;
use anyhow::{Result, ensure};
use std::{
    fs::File,
    path::{Path, PathBuf},
};

pub struct Directory {
    path: PathBuf,
    #[cfg(windows)]
    _handles: Vec<File>,
}

#[cfg(all(test, windows))]
mod tests {
    use super::*;
    unsafe extern "C" {
        fn tk_storage_validate_descriptor(descriptor: *mut std::ffi::c_void) -> u32;
    }
    #[link(name = "advapi32")]
    unsafe extern "system" {
        fn ConvertStringSecurityDescriptorToSecurityDescriptorW(
            text: *const u16,
            revision: u32,
            out: *mut *mut std::ffi::c_void,
            size: *mut u32,
        ) -> i32;
    }
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn LocalFree(memory: *mut std::ffi::c_void) -> *mut std::ffi::c_void;
    }
    fn descriptor_result(text: &str) -> u32 {
        let text: Vec<_> = text.encode_utf16().chain(Some(0)).collect();
        let mut descriptor = std::ptr::null_mut();
        assert_ne!(
            unsafe {
                ConvertStringSecurityDescriptorToSecurityDescriptorW(
                    text.as_ptr(),
                    1,
                    &mut descriptor,
                    std::ptr::null_mut(),
                )
            },
            0
        );
        let result = unsafe { tk_storage_validate_descriptor(descriptor) };
        unsafe { LocalFree(descriptor) };
        result
    }
    fn acl(path: &Path) -> Vec<u8> {
        let output = std::process::Command::new("icacls.exe")
            .arg(path)
            .output()
            .unwrap();
        assert!(output.status.success());
        output.stdout
    }
    fn grant_foreign_write(path: &Path) {
        let output = std::process::Command::new("icacls.exe")
            .arg(path)
            .args(["/grant", "*S-1-1-0:(W)"])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "disposable ACL fixture creation failed"
        );
    }
    #[test]
    fn actual_owner_descriptor_rejects_foreign_sid_null_dacl_and_foreign_write() {
        let sid = crate::local_ipc::owner_sid().unwrap();
        assert_eq!(descriptor_result(&format!("O:{sid}D:P(A;;GA;;;{sid})")), 0);
        for descriptor in [
            format!("O:SYD:P(A;;GA;;;{sid})"),
            format!("O:{sid}D:NO_ACCESS_CONTROL"),
            format!("O:{sid}D:P(A;;GA;;;{sid})(A;;GW;;;WD)"),
        ] {
            assert_ne!(descriptor_result(&descriptor), 0);
        }
    }
    #[test]
    fn existing_writable_registry_sidecars_and_directory_are_rejected_without_acl_repair() {
        let temp = tempfile::tempdir().unwrap();
        for name in [
            "pairs.sqlite",
            "pairs.sqlite-wal",
            "pairs.sqlite-shm",
            "pairs.sqlite-journal",
            "service.lock",
        ] {
            let root = temp.path().join(name.replace('.', "_"));
            let directory = Directory::open(&root).unwrap();
            let file = root.join(name);
            drop(directory.file(&file, true, false).unwrap());
            std::fs::write(&file, b"untrusted fixture bytes").unwrap();
            grant_foreign_write(&file);
            let before = acl(&file);
            assert!(directory.validate_existing_files().is_err());
            assert!(
                crate::pairing::Registry::open(&root.join("pairs.sqlite"), &crate::core::id())
                    .is_err()
            );
            assert_eq!(acl(&file), before);
            assert_eq!(std::fs::read(&file).unwrap(), b"untrusted fixture bytes");
        }
        let root = temp.path().join("unsafe-directory");
        drop(Directory::open(&root).unwrap());
        grant_foreign_write(&root);
        let before = acl(&root);
        assert!(Directory::open(&root).is_err());
        assert_eq!(acl(&root), before);
        let ancestor = temp.path().join("unsafe-ancestor");
        drop(Directory::open(&ancestor).unwrap());
        let child = ancestor.join("private-child");
        drop(Directory::open(&child).unwrap());
        grant_foreign_write(&ancestor);
        let before = acl(&ancestor);
        assert!(Directory::open(&child).is_err());
        assert_eq!(acl(&ancestor), before);
    }
    #[test]
    fn directory_and_ancestor_junctions_hardlinks_and_path_replacement_are_refused() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("private");
        let directory = Directory::open(&root).unwrap();
        use std::os::windows::fs::OpenOptionsExt;
        assert!(
            std::fs::OpenOptions::new()
                .access_mode(0x40000000)
                .custom_flags(0x02000000 | 0x00200000)
                .open(&root)
                .is_err()
        );
        assert!(std::fs::rename(&root, temp.path().join("replacement")).is_err());
        let file = root.join("pairs.sqlite");
        let held = directory.file(&file, true, false).unwrap();
        assert!(std::fs::rename(&file, root.join("swapped")).is_err());
        drop(held);
        drop(directory);
        std::fs::hard_link(&file, root.join("alias")).unwrap();
        let directory = Directory::open(&root).unwrap();
        assert!(directory.file(&file, false, false).is_err());
        let junction = temp.path().join("junction");
        let created = std::process::Command::new("cmd.exe")
            .args(["/c", "mklink", "/J"])
            .arg(&junction)
            .arg(&root)
            .output()
            .unwrap();
        assert!(
            created.status.success(),
            "disposable junction creation failed"
        );
        assert!(Directory::open(&junction).is_err());
        assert!(Directory::open(&junction.join("descendant")).is_err());
        std::fs::remove_dir(junction).unwrap();
    }
}
impl Directory {
    pub fn open(path: &Path) -> Result<Self> {
        let path = std::path::absolute(path)?;
        #[cfg(windows)]
        {
            let mut handles = vec![];
            let mut ancestors: Vec<_> = path.ancestors().collect();
            ancestors.reverse();
            for component in ancestors {
                let final_component = component == path;
                handles.push(crate::local_ipc::storage_handle(
                    component,
                    1 | 2 | if final_component { 4 } else { 32 },
                )?);
            }
            Ok(Self {
                path,
                _handles: handles,
            })
        }
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::fs::{DirBuilderExt, MetadataExt};
            if !path.exists() {
                std::fs::DirBuilder::new()
                    .recursive(true)
                    .mode(0o700)
                    .create(&path)?;
            }
            // A private leaf beneath a foreign-owned or writable ancestor can
            // still be swapped. Permit the root-owned sticky /tmp namespace;
            // ordinary ancestors must be root/this UID and not writable by others.
            for ancestor in path.ancestors() {
                let metadata = std::fs::symlink_metadata(ancestor)?;
                let trusted_owner =
                    metadata.uid() == 0 || metadata.uid() == unsafe { libc::geteuid() };
                let trusted_sticky = metadata.uid() == 0 && metadata.mode() & 0o1000 != 0;
                ensure!(
                    metadata.is_dir()
                        && !metadata.file_type().is_symlink()
                        && trusted_owner
                        && (metadata.mode() & 0o022 == 0 || trusted_sticky),
                    "PAIRING_ANCESTOR_UNSAFE"
                );
            }
            let metadata = std::fs::symlink_metadata(&path)?;
            ensure!(
                metadata.is_dir()
                    && !metadata.file_type().is_symlink()
                    && metadata.uid() == unsafe { libc::geteuid() }
                    && metadata.mode() & 0o077 == 0,
                "PAIRING_DIRECTORY_NOT_PRIVATE"
            );
            Ok(Self { path })
        }
    }
    pub fn file(&self, path: &Path, create: bool, sqlite_sidecar: bool) -> Result<File> {
        ensure!(
            std::path::absolute(path)?.parent() == Some(self.path.as_path()),
            "PAIRING_FILE_OUTSIDE_DIRECTORY"
        );
        #[cfg(windows)]
        {
            crate::local_ipc::storage_handle(
                path,
                4 | if create { 2 } else { 0 } | if sqlite_sidecar { 8 } else { 0 },
            )
        }
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
            let _ = sqlite_sidecar;
            let mut options = std::fs::OpenOptions::new();
            options.read(true).custom_flags(libc::O_NOFOLLOW);
            if create {
                options.write(true).create(true).mode(0o600);
            }
            let file = options.open(path).context("PAIRING_FILE_UNSAFE")?;
            let metadata = file.metadata()?;
            ensure!(
                metadata.is_file()
                    && metadata.uid() == unsafe { libc::geteuid() }
                    && metadata.mode() & 0o077 == 0
                    && metadata.nlink() == 1,
                "PAIRING_FILE_UNSAFE"
            );
            Ok(file)
        }
    }
    pub fn validate_existing_files(&self) -> Result<()> {
        // Include WAL, SHM, rollback journals, service locks and credential files;
        // unknown files get the same policy, rather than a bypass by filename.
        for entry in std::fs::read_dir(&self.path)? {
            let path = entry?.path();
            #[cfg(windows)]
            // Attribute/security inspection does not interfere with another
            // connection's SQLite sidecar or the service's exclusive lock.
            // The directory and primary DB retain full no-share-delete pins.
            let result = crate::local_ipc::storage_handle(&path, 4 | 8 | 16);
            #[cfg(not(windows))]
            let result = self.file(&path, false, true);
            match result {
                Ok(_) => {}
                Err(error)
                    if error
                        .downcast_ref::<std::io::Error>()
                        .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }
}
