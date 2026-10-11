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

/// Keep SQLite's auxiliary files in the validated private namespace on Windows.
/// Last-close deletion otherwise races another connection's security inspection
/// (including zero-link handles and CreateFile's delete-pending AccessDenied).
/// Security checks stay unchanged; no error is retried or ignored here.
#[cfg(windows)]
pub(crate) fn retain_sqlite_sidecars(db: &rusqlite::Connection) -> Result<()> {
    use rusqlite::ffi::{SQLITE_FCNTL_PERSIST_WAL, SQLITE_OK, sqlite3_file_control};
    let mut enabled: std::ffi::c_int = 1;
    // The borrowed connection is live and used synchronously; SQLite's file
    // control consumes neither the handle nor this stack-allocated integer.
    let result = unsafe {
        sqlite3_file_control(
            db.handle(),
            c"main".as_ptr(),
            SQLITE_FCNTL_PERSIST_WAL,
            (&mut enabled as *mut std::ffi::c_int).cast(),
        )
    };
    ensure!(
        result == SQLITE_OK,
        "PAIRING_SQLITE_PERSISTENCE_UNAVAILABLE"
    );
    enabled = -1;
    let result = unsafe {
        sqlite3_file_control(
            db.handle(),
            c"main".as_ptr(),
            SQLITE_FCNTL_PERSIST_WAL,
            (&mut enabled as *mut std::ffi::c_int).cast(),
        )
    };
    ensure!(
        result == SQLITE_OK && enabled == 1,
        "PAIRING_SQLITE_PERSISTENCE_UNAVAILABLE"
    );
    // A completed last-close checkpoint truncates WAL to zero instead of
    // retaining old pages. SQLite still controls checkpoint safety and locks.
    db.pragma_update(None, "journal_size_limit", 0)?;
    Ok(())
}

#[cfg(windows)]
fn inspect_existing(path: &Path) -> Result<File> {
    use std::os::windows::{ffi::OsStrExt, io::FromRawHandle};
    unsafe extern "C" {
        fn tk_storage_open_diagnostic(
            path: *const u16,
            flags: u32,
            out: *mut *mut std::ffi::c_void,
            stage: *mut u32,
        ) -> u32;
    }
    let path_wide: Vec<_> = path.as_os_str().encode_wide().chain(Some(0)).collect();
    let mut handle = std::ptr::null_mut();
    let mut stage = 0;
    let code = unsafe {
        tk_storage_open_diagnostic(path_wide.as_ptr(), 4 | 8 | 16, &mut handle, &mut stage)
    };
    if code != 0 {
        use anyhow::Context;
        return Err(std::io::Error::from_raw_os_error(code as i32)).with_context(|| {
            format!(
                "PAIRING_STORAGE_UNSAFE: {} (flags 28, stage {stage})",
                path.display()
            )
        });
    }
    Ok(unsafe { File::from_raw_handle(handle) })
}

#[cfg(all(test, windows))]
mod tests {
    use super::*;
    fn identity(file: &File) -> (u32, u64, u32) {
        use std::os::windows::io::AsRawHandle;
        #[repr(C)]
        #[derive(Default)]
        struct Info {
            attributes: u32,
            created: [u32; 2],
            accessed: [u32; 2],
            written: [u32; 2],
            volume: u32,
            size_high: u32,
            size_low: u32,
            links: u32,
            index_high: u32,
            index_low: u32,
        }
        #[link(name = "kernel32")]
        unsafe extern "system" {
            fn GetFileInformationByHandle(handle: *mut std::ffi::c_void, info: *mut Info) -> i32;
        }
        let mut info = Info::default();
        assert_ne!(
            unsafe { GetFileInformationByHandle(file.as_raw_handle(), &mut info) },
            0
        );
        (
            info.volume,
            (u64::from(info.index_high) << 32) | u64::from(info.index_low),
            info.links,
        )
    }
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
    fn sqlite_sidecars_survive_last_close_with_inspection_handles() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("registry");
        let directory = Directory::open(&root).unwrap();
        let db_path = root.join("pairs.sqlite");
        let _db_pin = directory.file(&db_path, true, false).unwrap();
        let db = rusqlite::Connection::open(&db_path).unwrap();
        retain_sqlite_sidecars(&db).unwrap();
        db.execute_batch(
            "PRAGMA journal_mode=WAL; CREATE TABLE fixture(v); INSERT INTO fixture VALUES(1)",
        )
        .unwrap();
        let shm = root.join("pairs.sqlite-shm");
        let inspection = inspect_existing(&shm).unwrap();
        let before = identity(&inspection);
        assert_eq!(before.2, 1);
        drop(db);
        let after = inspect_existing(&shm).unwrap();
        assert_eq!(identity(&after), before);
        assert_eq!(identity(&inspection), before);
        assert_eq!(
            std::fs::metadata(root.join("pairs.sqlite-wal"))
                .unwrap()
                .len(),
            0
        );
        directory.validate_existing_files().unwrap();
        drop(inspection);
        directory.validate_existing_files().unwrap();
        let reopened = rusqlite::Connection::open(&db_path).unwrap();
        retain_sqlite_sidecars(&reopened).unwrap();
        assert_eq!(
            reopened
                .query_row("SELECT v FROM fixture", [], |r| r.get::<_, u64>(0))
                .unwrap(),
            1
        );
    }
    #[test]
    fn concurrent_sqlite_registry_validation_never_rejects_safe_sidecar_lifecycle() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("registry").join("pairs.sqlite");
        let installation = crate::core::id();
        drop(crate::pairing::Registry::open(&path, &installation).unwrap());
        let start = std::sync::Barrier::new(4);
        let errors = std::sync::Mutex::new(Vec::new());
        std::thread::scope(|scope| {
            for _ in 0..4 {
                scope.spawn(|| {
                    start.wait();
                    for _ in 0..150 {
                        if let Err(error) = crate::pairing::Registry::open(&path, &installation) {
                            errors.lock().unwrap().push(format!("{error:#}"));
                        }
                    }
                });
            }
        });
        let errors = errors.into_inner().unwrap();
        assert!(
            errors.is_empty(),
            "{} lifecycle failures; first: {:?}",
            errors.len(),
            &errors[..errors.len().min(4)]
        );
    }
    #[test]
    fn readonly_last_close_keeps_validated_sidecars_and_committed_data() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("registry");
        let directory = Directory::open(&root).unwrap();
        let path = root.join("pairs.sqlite");
        let _pin = directory.file(&path, true, false).unwrap();
        let writer = rusqlite::Connection::open(&path).unwrap();
        retain_sqlite_sidecars(&writer).unwrap();
        writer
            .execute_batch(
                "PRAGMA journal_mode=WAL; CREATE TABLE fixture(v); INSERT INTO fixture VALUES(17)",
            )
            .unwrap();
        let reader = rusqlite::Connection::open_with_flags(
            &path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .unwrap();
        retain_sqlite_sidecars(&reader).unwrap();
        assert_eq!(
            reader
                .query_row("SELECT v FROM fixture", [], |r| r.get::<_, u64>(0))
                .unwrap(),
            17
        );
        let wal = root.join("pairs.sqlite-wal");
        let shm = root.join("pairs.sqlite-shm");
        let wal_before = identity(&inspect_existing(&wal).unwrap());
        let shm_before = identity(&inspect_existing(&shm).unwrap());
        drop(writer);
        drop(reader);
        assert_eq!(identity(&inspect_existing(&wal).unwrap()), wal_before);
        assert_eq!(identity(&inspect_existing(&shm).unwrap()), shm_before);
        directory.validate_existing_files().unwrap();
        let reopened = rusqlite::Connection::open(&path).unwrap();
        retain_sqlite_sidecars(&reopened).unwrap();
        assert_eq!(
            reopened
                .query_row("SELECT v FROM fixture", [], |r| r.get::<_, u64>(0))
                .unwrap(),
            17
        );
    }
    #[test]
    fn persistent_sidecars_still_reject_hardlinks_and_foreign_write_without_repair() {
        let temp = tempfile::tempdir().unwrap();
        for name in ["pairs.sqlite-wal", "pairs.sqlite-shm"] {
            let root = temp.path().join(name);
            let path = root.join("pairs.sqlite");
            drop(crate::pairing::Registry::open(&path, &crate::core::id()).unwrap());
            let sidecar = root.join(name);
            let alias = root.join("alias");
            std::fs::hard_link(&sidecar, &alias).unwrap();
            let directory = Directory::open(&root).unwrap();
            assert!(directory.validate_existing_files().is_err());
            drop(directory);
            std::fs::remove_file(alias).unwrap();
            let directory = Directory::open(&root).unwrap();
            directory.validate_existing_files().unwrap();
            grant_foreign_write(&sidecar);
            let before = acl(&sidecar);
            assert!(directory.validate_existing_files().is_err());
            assert_eq!(acl(&sidecar), before);
        }
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
    #[cfg(windows)]
    pub(crate) fn reserve_publication(&mut self) -> Result<&File> {
        let previous = self
            ._handles
            .pop()
            .ok_or_else(|| anyhow::anyhow!("PAIRING_PARENT_PIN_MISSING"))?;
        let parent = crate::local_ipc::storage_publication_parent(&self.path, previous)?;
        self._handles.push(parent);
        Ok(self._handles.last().expect("reserved directory pin"))
    }
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
            let result = inspect_existing(&path);
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
