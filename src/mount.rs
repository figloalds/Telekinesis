//! WinFsp callbacks use host scratch files and the pathname-shared Windows cache.
//!
//! W6-P0-MOUNT-LIFECYCLE: this process never holds a handle to, or issues I/O
//! against, its own mounted namespace. Windows closes a dying process's handles
//! one at a time and synchronously; a cleanup that needs this server's own
//! dispatcher never completes, and the process can neither exit nor be killed.
//! Cached bytes reach the server through the driver (`FlushAndPurgeOnCleanup`),
//! and the native shim refuses opens that originate from this PID.
use crate::{
    content::working::lock,
    core::{Entry, EntryKind},
    runtime::Engine,
};
use anyhow::{Context, Result, ensure};
use std::{
    path::{Path, PathBuf},
    sync::atomic::Ordering,
    sync::{Arc, Mutex, RwLock},
};

static ENGINE: RwLock<Option<Arc<Engine>>> = RwLock::new(None);
static MOUNT: Mutex<Option<(usize, PathBuf)>> = Mutex::new(None);
pub fn mounted_path() -> Option<PathBuf> {
    lock(&MOUNT).as_ref().map(|(_, path)| path.clone())
}

static CALLS: [std::sync::atomic::AtomicU64; 16] =
    [const { std::sync::atomic::AtomicU64::new(0) }; 16];
static NANOS: [std::sync::atomic::AtomicU64; 16] =
    [const { std::sync::atomic::AtomicU64::new(0) }; 16];
static METADATA_OPENS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub fn stats() -> Vec<serde_json::Value> {
    let mut stats: Vec<_> = (1..16).map(|i|serde_json::json!({"operation":i,"calls":CALLS[i].load(Ordering::Relaxed),"ms":NANOS[i].load(Ordering::Relaxed) as f64/1e6})).collect();
    stats.push(serde_json::json!({"operation":"open_stages", "stages":crate::runtime::engine::open_stats()}));
    stats.push(serde_json::json!({"operation":"namespace_cache", "cache":crate::runtime::engine::namespace_stats()}));
    stats.push(
        serde_json::json!({"operation":"metadata_cost", "cost":crate::core::metadata_stats()}),
    );
    stats.push(serde_json::json!({"operation":"metadata_opens", "calls":METADATA_OPENS.load(Ordering::Relaxed)}));
    stats.push(serde_json::json!({"operation":"self_open_refusals", "calls":unsafe { tk_self_open_refusals() }}));
    stats
}
#[repr(C)]
#[derive(Default)]
pub struct Info {
    size: u64,
    creation_time: u64,
    access_time: u64,
    write_time: u64,
    change_time: u64,
    index: u64,
    attributes: u32,
}
unsafe extern "C" {
    fn tk_winfsp_load(attempted: *mut u16, count: u32) -> u32;
    fn tk_winfsp_error_detail(buffer: *mut u16, count: u32);
    fn tk_mount_start(
        path: *const u16,
        out: *mut usize,
        threads: u32,
        timeout: u32,
        fault: u32,
    ) -> u32;
    fn tk_mount_stop(native: usize);
    fn tk_mount_notify(native: usize, path: *const u16, action: u32) -> u32;
    fn tk_self_open_refusals() -> u32;
}

fn fill(entry: &Entry, size: u64) -> Info {
    let size = entry.working_len.unwrap_or(size);
    let index = uuid::Uuid::parse_str(&entry.id)
        .map(|u| u.as_u128() as u64)
        .unwrap_or(0);
    Info {
        size,
        creation_time: entry.times.created,
        access_time: entry.times.accessed,
        write_time: entry.times.modified,
        change_time: entry.times.changed,
        index,
        attributes: entry.attributes
            | if entry.kind == EntryKind::Directory {
                0x10
            } else {
                0
            },
    }
}

fn path_within(path: &Path, ancestor: &Path) -> bool {
    use std::os::windows::ffi::OsStrExt;
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn CompareStringOrdinal(
            a: *const u16,
            alen: i32,
            b: *const u16,
            blen: i32,
            ignore_case: i32,
        ) -> i32;
    }
    let mut components = path.components();
    ancestor.components().all(|parent| {
        let Some(child) = components.next() else {
            return false;
        };
        let parent: Vec<_> = parent.as_os_str().encode_wide().collect();
        let child: Vec<_> = child.as_os_str().encode_wide().collect();
        unsafe {
            CompareStringOrdinal(
                parent.as_ptr(),
                parent.len() as i32,
                child.as_ptr(),
                child.len() as i32,
                1,
            ) == 2
        }
    })
}

/// Resolve existing reparse points before attachment, including the parent of
/// a directory mount that WinFsp has yet to create. Never resolve it afterwards:
/// that would itself access this process's served namespace.
fn prospective_mount_path(path: &Path) -> Result<PathBuf> {
    let mut existing = std::path::absolute(path)?;
    let mut tail = Vec::new();
    loop {
        match existing.canonicalize() {
            Ok(mut resolved) => {
                for name in tail.into_iter().rev() {
                    resolved.push(name);
                }
                return Ok(resolved);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                tail.push(
                    existing
                        .file_name()
                        .context("INVALID_MOUNT_PATH")?
                        .to_owned(),
                );
                ensure!(existing.pop(), "INVALID_MOUNT_PATH");
            }
            Err(error) => return Err(error).context("RESOLVE_MOUNT_PATH"),
        }
    }
}

fn validate_backing_paths(root: &Path, mount: &Path) -> Result<()> {
    let root = root.canonicalize()?;
    let text = mount.as_os_str().to_string_lossy();
    let mount = if text.len() == 2 && text.ends_with(':') {
        // A drive-letter mount always names its root, independent of that
        // drive's per-process current directory; the drive may not exist yet.
        PathBuf::from(format!("\\\\?\\{}\\", text))
    } else {
        prospective_mount_path(mount)?
    };
    ensure!(!path_within(&root, &mount), "MOUNT_COVERS_BACKING_STORE");
    for backing in [
        root.join("scratch"),
        root.join("objects"),
        root.join("objects/packs"),
    ] {
        let backing = backing.canonicalize()?;
        ensure!(
            !path_within(&backing, &mount) && !path_within(&mount, &backing),
            "MOUNT_OVERLAPS_BACKING_STORAGE: {}",
            backing.display()
        );
    }
    Ok(())
}

/// WinFsp creates a directory mount point itself and fails with
/// STATUS_OBJECT_NAME_COLLISION when anything already occupies that name,
/// including an empty directory or a reparse point left by a killed process.
pub fn ensure_mount_point_free(path: &Path) -> Result<()> {
    let text = path.as_os_str().to_string_lossy();
    if text.len() == 2 && text.ends_with(':') {
        return Ok(());
    }
    ensure!(
        std::fs::symlink_metadata(path).is_err(),
        "MOUNT_POINT_EXISTS: {} already exists; choose a path that does not exist yet          (Telekinesis creates the mount directory), or remove it if it is an empty leftover",
        path.display()
    );
    Ok(())
}

/// WinFsp creates only the last component of a directory mount: its parent
/// (`default_mount_directory`, say) is created here, before any volume
/// exists, so this is never I/O on the served namespace.
fn create_mount_parent(path: &Path) -> Result<()> {
    let text = path.as_os_str().to_string_lossy();
    if text.len() == 2 && text.ends_with(':') {
        return Ok(());
    }
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("MOUNT_PARENT_UNAVAILABLE: {}", parent.display()))?;
    }
    Ok(())
}

pub fn attach(engine: Arc<Engine>, path: &Path) -> Result<()> {
    let mut mount = lock(&MOUNT);
    ensure!(mount.is_none(), "MOUNT_ALREADY_ATTACHED");
    // An otherwise valid empty mount such as <state>/scratch would redirect
    // future backend opens into our own dispatcher. Reject before any volume
    // exists, rather than relying on the callback self-open refusal in a race
    // with forced process termination.
    let backing_root = { lock(&engine.store).root.clone() };
    validate_backing_paths(&backing_root, path)?;
    ensure_mount_point_free(path)?;
    create_mount_parent(path)?;
    // Explicit opt-in fault seam for VM lifecycle acceptance; never set during
    // normal operation. Every stage follows the real native unwind path.
    let fault = match std::env::var("TKFS_TEST_MOUNT_FAIL").as_deref() {
        Err(std::env::VarError::NotPresent) => 0,
        Ok("after-create") => 1,
        Ok("after-mount") => 2,
        Ok("after-dispatcher") => 3,
        _ => anyhow::bail!("INVALID_MOUNT_FAULT"),
    };
    let self_open_test = match std::env::var("TKFS_TEST_MOUNT_SELF_OPEN").as_deref() {
        Err(std::env::VarError::NotPresent) => false,
        Ok("1") => true,
        _ => anyhow::bail!("INVALID_MOUNT_SELF_OPEN_TEST"),
    };
    let mut attempted = vec![0; 1024];
    let status = unsafe { tk_winfsp_load(attempted.as_mut_ptr(), 1024) };
    if status != 0 {
        let mut detail = vec![0; 2048];
        unsafe { tk_winfsp_error_detail(detail.as_mut_ptr(), detail.len() as u32) };
        let end = detail.iter().position(|c| *c == 0).unwrap_or(detail.len());
        anyhow::bail!(
            "WINFSP_LOAD_FAILED: {status:08x}: {} (selected {})",
            String::from_utf16_lossy(&detail[..end]),
            String::from_utf16_lossy(
                &attempted[..attempted
                    .iter()
                    .position(|c| *c == 0)
                    .unwrap_or(attempted.len())]
            )
        );
    }
    *ENGINE.write().unwrap() = Some(engine.clone());
    let wide: Vec<_> = path
        .as_os_str()
        .to_string_lossy()
        .encode_utf16()
        .chain(Some(0))
        .collect();
    let mut native = 0;
    let status = unsafe { tk_mount_start(wide.as_ptr(), &mut native, 0, 60_000, fault) };
    if status != 0 {
        *ENGINE.write().unwrap() = None;
        if status == 0xc0000035 {
            anyhow::bail!(
                "MOUNT_FAILED: {status:08x}: mount point {} is already in use",
                path.display()
            );
        }
        anyhow::bail!("MOUNT_FAILED: {status:08x}");
    }
    if self_open_test && let Err(error) = verify_self_open_guard(&engine, path) {
        unsafe { tk_mount_stop(native) };
        *ENGINE.write().unwrap() = None;
        return Err(error);
    }
    *mount = Some((native, path.to_owned()));
    engine.set_invalidator(Arc::new(notify));
    Ok(())
}
/// Negative acceptance only, explicitly enabled inside the isolated VM. No
/// successful namespace handle may escape either attempt. The store lock makes
/// a too-late native guard fail the bounded startup check rather than pass it.
fn verify_self_open_guard(engine: &Engine, path: &Path) -> Result<()> {
    use std::os::windows::fs::OpenOptionsExt;
    let root = if path.as_os_str().len() == 2 && path.to_string_lossy().ends_with(':') {
        PathBuf::from(format!("{}\\", path.display()))
    } else {
        path.to_owned()
    };
    for _ in 0..2 {
        let opened = {
            let _store = lock(&engine.store);
            std::fs::OpenOptions::new()
                .read(true)
                .custom_flags(0x02000000) // FILE_FLAG_BACKUP_SEMANTICS (root directory)
                .open(&root)
        }; // release the store lock before any unexpected successful handle drops
        let error = match opened {
            Ok(file) => {
                drop(file);
                anyhow::bail!("SELF_OPEN_GUARD_BYPASSED");
            }
            Err(error) => error,
        };
        ensure!(
            error.kind() == std::io::ErrorKind::PermissionDenied,
            "SELF_OPEN_TEST_FAILED: {error}"
        );
    }
    ensure!(
        unsafe { tk_self_open_refusals() } >= 2,
        "SELF_OPEN_GUARD_NOT_REACHED"
    );
    Ok(())
}
/// Stops serving first, then seals what is left. A failed final seal is a
/// reported durability failure; it never leaves the dispatcher running.
pub fn detach() -> Result<()> {
    let native = lock(&MOUNT).take();
    if let Some((native, _)) = native {
        unsafe { tk_mount_stop(native) };
    }
    let engine = ENGINE.write().unwrap().take();
    if let Some(engine) = engine {
        engine.flush(None)?;
    }
    Ok(())
}
pub fn notify(path: &str, action: u32) -> Result<()> {
    let mount = lock(&MOUNT);
    if let Some((native, _)) = mount.as_ref() {
        let path = path.replace('/', "\\").to_uppercase();
        let wide: Vec<_> = path.encode_utf16().chain(Some(0)).collect();
        let status = unsafe { tk_mount_notify(*native, wide.as_ptr(), action) };
        ensure!(status == 0, "NOTIFY_FAILED: {status:08x}");
    }
    Ok(())
}
unsafe fn text(path: *const u16) -> Result<String> {
    if path.is_null() {
        return Ok(String::new());
    }
    let mut len = 0;
    while len < 32768 && unsafe { *path.add(len) } != 0 {
        len += 1;
    }
    ensure!(len < 32768, "INVALID_NAME");
    Ok(String::from_utf16(unsafe {
        std::slice::from_raw_parts(path, len)
    })?)
}
fn status(error: &anyhow::Error) -> u32 {
    let text = format!("{error:#}");
    for (code, value) in [
        ("NOT_FOUND", 0xc0000034),
        ("ALREADY_EXISTS", 0xc0000035),
        ("NOT_A_DIRECTORY", 0xc0000103),
        ("IS_A_DIRECTORY", 0xc00000ba),
        ("DIRECTORY_NOT_EMPTY", 0xc0000101),
        ("ACCESS_DENIED", 0xc0000022),
        ("INVALID_HANDLE", 0xc0000008),
        ("INVALID_NAME", 0xc0000033),
        ("OBJECT_MISSING", 0xc00000a3),
        ("STOPPING", 0xc00000a3),
    ] {
        if text.contains(code) {
            return value;
        }
    }
    if let Some(io) = error.downcast_ref::<std::io::Error>() {
        match io.kind() {
            std::io::ErrorKind::NotFound => return 0xc0000034,
            std::io::ErrorKind::PermissionDenied => return 0xc0000022,
            _ => {}
        }
    }
    eprintln!("mount callback: {error:#}");
    0xc0000001
}
/// # Safety
/// Called by the WinFsp shim with buffers valid for the supplied operation.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn tk_call(
    op: u32,
    name: *const u16,
    token: *mut u64,
    offset: u64,
    buffer: *mut std::ffi::c_void,
    length: u32,
    flags: u32,
    info: *mut Info,
    count: *mut u32,
) -> u32 {
    let started = std::time::Instant::now();
    if op < 16 {
        CALLS[op as usize].fetch_add(1, Ordering::Relaxed);
    }
    let result = std::panic::catch_unwind(|| -> Result<u32> {
        let engine = ENGINE.read().unwrap().clone().context("STOPPING")?;
        let h = if token.is_null() {
            0
        } else {
            unsafe { *token }
        };
        if !count.is_null() {
            unsafe { *count = 0 };
        }
        match op {
            1 => {
                let entry = engine.lookup(&unsafe { text(name) }?)?;
                if !info.is_null() {
                    unsafe {
                        *info = fill(
                            &entry,
                            entry.content.as_ref().map(|c| c.total_len).unwrap_or(0),
                        )
                    };
                }
            }
            2 | 3 => {
                if flags & 8 != 0 {
                    METADATA_OPENS.fetch_add(1, Ordering::Relaxed);
                }
                let attrs = if op == 2 && !buffer.is_null() {
                    Some(unsafe { *(buffer as *const u32) })
                } else {
                    None
                };
                let h = engine.open_native_file(
                    &unsafe { text(name) }?,
                    op == 2,
                    flags & 1 != 0,
                    flags & 2 != 0,
                    attrs,
                    flags & 8 != 0,
                )?;
                unsafe { *token = h };
                if flags & 4 != 0 {
                    engine.handle(h)?.delete.store(true, Ordering::Release);
                }
                let (e, len) = engine.info(h)?;
                unsafe { *info = fill(&e, len) };
            }
            4 => {
                let out = if length == 0 {
                    &mut []
                } else {
                    unsafe { std::slice::from_raw_parts_mut(buffer as *mut u8, length as usize) }
                };
                let n = engine.read_visible(h, offset, out)?;
                if !count.is_null() {
                    unsafe { *count = n as u32 };
                }
                if n == 0 && length != 0 {
                    return Ok(0xc0000011);
                }
            }
            5 => {
                let bytes = if length == 0 {
                    &[]
                } else {
                    unsafe { std::slice::from_raw_parts(buffer as *const u8, length as usize) }
                };
                let n = engine.write_native(h, offset, bytes, flags & 1 != 0, flags & 2 != 0)?;
                if !count.is_null() {
                    unsafe { *count = n as u32 };
                }
                let (e, len) = engine.info_visible(h)?;
                unsafe { *info = fill(&e, len) };
            }
            6 => {
                engine.truncate(h, offset, flags & 1 != 0)?;
                let (e, len) = engine.info(h)?;
                unsafe { *info = fill(&e, len) };
            }
            7 => {
                if h == 0 {
                    engine.flush_native_volume()?;
                } else {
                    engine.flush_native_file(h)?;
                }
                if h != 0 {
                    let (e, len) = engine.info_visible(h)?;
                    unsafe { *info = fill(&e, len) };
                }
            }
            8 => {
                let (e, len) = engine.info_visible(h)?;
                unsafe { *info = fill(&e, len) };
            }
            9 => engine.cleanup_native(h, flags & 1 != 0, flags & 2 != 0)?,
            10 => engine.close(h)?,
            11 => {
                let handle = engine.handle(h)?;
                let entry = lock(&handle.entry);
                ensure!(entry.id != crate::core::ROOT, "ACCESS_DENIED");
                if entry.kind == EntryKind::Directory {
                    ensure!(
                        lock(&engine.store)
                            .projection()
                            .children(&entry.id)
                            .is_empty(),
                        "DIRECTORY_NOT_EMPTY"
                    );
                }
            }
            12 => engine.rename(h, &unsafe { text(name) }?, flags & 1 != 0)?,
            13 => {
                let entries = engine.children(h, flags & 1 != 0)?;
                let Some(entry) = entries.get(offset as usize) else {
                    return Ok(0x80000006);
                };
                let wide: Vec<_> = entry.name.encode_utf16().chain(Some(0)).collect();
                ensure!(wide.len() <= length as usize, "INVALID_NAME");
                unsafe {
                    std::ptr::copy_nonoverlapping(wide.as_ptr(), buffer as *mut u16, wide.len());
                    *info = fill(
                        entry,
                        entry.content.as_ref().map(|c| c.total_len).unwrap_or(0),
                    );
                }
            }
            14 => {
                ensure!(length >= 36, "INVALID_BASIC_INFO");
                let times = unsafe { std::slice::from_raw_parts(buffer as *const u64, 4) };
                let attrs = unsafe { *((buffer as *const u8).add(32) as *const u32) };
                engine.basic(h, attrs, times.try_into()?)?;
                let (e, len) = engine.info(h)?;
                unsafe { *info = fill(&e, len) };
            }
            15 => engine
                .handle(h)?
                .delete
                .store(flags & 1 != 0, Ordering::Release),
            _ => anyhow::bail!("UNSUPPORTED_OPERATION"),
        }
        Ok(0)
    });
    if op < 16 {
        NANOS[op as usize].fetch_add(started.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }
    match result {
        Ok(Ok(status)) => status,
        Ok(Err(error)) => {
            if matches!(op, 5..=7 | 9) {
                eprintln!("mount I/O operation {op}: {error:#}");
            }
            status(&error)
        }
        Err(_) => 0xc0000001,
    }
}

#[cfg(test)]
mod mount_path_tests {

    #[test]
    fn mount_cannot_redirect_future_backing_io_into_its_dispatcher() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("state");
        std::fs::create_dir_all(root.join("scratch")).unwrap();
        std::fs::create_dir_all(root.join("objects/packs")).unwrap();
        for mount in [
            root.clone(),
            root.join("scratch"),
            root.join("scratch/new-mount"),
            root.join("objects/new-mount"),
            temp.path().to_owned(),
        ] {
            assert!(
                super::validate_backing_paths(&root, &mount).is_err(),
                "{}",
                mount.display()
            );
        }
        assert!(super::validate_backing_paths(&root, &root.join("mount")).is_ok());
        assert!(super::validate_backing_paths(&root, &temp.path().join("separate/mount")).is_ok());
        let folded = root.join("scratch").to_string_lossy().to_uppercase();
        assert!(super::validate_backing_paths(&root, std::path::Path::new(&folded)).is_err());
        let canonical = root.canonicalize().unwrap();
        let drive = canonical
            .components()
            .next()
            .unwrap()
            .as_os_str()
            .to_string_lossy();
        assert!(
            super::validate_backing_paths(
                &root,
                std::path::Path::new(drive.trim_start_matches("\\\\?\\"))
            )
            .is_err()
        );
    }
}
