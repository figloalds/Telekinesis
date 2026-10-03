//! Real WinFsp mount. Only the C shim touches WinFsp structures.
use crate::{core::*, runtime::Shared};
use anyhow::{Context, Result, bail, ensure};
use serde_json::Value;
use std::{
    path::{Path, PathBuf},
    sync::{Mutex, OnceLock},
};
static ENGINE: OnceLock<Shared> = OnceLock::new();
struct Mount {
    native: usize,
    path: PathBuf,
}
static MOUNT: Mutex<Option<Mount>> = Mutex::new(None);
#[repr(C)]
#[derive(Default)]
pub struct Info {
    size: u64,
    time: u64,
    index: u64,
    attributes: u32,
}
unsafe extern "C" {
    fn tk_winfsp_load(attempted: *mut u16, count: u32) -> u32;
    fn tk_mount_start(path: *const u16, out: *mut usize) -> u32;
    fn tk_mount_stop(native: usize);
    fn tk_mount_notify(native: usize, path: *const u16, action: u32) -> u32;
}
/// Called outside the engine mutex, using paths in their WinFsp-normalized form.
pub fn notify(path: &str, action: u32) -> Result<()> {
    let guard = MOUNT.lock().unwrap();
    let Some(m) = guard.as_ref() else {
        return Ok(());
    };
    let path = path.to_uppercase();
    let encoded: Vec<_> = path.encode_utf16().chain(Some(0)).collect();
    let status = unsafe { tk_mount_notify(m.native, encoded.as_ptr(), action) };
    ensure!(status == 0, "NOTIFICATION_FAILED: 0x{status:08x}");
    Ok(())
}
fn wide(path: &Path) -> Vec<u16> {
    use std::os::windows::ffi::OsStrExt;
    // canonicalize produces extended DOS paths. WinFsp's mount-point parser
    // expects a DOS folder path; retain canonical paths for reservation checks.
    let text = path.as_os_str().to_string_lossy();
    let normalized = if let Some(unc) = text.strip_prefix(r"\\?\UNC\") {
        format!(r"\\{unc}")
    } else {
        text.strip_prefix(r"\\?\").unwrap_or(&text).to_owned()
    };
    std::ffi::OsStr::new(&normalized)
        .encode_wide()
        .chain(Some(0))
        .collect()
}
pub fn start(engine: Shared, path: &Path) -> Result<()> {
    let mut attempted = vec![0u16; 32768];
    let loaded = unsafe { tk_winfsp_load(attempted.as_mut_ptr(), attempted.len() as u32) };
    let length = attempted
        .iter()
        .position(|c| *c == 0)
        .unwrap_or(attempted.len());
    ensure!(
        loaded == 0,
        "WINFSP_RUNTIME_UNAVAILABLE: {} at {}; install WinFsp or set WINFSP_DIR to its installation root",
        std::io::Error::from_raw_os_error(loaded as i32),
        String::from_utf16_lossy(&attempted[..length])
    );
    ensure!(ENGINE.set(engine.clone()).is_ok(), "ONE_MOUNT_PER_RUNTIME");
    ensure!(
        !path.exists(),
        "MOUNT_PATH_MUST_NOT_EXIST: {}",
        path.display()
    );
    let mut native = 0;
    let status = unsafe { tk_mount_start(wide(path).as_ptr(), &mut native) };
    ensure!(status == 0, "WINFSP_MOUNT_FAILED: 0x{status:08x}");
    *MOUNT.lock().unwrap() = Some(Mount {
        native,
        path: path.to_owned(),
    });
    engine.lock().unwrap().mounted = true;
    Ok(())
}
/// Cooperative O1 stop. Never hold the engine mutex while stopping callbacks.
pub fn stop(engine: &Shared) -> Result<()> {
    let mut guard = MOUNT.lock().unwrap();
    {
        let mut e = engine.lock().unwrap();
        e.quiet()?;
        e.switching = true;
    }
    if let Some(mount) = guard.take()
        && mount.native != 0
    {
        unsafe { tk_mount_stop(mount.native) };
    }
    let mut e = engine.lock().unwrap();
    e.mounted = false;
    // Remain fenced until process exit; no new opens in the shutdown gap.
    Ok(())
}
pub fn checkout(engine: &Shared, request: &str, payload: &Value) -> Result<Value> {
    let mut mount_guard = MOUNT.lock().unwrap();
    let mount = mount_guard.as_mut().context("MOUNT_NOT_RUNNING")?;
    {
        let mut e = engine.lock().unwrap();
        if let Some(result) = e.store.receipt(request, payload)? {
            return Ok(result);
        }
        if let Some(g) = payload["generation"].as_u64() {
            ensure!(g == e.store.generation, "STALE_VIEW");
        }
        e.quiet()?;
        let branch = e
            .store
            .branch(payload["name"].as_str().context("MISSING_ARGUMENT")?)?;
        let p = e.store.projection(&branch.id)?;
        ensure!(p.pending.is_empty(), "INCOMPLETE_CAUSAL_HISTORY");
        for entry in p.entries.values().filter(|e| e.alive) {
            if let Some(h) = &entry.content {
                e.store.read_object(h)?;
            }
        }
        e.switching = true;
    }
    // New opens fenced before stopping. No engine mutex during dispatcher stop.
    unsafe { tk_mount_stop(mount.native) };
    mount.native = 0;
    let result = {
        let mut e = engine.lock().unwrap();
        e.mounted = false;
        e.quiet().and_then(|()| e.control(request, payload))
    };
    let mut native = 0;
    let status = unsafe { tk_mount_start(wide(&mount.path).as_ptr(), &mut native) };
    let mut e = engine.lock().unwrap();
    e.switching = false;
    if status != 0 {
        e.health = Some(format!("REMOUNT_FAILED: 0x{status:08x}; restart runtime"));
        bail!("REMOUNT_FAILED: 0x{status:08x}");
    }
    mount.native = native;
    e.mounted = true;
    result
}
fn fill(info: &mut Info, entry: &Entry, size: usize) {
    info.size = size as u64;
    info.attributes = if entry.kind == "directory" {
        0x10
    } else {
        0x20
    };
    info.time = entry.modified_ms * 10_000 + 116_444_736_000_000_000;
    let h = hash(entry.id.as_bytes());
    info.index = u64::from_str_radix(&h[..16], 16).unwrap();
}
unsafe fn string(ptr: *const u16) -> String {
    if ptr.is_null() {
        return String::new();
    }
    let mut n = 0;
    while n < 32768 && unsafe { *ptr.add(n) } != 0 {
        n += 1;
    }
    String::from_utf16_lossy(unsafe { std::slice::from_raw_parts(ptr, n) })
}
/// C shim entry point.
///
/// # Safety
/// The caller must provide a valid token pointer and writable output pointers.
/// Names must be NUL-terminated UTF-16. I/O buffers must cover `length` bytes;
/// directory enumeration buffers must cover `length` UTF-16 code units.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn tk_call(
    op: u32,
    name: *const u16,
    token: *mut u64,
    offset: u64,
    buffer: *mut u8,
    length: u32,
    flags: u32,
    info: *mut Info,
    transferred: *mut u32,
) -> u32 {
    let run = std::panic::catch_unwind(|| -> Result<u32> {
        let mut e = ENGINE
            .get()
            .context("ENGINE_NOT_READY")?
            .lock()
            .map_err(|_| anyhow::anyhow!("ENGINE_POISONED"))?;
        let h = unsafe { *token };
        let path = unsafe { string(name) };
        let mut out = Info::default();
        let mut count = 0;
        match op {
            1 => {
                let special = path
                    .trim_matches('\\')
                    .eq_ignore_ascii_case(".tkfs-runtime.json");
                let entry = if special {
                    Entry {
                        id: "control".into(),
                        kind: "file".into(),
                        ..Default::default()
                    }
                } else {
                    e.store.lookup(&path)?
                };
                let size = if special {
                    e.discovery.len()
                } else {
                    entry
                        .content
                        .as_ref()
                        .map(|h| e.store.read_object(h).map(|b| b.len()))
                        .transpose()?
                        .unwrap_or(0)
                };
                fill(&mut out, &entry, size);
            }
            2 | 3 => {
                let create = if op == 2 {
                    Some(if flags & 1 != 0 { "directory" } else { "file" })
                } else {
                    None
                };
                let (handle, entry) = e.open(&path, create, flags & 2 != 0)?;
                unsafe { *token = handle };
                fill(&mut out, &entry, e.session(handle)?.bytes.len());
            }
            4 => {
                let s = e.session(h)?;
                let start = usize::try_from(offset)?;
                if start >= s.bytes.len() {
                    return Ok(0xc0000011);
                }
                count = (length as usize).min(s.bytes.len() - start) as u32;
                unsafe {
                    std::ptr::copy_nonoverlapping(s.bytes[start..].as_ptr(), buffer, count as usize)
                };
            }
            5 => {
                let bytes = unsafe { std::slice::from_raw_parts(buffer, length as usize) };
                count = e.write(h, offset, bytes, flags & 1 != 0, flags & 2 != 0)? as u32;
            }
            6 => e.truncate(h, offset, flags & 1 != 0)?,
            7 => e.flush(h)?,
            8 => {}
            9 => e.cleanup(h, flags & 1 != 0)?,
            10 => e.close(h)?,
            11 => {
                let s = e.session(h)?;
                let p = e.store.projection(&e.store.active)?;
                ensure!(
                    s.entry.id != ROOT && s.entry.id != "control",
                    "PERMISSION_DENIED"
                );
                ensure!(
                    !p.entries
                        .values()
                        .any(|v| v.alive && v.parent == s.entry.id),
                    "DIRECTORY_NOT_EMPTY"
                );
            }
            12 => {
                e.flush(h)?;
                let entity = e.session(h)?.entry.id.clone();
                e.store.rename(&entity, &path, flags & 1 != 0)?;
                let entry = e.store.lookup(&path)?;
                e.sessions.get_mut(&entity).unwrap().entry = entry;
            }
            13 => {
                let entity = e.session(h)?.entry.id.clone();
                let p = e.store.projection(&e.store.active)?;
                let mut children: Vec<_> = p
                    .entries
                    .values()
                    .filter(|entry| entry.alive && entry.parent == entity)
                    .cloned()
                    .collect();
                if entity == ROOT {
                    children.push(Entry {
                        id: "control".into(),
                        name: ".tkfs-runtime.json".into(),
                        kind: "file".into(),
                        ..Default::default()
                    });
                }
                children.sort_by_key(|e| e.name.to_lowercase());
                let Some(entry) = children.get(offset as usize) else {
                    return Ok(0x80000006);
                };
                let encoded: Vec<_> = entry.name.encode_utf16().chain(Some(0)).collect();
                ensure!(encoded.len() <= length as usize, "BUFFER_TOO_SMALL");
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        encoded.as_ptr(),
                        buffer.cast::<u16>(),
                        encoded.len(),
                    )
                };
                let size = if entry.id == "control" {
                    e.discovery.len()
                } else {
                    entry
                        .content
                        .as_ref()
                        .map(|h| e.store.read_object(h).map(|b| b.len()))
                        .transpose()?
                        .unwrap_or(0)
                };
                fill(&mut out, entry, size);
            }
            _ => bail!("UNSUPPORTED_FS_OPERATION"),
        }
        if [5, 6, 7, 8].contains(&op) && h != 0 {
            let s = e.session(h)?;
            fill(&mut out, &s.entry, s.bytes.len());
        }
        if !info.is_null() {
            unsafe { *info = out };
        }
        if !transferred.is_null() {
            unsafe { *transferred = count };
        }
        Ok(0)
    });
    match run {
        Ok(Ok(status)) => status,
        Ok(Err(error)) => {
            let message = format!("{error:#}");
            eprintln!("filesystem op={op}: {message}");
            if [9, 10].contains(&op)
                && let Some(engine) = ENGINE.get()
                && let Ok(mut e) = engine.lock()
            {
                e.health = Some(message.clone());
            }
            if message.contains("FILE_NOT_FOUND") {
                0xc0000034
            } else if message.contains("NAME_COLLISION") {
                0xc0000035
            } else if message.contains("NOT_EMPTY") {
                0xc0000101
            } else if message.contains("NOT_DIRECTORY") {
                0xc0000103
            } else if message.contains("BUSY") {
                0xc0000043
            } else if message.contains("INVALID_NAME") {
                0xc0000033
            } else if message.contains("PERMISSION") || message.contains("ACCESS_DENIED") {
                0xc0000022
            } else {
                0xc0000185
            }
        }
        Err(_) => 0xc0000185,
    }
}
