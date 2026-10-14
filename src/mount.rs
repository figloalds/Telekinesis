//! Real WinFsp mount. Only the C shim touches WinFsp structures.
//!
//! Re-attachable (REVAMP_PLAN §4.3, WP1.5): [`bind`] sets the engine the
//! callbacks serve (idempotent for the same `Arc`, independent of any path
//! check); every [`attach`] starts a fresh `FSP_FILE_SYSTEM`; [`detach`] runs
//! fence → drain → [force: retain dirty] → stop → release → unfence (see
//! `worker::mount_ctl`). Mount failures never write `Engine.health`: callback
//! failures go to the mount-scoped error counter and a rate-limited log, and a
//! failed checkout remount reports `Lost` to the controller.
//!
//! `MOUNT` is the transition lock: [`bind`], [`attach`], [`detach`] and
//! [`checkout`] hold it across their mount I/O (so they wait for each other);
//! a callback, [`attached`], [`serves`] and [`inflight`] never take it, and
//! [`notify`] only tries it, so readers never wait for a transition.
use crate::{
    core::*,
    model::ipc::timing::DETACH_DRAIN,
    runtime::{Shared, lock_engine},
    util::lock::lock,
    worker::mount_ctl::adapter,
};
use anyhow::{Context, Result, bail, ensure};
use serde_json::Value;
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex, PoisonError, RwLock},
};
/// The engine callbacks serve. Replaced only while nothing is attached.
static ENGINE: RwLock<Option<Shared>> = RwLock::new(None);
struct Mount {
    native: usize,
    path: PathBuf,
}
/// The attached native instance. Lock order: `MOUNT`, then `ENGINE`, then the
/// engine lock; never taken by a callback.
static MOUNT: Mutex<Option<Mount>> = Mutex::new(None);
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
    fn tk_mount_start(path: *const u16, out: *mut usize) -> u32;
    fn tk_mount_stop(native: usize);
    fn tk_mount_notify(native: usize, path: *const u16, action: u32) -> u32;
}
/// Called outside the engine mutex, using paths in their WinFsp-normalized form.
/// Never waits for a mount transition (a detach can hold the adapter for its
/// quiet wait and drain): `BUSY_VIEW`, and the caller keeps the notification
/// for its next delivery.
pub fn notify(path: &str, action: u32) -> Result<()> {
    let guard = match MOUNT.try_lock() {
        Ok(guard) => guard,
        Err(std::sync::TryLockError::Poisoned(poison)) => poison.into_inner(),
        Err(std::sync::TryLockError::WouldBlock) => {
            bail!("BUSY_VIEW: mount transition in progress")
        }
    };
    let Some(m) = guard.as_ref().filter(|m| m.native != 0) else {
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
fn load_runtime() -> Result<()> {
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
    Ok(())
}
fn bound() -> Result<Shared> {
    ENGINE
        .read()
        .unwrap_or_else(PoisonError::into_inner)
        .clone()
        .context("ENGINE_NOT_READY: no engine bound to the mount")
}
/// Sets the engine the callbacks serve. Idempotent for the same `Arc`; a
/// different engine replaces it only while nothing is attached
/// (`ONE_MOUNT_PER_RUNTIME` otherwise). Performs no path check.
pub fn bind(engine: &Shared) -> Result<()> {
    let mount = lock(&MOUNT);
    let mut slot = ENGINE.write().unwrap_or_else(PoisonError::into_inner);
    if let Some(current) = slot.as_ref() {
        if Arc::ptr_eq(current, engine) {
            return Ok(());
        }
        ensure!(
            mount.is_none(),
            "ONE_MOUNT_PER_RUNTIME: another engine owns the attached mount"
        );
    }
    *slot = Some(engine.clone());
    Ok(())
}
/// `MOUNT_PATH_MUST_NOT_EXIST` unless nothing is at `path`.
pub fn vacant(path: &Path) -> Result<()> {
    adapter::vacant(path)
}
/// Whether `engine` is the engine the callbacks serve.
pub fn serves(engine: &Shared) -> bool {
    ENGINE
        .read()
        .unwrap_or_else(PoisonError::into_inner)
        .as_ref()
        .is_some_and(|bound| Arc::ptr_eq(bound, engine))
}
/// The path of the attached native instance, if any. Never waits for a
/// transition in progress.
pub fn attached() -> Option<PathBuf> {
    adapter::attached_path()
}
/// Whether the attached instance was unmounted outside this process. Always
/// `false` for WinFsp: the mount point lives as long as the native instance,
/// which only `tk_mount_stop` ends (FUSE has a real probe).
pub fn vanished() -> bool {
    false
}
/// Callbacks currently running.
pub fn inflight() -> usize {
    adapter::inflight()
}
/// Mount-scoped callback failures since the process started.
pub fn mount_errors() -> u64 {
    adapter::mount_errors()
}
/// Starts a fresh WinFsp instance at `path` for the bound engine. The path
/// must not exist (WinFsp creates and removes the mount point).
pub fn attach(path: &Path) -> Result<()> {
    let mut guard = lock(&MOUNT);
    if let Some(mount) = guard.as_ref() {
        bail!(
            "ONE_MOUNT_PER_RUNTIME: already attached at {}",
            mount.path.display()
        );
    }
    let engine = bound()?;
    load_runtime()?;
    adapter::vacant(path)?;
    let mut native = 0;
    let status = unsafe { tk_mount_start(wide(path).as_ptr(), &mut native) };
    ensure!(status == 0, "WINFSP_MOUNT_FAILED: 0x{status:08x}");
    *guard = Some(Mount {
        native,
        path: path.to_owned(),
    });
    adapter::set_attached_path(Some(path.to_owned()));
    adapter::attached(&engine);
    Ok(())
}
/// Stops the attached instance (no-op when nothing is attached): fence →
/// drain (≤ `DETACH_DRAIN`) → [force: `retain_all_dirty`] → `tk_mount_stop`
/// → release handles → unfence. A graceful detach is refused with
/// `BUSY_VIEW` (and stays attached, unfenced) while handles are open, a
/// session is dirty, or callbacks do not drain.
pub fn detach(force: bool) -> Result<()> {
    let mut guard = lock(&MOUNT);
    let Some(mount) = guard.as_ref() else {
        return Ok(());
    };
    let engine = bound()?;
    adapter::fence(&engine, force)?;
    if !adapter::drain(DETACH_DRAIN) && !force {
        adapter::unfence(&engine);
        bail!(
            "BUSY_VIEW: {} filesystem callbacks still running",
            adapter::inflight()
        );
    }
    if force {
        adapter::persist_dirty(&engine);
    }
    // Never hold the engine mutex while stopping callbacks.
    if mount.native != 0 {
        unsafe { tk_mount_stop(mount.native) };
    }
    *guard = None;
    adapter::set_attached_path(None);
    adapter::release(&engine);
    Ok(())
}
/// Legacy entry (`daemon --mount`): bind, then attach.
pub fn start(engine: Shared, path: &Path) -> Result<()> {
    bind(&engine)?;
    attach(path)
}
/// Legacy `stop` op: refuses unless the engine is quiet, then detaches
/// gracefully. The fence is lifted afterwards.
pub fn stop(engine: &Shared) -> Result<()> {
    lock_engine(engine).quiet()?;
    detach(false)
}
pub fn checkout(engine: &Shared, request: &str, payload: &Value) -> Result<Value> {
    let mut mount_guard = lock(&MOUNT);
    let mount = mount_guard.as_mut().context("MOUNT_NOT_RUNNING")?;
    {
        let mut e = lock_engine(engine);
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
        e.switching = true;
    }
    // New opens fenced before stopping. No engine mutex during dispatcher stop.
    adapter::drain(DETACH_DRAIN);
    unsafe { tk_mount_stop(mount.native) };
    mount.native = 0;
    let result = {
        let mut e = lock_engine(engine);
        e.mounted = false;
        e.quiet().and_then(|()| e.control(request, payload))
    };
    let remount = crate::util::fault::fail("fs.remount").and_then(|()| {
        let mut native = 0;
        let status = unsafe { tk_mount_start(wide(&mount.path).as_ptr(), &mut native) };
        ensure!(status == 0, "WINFSP_MOUNT_FAILED: 0x{status:08x}");
        Ok(native)
    });
    match remount {
        Ok(native) => {
            mount.native = native;
            adapter::attached(engine);
            result
        }
        Err(error) => {
            // The mount is gone: Lost (never `Engine.health`).
            *mount_guard = None;
            adapter::set_attached_path(None);
            adapter::release(engine);
            let error = anyhow::anyhow!("REMOUNT_FAILED: {error:#}");
            adapter::report_lost(&error);
            Err(error)
        }
    }
}
fn fill(info: &mut Info, entry: &Entry, size: usize) {
    info.size = size as u64;
    let basic = entry.basic_info();
    info.attributes = basic.attributes;
    info.creation_time = basic.creation_time;
    info.access_time = basic.access_time;
    info.write_time = basic.write_time;
    info.change_time = basic.change_time;
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
    // Counted until return, so a detach can drain callbacks before stopping.
    let _callback = adapter::callback();
    let run = std::panic::catch_unwind(|| -> Result<u32> {
        let engine = bound()?;
        let engine = &engine;
        let h = unsafe { *token };
        let path = unsafe { string(name) };
        if op == 4 || op == 5 || op == 6 && offset != 0 {
            crate::runtime::prepare_content(engine, h, std::time::Duration::from_secs(15))?;
        }
        let mut e = lock_engine(engine);
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
                        .map(|h| e.store.object_size(h).map(|size| size as usize))
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
                let attributes = if op == 2 && length == 4 {
                    Some(unsafe { std::ptr::read_unaligned(buffer.cast::<u32>()) })
                } else {
                    None
                };
                let (handle, entry) =
                    e.open_with_attributes(&path, create, flags & 2 != 0, attributes)?;
                unsafe { *token = handle };
                fill(&mut out, &entry, e.session(handle)?.bytes.len());
            }
            4 => {
                let s = e.session(h)?;
                let start = usize::try_from(offset)?;
                if start >= s.bytes.len() {
                    return Ok(0xc0000011);
                }
                count = s.bytes.read_at(start, unsafe {
                    std::slice::from_raw_parts_mut(buffer, length as usize)
                })? as u32;
            }
            5 => {
                let bytes = unsafe { std::slice::from_raw_parts(buffer, length as usize) };
                count = e.write(h, offset, bytes, flags & 1 != 0, flags & 2 != 0)? as u32;
            }
            6 => e.truncate(h, offset, flags & 1 != 0)?,
            7 => e.flush(h)?,
            8 => {}
            9 => e.cleanup(h, flags & 1 != 0)?,
            10 => {
                e.close(h)?;
                // Qualification hook: a close-path failure after the handle closed.
                crate::util::fault::fail("fs.close")?;
            }
            11 => {
                let s = e.session(h)?;
                ensure!(
                    s.entry.id != ROOT && s.entry.id != "control",
                    "PERMISSION_DENIED"
                );
                ensure!(
                    s.entry.kind == "directory" || s.entry.basic_info().attributes & 1 == 0,
                    "ACCESS_DENIED: readonly file"
                );
                ensure!(
                    e.store.children(&s.entry.id)?.is_empty(),
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
                // Runtime discovery is direct-lookup-only, never a directory entry.
                // FindFirstFile on a completely empty directory must still open a
                // search handle. Git's Windows opendir rejects ERROR_FILE_NOT_FOUND;
                // ordinary directory consumers discard these standard dot entries.
                let Some((entry, size)) =
                    e.directory_entry(h, usize::try_from(offset)?, flags & 1 != 0)?
                else {
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
                fill(&mut out, &entry, size);
            }
            14 => {
                #[repr(C)]
                #[derive(Clone, Copy)]
                struct BasicUpdate {
                    times: [u64; 4],
                    attributes: u32,
                }
                ensure!(
                    length as usize == std::mem::size_of::<BasicUpdate>(),
                    "INVALID_PARAMETER"
                );
                let update = unsafe { std::ptr::read_unaligned(buffer.cast::<BasicUpdate>()) };
                e.set_basic(h, update.attributes, update.times)?;
            }
            _ => bail!("UNSUPPORTED_FS_OPERATION"),
        }
        if [5, 6, 7, 8, 14].contains(&op) && h != 0 {
            let s = e.session(h)?;
            fill(&mut out, &s.entry, s.bytes.len());
        }
        if !info.is_null() {
            unsafe { *info = out };
        }
        if !transferred.is_null() {
            unsafe { *transferred = count };
        }
        e.publish_observation();
        Ok(0)
    });
    match run {
        Ok(Ok(status)) => status,
        Ok(Err(error)) => {
            let status = nt_status(&error);
            // Mount-scoped: counted and logged at a bounded rate, never
            // written to `Engine.health` (which would block stop, RC11).
            adapter::fs_error(
                op,
                &error,
                [9, 10].contains(&op) || status == STATUS_IO_DEVICE_ERROR,
            );
            status
        }
        Err(_) => {
            adapter::fs_error(
                op,
                &anyhow::anyhow!("UNKNOWN_ERROR: filesystem callback panicked"),
                true,
            );
            STATUS_IO_DEVICE_ERROR
        }
    }
}
const STATUS_IO_DEVICE_ERROR: u32 = 0xc0000185;
fn nt_status(error: &anyhow::Error) -> u32 {
    let message = format!("{error:#}");
    if message.contains("CONTENT_UNAVAILABLE") {
        0xc0000267 // STATUS_FILE_IS_OFFLINE
    } else if message.contains("FILE_NOT_FOUND") {
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
    } else if message.contains("UNSUPPORTED") {
        0xc00000bb
    } else if message.contains("INVALID_TIMESTAMP")
        || message.contains("INVALID_ATTRIBUTES")
        || message.contains("INVALID_PARAMETER")
    {
        0xc000000d
    } else {
        STATUS_IO_DEVICE_ERROR
    }
}
