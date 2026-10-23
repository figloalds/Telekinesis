//! WinFsp callbacks use host scratch files and the pathname-shared Windows cache.
use crate::{
    content::working::{PENDING_BUDGET, Scratch, WRITEBACK_DELAY, lock},
    core::{Entry, EntryKind},
    runtime::Engine,
};
use anyhow::{Context, Result, ensure};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::atomic::Ordering,
    sync::{Arc, Mutex, RwLock},
    time::{Duration, Instant},
};

static ENGINE: RwLock<Option<Arc<Engine>>> = RwLock::new(None);
static MOUNT: Mutex<Option<(usize, PathBuf, u64)>> = Mutex::new(None);
static CACHE_DRAIN: Mutex<()> = Mutex::new(());
type CachedClose = (Instant, u64, Arc<Mutex<Scratch>>);
static CACHED_CLOSES: Mutex<BTreeMap<String, CachedClose>> = Mutex::new(BTreeMap::new());

fn cache_close(engine: &Engine, token: u64, modified: bool) -> Result<()> {
    let handle = engine.handle(token)?;
    if !handle.writable {
        return Ok(());
    }
    if let Some(scratch) = &handle.scratch {
        let (id, generation) = {
            let mut s = lock(scratch);
            if !modified && !s.cache_pending {
                return Ok(());
            }
            s.cache_pending = true;
            s.cache_generation = s.cache_generation.wrapping_add(1);
            (s.record.id.clone(), s.cache_generation)
        };
        lock(&CACHED_CLOSES).insert(id, (Instant::now(), generation, scratch.clone()));
    }
    Ok(())
}
fn cache_flushed(engine: &Engine, scratch: &Arc<Mutex<Scratch>>, expected: Option<u64>) {
    let (id, generation) = {
        let mut s = lock(scratch);
        if expected.is_some_and(|g| g != s.cache_generation) {
            return;
        }
        s.cache_pending = false;
        if s.writers == 0 && s.changed {
            s.closed_at = Instant::now().checked_sub(WRITEBACK_DELAY);
        }
        (s.record.id.clone(), s.cache_generation)
    };
    let mut pending = lock(&CACHED_CLOSES);
    if pending.get(&id).is_some_and(|(_, g, _)| *g == generation) {
        pending.remove(&id);
    }
    drop(pending);
    engine.working.schedule(scratch);
}
/// A root-directory flush synchronously drains the Windows volume cache,
/// including read-only files, then seals its materialized bytes as one batch.
pub fn flush_cached(force: bool) -> Result<()> {
    let _drain = lock(&CACHE_DRAIN);
    let Some(engine) = ENGINE.read().unwrap().clone() else {
        return Ok(());
    };
    if force {
        for token in engine.writable_tokens() {
            cache_close(&engine, token, true)?;
        }
    }
    let pending = lock(&CACHED_CLOSES).clone();
    let bytes: u64 = pending.values().map(|(_, _, s)| lock(s).length).sum();
    let due = force
        || pending.values().any(|(closed, _, scratch)| {
            lock(scratch).writers == 0
                && (bytes > PENDING_BUDGET || closed.elapsed() >= WRITEBACK_DELAY)
        });
    if !due || pending.is_empty() {
        return Ok(());
    }
    let root = lock(&MOUNT).as_ref().map(|(_, _, root)| *root);
    if let Some(root) = root {
        let status = unsafe { tk_cache_flush(root) };
        ensure!(status == 0, "CACHE_DRAIN_FAILED: {status:08x}");
    }
    for (_, generation, scratch) in pending.values() {
        cache_flushed(&engine, scratch, Some(*generation));
    }
    Ok(())
}
static CALLS: [std::sync::atomic::AtomicU64; 16] =
    [const { std::sync::atomic::AtomicU64::new(0) }; 16];
static NANOS: [std::sync::atomic::AtomicU64; 16] =
    [const { std::sync::atomic::AtomicU64::new(0) }; 16];
pub fn stats() -> Vec<serde_json::Value> {
    (1..16).map(|i|serde_json::json!({"operation":i,"calls":CALLS[i].load(Ordering::Relaxed),"ms":NANOS[i].load(Ordering::Relaxed) as f64/1e6})).collect()
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
    fn tk_mount_start(path: *const u16, out: *mut usize, threads: u32, timeout: u32) -> u32;
    fn tk_mount_stop(native: usize);
    fn tk_mount_notify(native: usize, path: *const u16, action: u32) -> u32;
    fn tk_cache_open(path: *const u16, out: *mut u64) -> u32;
    fn tk_cache_flush(root: u64) -> u32;
    fn tk_cache_close(root: u64);
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
pub fn attach(engine: Arc<Engine>, path: &Path) -> Result<()> {
    let mut mount = lock(&MOUNT);
    ensure!(mount.is_none(), "MOUNT_ALREADY_ATTACHED");
    let mut attempted = vec![0; 1024];
    let status = unsafe { tk_winfsp_load(attempted.as_mut_ptr(), 1024) };
    ensure!(status == 0, "WINFSP_LOAD_FAILED: {status:08x}");
    *ENGINE.write().unwrap() = Some(engine.clone());
    let wide: Vec<_> = path
        .as_os_str()
        .to_string_lossy()
        .encode_utf16()
        .chain(Some(0))
        .collect();
    let mut native = 0;
    let status = unsafe { tk_mount_start(wide.as_ptr(), &mut native, 0, 60_000) };
    if status != 0 {
        *ENGINE.write().unwrap() = None;
        anyhow::bail!("MOUNT_FAILED: {status:08x}");
    }
    let mut root = 0;
    let status = unsafe { tk_cache_open(wide.as_ptr(), &mut root) };
    if status != 0 {
        unsafe { tk_mount_stop(native) };
        *ENGINE.write().unwrap() = None;
        anyhow::bail!("CACHE_ROOT_OPEN_FAILED: {status:08x}");
    }
    *mount = Some((native, path.to_owned(), root));
    engine.set_invalidator(Arc::new(notify));
    let weak = Arc::downgrade(&engine);
    std::thread::Builder::new()
        .name("tkfs-cache-drain".into())
        .spawn(move || {
            loop {
                std::thread::sleep(Duration::from_millis(25));
                let Some(engine) = weak.upgrade() else {
                    break;
                };
                if engine.stopping.load(Ordering::Acquire) {
                    break;
                }
                if let Err(error) = flush_cached(false) {
                    *lock(&engine.error) = Some(format!("{error:#}"));
                }
            }
        })?;
    Ok(())
}
pub fn detach() -> Result<()> {
    flush_cached(true)?;
    let mut mount = lock(&MOUNT);
    if let Some((native, _, root)) = mount.take() {
        unsafe {
            tk_cache_close(root);
            tk_mount_stop(native)
        };
    }
    if let Some(engine) = ENGINE.write().unwrap().take() {
        engine.flush(None)?;
    }
    Ok(())
}
pub fn notify(path: &str, action: u32) -> Result<()> {
    let mount = lock(&MOUNT);
    if let Some((native, _, _)) = mount.as_ref() {
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
                let attrs = if op == 2 && !buffer.is_null() {
                    Some(unsafe { *(buffer as *const u32) })
                } else {
                    None
                };
                let h = engine.open_file_with_attributes(
                    &unsafe { text(name) }?,
                    op == 2,
                    flags & 1 != 0,
                    flags & 2 != 0,
                    attrs,
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
                let n = engine.write(h, offset, bytes, flags & 1 != 0, flags & 2 != 0)?;
                if !count.is_null() {
                    unsafe { *count = n as u32 };
                }
                let (e, len) = engine.info(h)?;
                unsafe { *info = fill(&e, len) };
            }
            6 => {
                engine.truncate(h, offset, flags & 1 != 0)?;
                let (e, len) = engine.info(h)?;
                unsafe { *info = fill(&e, len) };
            }
            7 => {
                if h != 0
                    && let Some(scratch) = &engine.handle(h)?.scratch
                {
                    cache_flushed(&engine, scratch, None);
                }
                if h == 0 {
                    engine.flush_native_volume()?;
                } else {
                    engine.flush(Some(h))?;
                }
                if h != 0 {
                    let (e, len) = engine.info(h)?;
                    unsafe { *info = fill(&e, len) };
                }
            }
            8 => {
                let (e, len) = engine.info_visible(h)?;
                unsafe { *info = fill(&e, len) };
            }
            9 => {
                if flags & 1 == 0 {
                    cache_close(&engine, h, flags & 2 != 0)?;
                }
                engine.cleanup(h, flags & 1 != 0)?;
            }
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
        Ok(Err(error)) => status(&error),
        Err(_) => 0xc0000001,
    }
}
