//! Owned by WP1.4.
//!
//! `STATE/worker.json`, the adoption record (REVAMP_PLAN §3.3, §6.1): written
//! atomically once the RPC listener is up, read back by the next worker (FUSE
//! recovery, §4.1 step 2) and by the orchestrator (adoption), removed on a
//! clean exit. Also the process identity it carries: `process_start` (the
//! Windows creation FILETIME, or Linux `/proc/<pid>/stat` field 22) and the
//! executable path, which adoption compares before trusting or killing a pid.
use crate::model::ipc::{WORKER_RECORD_FILE, WorkerRecord};
use anyhow::{Context, Result};
use std::{
    fs::{self, File},
    io::{ErrorKind, Write},
    path::{Path, PathBuf},
    sync::OnceLock,
};

/// `STATE/worker.json`.
pub fn path(state: &Path) -> PathBuf {
    state.join(WORKER_RECORD_FILE)
}

/// Writes `STATE/worker.json` atomically: a fsynced temporary file renamed
/// over the old record (and, on unix, a fsync of the directory), so a reader
/// sees the old record or the new one, never a torn one.
pub fn write_atomic(state: &Path, record: &WorkerRecord) -> Result<()> {
    let target = path(state);
    let temporary = state.join(format!("{WORKER_RECORD_FILE}.{}.tmp", std::process::id()));
    let written = (|| -> Result<()> {
        let mut file = File::create(&temporary)?;
        file.write_all(&serde_json::to_vec_pretty(record)?)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temporary, &target)?;
        #[cfg(unix)]
        File::open(state)?.sync_all()?;
        Ok(())
    })();
    if written.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    written.with_context(|| format!("cannot write {}", target.display()))
}

/// The record in `state`, `None` if there is none. A record that cannot be
/// parsed is an error (callers that only want a hint treat it as absent).
pub fn read(state: &Path) -> Result<Option<WorkerRecord>> {
    let target = path(state);
    match fs::read(&target) {
        Ok(bytes) => {
            Ok(Some(serde_json::from_slice(&bytes).with_context(|| {
                format!("invalid {}", target.display())
            })?))
        }
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

/// Removes the record; a missing record is not an error.
pub fn remove(state: &Path) -> Result<()> {
    match fs::remove_file(path(state)) {
        Err(error) if error.kind() != ErrorKind::NotFound => Err(error.into()),
        _ => Ok(()),
    }
}

/// This process's `process_start` (cached; 0 if the OS does not report it,
/// which never matches a live process, so adoption then fails closed).
pub fn process_start() -> u64 {
    static START: OnceLock<u64> = OnceLock::new();
    *START.get_or_init(|| process_start_of(std::process::id()).unwrap_or(0))
}

/// This process's executable, as recorded in `worker.json`.
pub fn executable() -> Result<PathBuf> {
    Ok(std::env::current_exe()?)
}

/// The `process_start` of a live process: the Windows creation FILETIME
/// (`GetProcessTimes`), or Linux `/proc/<pid>/stat` field 22 (start time in
/// clock ticks since boot). `None` if the process does not exist or cannot be
/// queried.
#[cfg(windows)]
pub fn process_start_of(pid: u32) -> Option<u64> {
    use std::ffi::c_void;
    #[repr(C)]
    #[derive(Default)]
    struct FileTime {
        low: u32,
        high: u32,
    }
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn OpenProcess(access: u32, inherit: i32, pid: u32) -> *mut c_void;
        fn GetProcessTimes(
            process: *mut c_void,
            creation: *mut FileTime,
            exit: *mut FileTime,
            kernel: *mut FileTime,
            user: *mut FileTime,
        ) -> i32;
        fn CloseHandle(handle: *mut c_void) -> i32;
    }
    const PROCESS_QUERY_LIMITED_INFORMATION: u32 = 0x1000;
    let handle = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
    if handle.is_null() {
        return None;
    }
    let mut creation = FileTime::default();
    let (mut exit, mut kernel, mut user) = (
        FileTime::default(),
        FileTime::default(),
        FileTime::default(),
    );
    let ok = unsafe { GetProcessTimes(handle, &mut creation, &mut exit, &mut kernel, &mut user) };
    unsafe { CloseHandle(handle) };
    (ok != 0).then(|| (u64::from(creation.high) << 32) | u64::from(creation.low))
}

/// See the Windows variant.
#[cfg(target_os = "linux")]
pub fn process_start_of(pid: u32) -> Option<u64> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    // Field 2 (comm) is parenthesized and may contain spaces and parentheses;
    // the fields after the last ')' start at field 3.
    let rest = &stat[stat.rfind(')')? + 1..];
    rest.split_whitespace().nth(22 - 3)?.parse().ok()
}

/// See the Windows variant (no process start time on this platform).
#[cfg(not(any(windows, target_os = "linux")))]
pub fn process_start_of(_pid: u32) -> Option<u64> {
    None
}
