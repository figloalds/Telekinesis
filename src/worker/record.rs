//! Owned by WP1.4; WP4.1 adds the worker's local token file.
//!
//! `STATE/worker.json`, the adoption record (REVAMP_PLAN §3.3, §6.1): written
//! atomically once the RPC listener is up, read back by the next worker (FUSE
//! recovery, §4.1 step 2) and by the orchestrator (adoption), removed on a
//! clean exit. Also the process identity it carries: `process_start` (the
//! Windows creation FILETIME, or Linux `/proc/<pid>/stat` field 22) and the
//! executable path, which adoption compares before trusting or killing a pid.
//!
//! **Local token files (WP4.1).** `STATE/runtime.token` holds the worker
//! instance's token (also in `worker.json` and the Ready line), which its
//! JSON RPC and loopback sessions require. Secret files are written with
//! [`write_private`]: atomically, and owner-only. On Unix the file is created
//! with mode 0600 before any byte is written. On Windows it inherits the ACL
//! of its directory: the state and data directories live under the user's
//! profile, whose ACL grants the user (plus SYSTEM and Administrators) only.
use crate::model::ipc::{WORKER_RECORD_FILE, WorkerRecord};
use anyhow::{Context, Result};
use std::{
    fs::{self, File},
    io::{ErrorKind, Write},
    path::{Path, PathBuf},
    sync::{
        OnceLock,
        atomic::{AtomicU64, Ordering},
    },
};

/// `STATE/worker.json`.
pub fn path(state: &Path) -> PathBuf {
    state.join(WORKER_RECORD_FILE)
}

/// Writes `STATE/worker.json` atomically: a fsynced temporary file renamed
/// over the old record (and, on unix, a fsync of the directory), so a reader
/// sees the old record or the new one, never a torn one.
pub fn write_atomic(state: &Path, record: &WorkerRecord) -> Result<()> {
    replace_file(&path(state), &serde_json::to_vec_pretty(record)?)
}

/// Replaces `target` atomically with `bytes`: a fsynced temporary file next
/// to it, renamed over it (and, on unix, a fsync of the directory). Readers
/// see the old content or the new one, never a torn file. Also used for
/// `runtime.json`, which a `mount-attach` may rewrite while readers poll it.
pub fn replace_file(target: &Path, bytes: &[u8]) -> Result<()> {
    replace(target, bytes, false)
}

/// [`replace_file`] for a secret: the file is owner-only (see the module
/// docs). Used for `STATE/runtime.token` and `DATA/orchestrator.token`.
pub fn write_private(target: &Path, bytes: &[u8]) -> Result<()> {
    replace(target, bytes, true)
}

/// `STATE/runtime.token`, the worker instance's local token (WP4.1).
pub const TOKEN_FILE: &str = "runtime.token";

/// `STATE/runtime.token`.
pub fn token_path(state: &Path) -> PathBuf {
    state.join(TOKEN_FILE)
}

/// The token in a token file (trimmed), `None` when the file is missing,
/// unreadable or empty.
pub fn read_token(path: &Path) -> Option<String> {
    let text = fs::read_to_string(path).ok()?;
    let token = text.trim();
    (!token.is_empty() && token.len() <= 1024 && !token.chars().any(char::is_control))
        .then(|| token.to_owned())
}

/// Removes `STATE/runtime.token`; a missing file is not an error.
pub fn remove_token(state: &Path) -> Result<()> {
    match fs::remove_file(token_path(state)) {
        Err(error) if error.kind() != ErrorKind::NotFound => Err(error.into()),
        _ => Ok(()),
    }
}

fn create_temporary(path: &Path, private: bool) -> std::io::Result<File> {
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    if private {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    #[cfg(not(unix))]
    let _ = private;
    options.open(path)
}

fn replace(target: &Path, bytes: &[u8], private: bool) -> Result<()> {
    let directory = target.parent().unwrap_or_else(|| Path::new("."));
    let name = target
        .file_name()
        .with_context(|| format!("no file name in {}", target.display()))?
        .to_string_lossy();
    // Unique per call, so two writers of one file never share a temporary.
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let temporary = directory.join(format!(
        "{name}.{}.{}.tmp",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let written = (|| -> Result<()> {
        let mut file = create_temporary(&temporary, private)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temporary, target)?;
        #[cfg(unix)]
        File::open(directory)?.sync_all()?;
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

/// The `process_start` of a running process: the Windows creation FILETIME
/// (`GetProcessTimes`), or Linux `/proc/<pid>/stat` field 22 (start time in
/// clock ticks since boot). `None` if the process does not exist, cannot be
/// queried, or **has exited**: this is a liveness check as well as an
/// identity one. An exited process can still be queried while it is not
/// reaped (a zombie, `/proc/<pid>/stat` state `Z`, on Linux) or while any
/// handle keeps its process object (for example the parent's
/// `std::process::Child`, on Windows); both read as absent.
///
/// Windows tells a running process by `GetExitCodeProcess` returning
/// `STILL_ACTIVE` (259), so a process that exited *with* code 259 reads as
/// running; tkfs never exits with 259 (its codes are 0, 70, 73, 75, 78 and
/// the fault exit).
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
        fn GetExitCodeProcess(process: *mut c_void, code: *mut u32) -> i32;
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
    const STILL_ACTIVE: u32 = 259;
    let handle = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
    if handle.is_null() {
        return None;
    }
    let mut code = 0u32;
    let running = unsafe { GetExitCodeProcess(handle, &mut code) } != 0 && code == STILL_ACTIVE;
    let mut creation = FileTime::default();
    let (mut exit, mut kernel, mut user) = (
        FileTime::default(),
        FileTime::default(),
        FileTime::default(),
    );
    let ok = unsafe { GetProcessTimes(handle, &mut creation, &mut exit, &mut kernel, &mut user) };
    unsafe { CloseHandle(handle) };
    (running && ok != 0).then(|| (u64::from(creation.high) << 32) | u64::from(creation.low))
}

/// See the Windows variant.
#[cfg(target_os = "linux")]
pub fn process_start_of(pid: u32) -> Option<u64> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    // Field 2 (comm) is parenthesized and may contain spaces and parentheses;
    // the fields after the last ')' start at field 3.
    let mut fields = stat[stat.rfind(')')? + 1..].split_whitespace();
    // Field 3, the state: a zombie (`Z`) or dead (`X`; `x` on 2.6.33–3.13
    // kernels) process has exited.
    if matches!(fields.next()?, "Z" | "X" | "x") {
        return None;
    }
    fields.nth(22 - 4)?.parse().ok()
}

/// See the Windows variant (no process start time on this platform, so
/// adoption fails closed).
#[cfg(not(any(windows, target_os = "linux")))]
pub fn process_start_of(_pid: u32) -> Option<u64> {
    None
}
