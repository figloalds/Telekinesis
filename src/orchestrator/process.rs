//! Worker process launcher and adoption.
//!
//! * [`spawn`] starts `EXE daemon --state S --supervised [--mount M]` with
//!   stdin null, stdout piped and stderr appended to `STATE/worker.log`. A
//!   reader thread copies every stdout line to `worker.log`, redacted
//!   (`util::log::redact_line`: the Ready line carries the worker token), and
//!   turns the Ready and fatal lines into [`ProcessEvent`]s. An exit waiter
//!   owns the [`Child`], blocks in the OS wait, lets the reader drain (≤ 2 s)
//!   and sends [`ProcessEvent::Exited`] with the last [`TAIL_BYTES`] of
//!   `worker.log`. `Exited` is always the last event of a process: the sink is
//!   dropped with it.
//! * [`adopt`] re-attaches to a worker an earlier daemon started: `None`
//!   unless `STATE/worker.json` names a live process whose OS start time and
//!   image ([`process_identity`]) are the recorded ones.
//! * [`kill_verified`] kills a recorded worker only while its identity still
//!   matches, so a recycled pid is never killed.
//!
//! Workers outlive the daemon so that the next one can adopt them: no Job
//! Object, no `PDEATHSIG`, a process group of their own on Unix (a terminal
//! Ctrl-C aimed at the daemon does not reach them), and dropping a
//! [`WorkerProcess`] never kills anything.
use super::worker::{RECORD_FILE, Ready, WorkerRecord, open_private};
use crate::util::{
    lock::lock,
    log::{self, event},
};
use anyhow::{Context, Result, ensure};
use serde_json::Value;
use std::{
    fs::{self, File, OpenOptions},
    io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    process::{Child, ChildStdout, Command, Stdio},
    sync::{Arc, Mutex, Weak, mpsc},
    thread,
    time::{Duration, Instant},
};

/// How long a controller waits for a spawned worker's Ready line.
pub const READY_TIMEOUT: Duration = Duration::from_secs(30);
/// At spawn, a `worker.log` larger than this is rotated…
pub const WORKER_LOG_ROTATE_BYTES: u64 = 10 * 1024 * 1024;
/// …keeping this many files (`worker.log` and `worker.log.1` to `.4`).
pub const WORKER_LOG_FILES: usize = 5;
/// `ProcessEvent::Exited.tail` is at most this much of the end of `worker.log`.
pub const TAIL_BYTES: usize = 4096;

pub const LOG_FILE: &str = "worker.log";
/// After the child exits, the waiter gives the stdout reader this long to
/// reach end of file (a grandchild holding the pipe delays `Exited` no more).
const DRAIN_WAIT: Duration = Duration::from_secs(2);
/// A longer stdout "line" is split (it is still logged).
const MAX_LINE: usize = 1024 * 1024;
/// An adopted process is waited on in steps this long, so the watcher ends
/// soon after its [`WorkerProcess`] is dropped.
const ADOPTED_WAIT: Duration = Duration::from_secs(1);
/// [`terminate`] gives a killed child this long to go.
const TERMINATE_WAIT: Duration = Duration::from_secs(10);
/// On Linux the start time of a record may differ this much from the OS's:
/// it is derived from `/proc/<pid>/stat` clock ticks since boot plus the
/// whole-second `btime` of `/proc/stat`. Readings agree exactly while the wall
/// clock runs (slewing moves boot and wall time alike), but `btime` moves when
/// the wall clock is stepped, so a record written before a step reads
/// differently after it; ±2000 ms absorbs small steps. Windows reads the
/// creation FILETIME itself and compares exactly.
#[cfg(target_os = "linux")]
pub const START_TOLERANCE_MS: u64 = 2000;
/// Windows `CREATE_NO_WINDOW`: the worker never gets a console.
#[cfg(windows)]
const CREATE_NO_WINDOW: u32 = 0x0800_0000;

/// What to launch: the worker's state directory (its log is
/// `state/worker.log`) and an optional mount point.
#[derive(Clone, Debug)]
pub struct LaunchSpec {
    pub executable: PathBuf,
    pub state: PathBuf,
    pub mount: Option<PathBuf>,
}

#[derive(Debug)]
pub enum ProcessEvent {
    Ready(Ready),
    Fatal {
        code: String,
        message: String,
    },
    /// Always the last event of a process. `code` is `None` when the OS gives
    /// none (a Unix signal, an adopted Unix process).
    Exited {
        code: Option<i32>,
        tail: String,
    },
}

/// A spawned or adopted worker. Dropping it never kills the process: workers
/// outlive the daemon so that the next one can adopt them.
pub struct WorkerProcess {
    pub pid: u32,
    /// Spawned: an observer next to the `Child` the exit waiter owns.
    /// Adopted: the observer the watcher waits on (it ends once this is gone).
    /// `None` only for a test stand-in ([`WorkerProcess::detached`]).
    process: Option<Arc<ObservedProcess>>,
}

impl WorkerProcess {
    /// A stand-in naming `pid` without observing any process, for scripted
    /// launchers in tests; [`Self::exited`] fails on it.
    #[cfg(test)]
    pub(crate) fn detached(pid: u32) -> Self {
        Self { pid, process: None }
    }

    pub fn exited(&self) -> Result<bool> {
        match &self.process {
            Some(process) => process.exited(),
            None => anyhow::bail!("PROCESS_DETACHED: pid {} is not observed", self.pid),
        }
    }
}

/// Spawns a worker for `spec`; its events go to `sink`.
pub fn spawn(spec: &LaunchSpec, sink: mpsc::Sender<ProcessEvent>) -> Result<WorkerProcess> {
    let executable = spec.executable.display().to_string();
    let failed = |step: &str| format!("WORKER_SPAWN_FAILED: {executable}: {step}");
    fs::create_dir_all(&spec.state).with_context(|| failed("create state directory"))?;
    let log = spec.state.join(LOG_FILE);
    if let Err(error) = log::rotate_if_larger(&log, WORKER_LOG_ROTATE_BYTES, WORKER_LOG_FILES) {
        event!(warn, WORKER_LOG_ROTATE_FAILED, {
            "path": log.display().to_string(),
            "error": error.to_string(),
        });
    }
    let log_file = open_private(OpenOptions::new().create(true).append(true), &log)
        .with_context(|| failed("open worker.log"))?;
    let stderr = log_file
        .try_clone()
        .with_context(|| failed("open worker.log"))?;
    let mut command = Command::new(&spec.executable);
    command
        .arg("daemon")
        .arg("--state")
        .arg(&spec.state)
        .arg("--supervised");
    if let Some(mount) = &spec.mount {
        command.arg("--mount").arg(mount);
    }
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(stderr);
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        command.creation_flags(CREATE_NO_WINDOW);
    }
    #[cfg(unix)]
    {
        // `setpgid(0, 0)` in the child.
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
    let mut child = command.spawn().with_context(|| failed("spawn"))?;
    // The command holds a copy of the stderr handle; close it.
    drop(command);
    let pid = child.id();
    let started = (|| {
        let process = Arc::new(ObservedProcess::open(pid)?);
        let stdout = child.stdout.take().context("stdout is not piped")?;
        let emitter = Arc::new(Emitter::new(sink));
        let (drained, drained_rx) = mpsc::channel::<()>();
        {
            let emitter = emitter.clone();
            thread::Builder::new()
                .name("tkfs-worker-out".into())
                .spawn(move || read_stdout(stdout, log_file, &emitter, drained))?;
        }
        // The waiter gets the child through a channel, so a failed thread
        // start leaves it here to be terminated.
        let (child_tx, child_rx) = mpsc::channel::<Child>();
        thread::Builder::new()
            .name("tkfs-worker-exit".into())
            .spawn(move || {
                let Ok(mut child) = child_rx.recv() else {
                    return;
                };
                let code = child.wait().ok().and_then(|status| status.code());
                let _ = drained_rx.recv_timeout(DRAIN_WAIT);
                emitter.exit(code, tail(&log));
            })?;
        anyhow::Ok((process, child_tx))
    })();
    match started {
        Ok((process, child_tx)) => match child_tx.send(child) {
            Ok(()) => Ok(WorkerProcess {
                pid,
                process: Some(process),
            }),
            Err(mpsc::SendError(mut child)) => {
                let error = anyhow::anyhow!(failed("start exit waiter"));
                Err(with_cleanup(error, terminate(&mut child)))
            }
        },
        Err(error) => {
            let error = error.context(failed("observe and start helper threads"));
            Err(with_cleanup(error, terminate(&mut child)))
        }
    }
}

/// `error`, carrying a failed cleanup (`STRANDED_PROCESS`) along with it.
fn with_cleanup(error: anyhow::Error, cleanup: Result<()>) -> anyhow::Error {
    match cleanup {
        Ok(()) => error,
        Err(cleanup) => error.context(format!("cleanup: {cleanup:#}")),
    }
}

/// Re-attaches to the worker `state/worker.json` names: `None` unless that
/// process is alive with the recorded start time and image. Its `Exited`
/// carries the exit code on Windows (none on Unix) and the `worker.log` tail.
pub fn adopt(
    state: &Path,
    sink: mpsc::Sender<ProcessEvent>,
) -> Result<Option<(WorkerRecord, WorkerProcess)>> {
    let path = state.join(RECORD_FILE);
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("WORKER_RECORD_UNREADABLE: {}", path.display()));
        }
    };
    // Written by rename, so never partial; an unreadable one names nothing.
    let Ok(record) = serde_json::from_slice::<WorkerRecord>(&bytes) else {
        return Ok(None);
    };
    let Ok(process) = ObservedProcess::open(record.pid) else {
        return Ok(None);
    };
    // Verified on the handle that is then watched: the pid cannot have been
    // recycled in between.
    if !process
        .identity()
        .is_ok_and(|identity| matches(&record, &identity))
    {
        return Ok(None);
    }
    let process = Arc::new(process);
    let watched = Arc::downgrade(&process);
    let emitter = Emitter::new(sink);
    let log = state.join(LOG_FILE);
    thread::Builder::new()
        .name("tkfs-worker-adopted".into())
        .spawn(move || watch_adopted(&watched, &emitter, &log))
        .context("WORKER_ADOPT_FAILED: start watcher")?;
    let pid = record.pid;
    Ok(Some((
        record,
        WorkerProcess {
            pid,
            process: Some(process),
        },
    )))
}

/// Sends `Exited` when the adopted process exits; ends without an event once
/// its [`WorkerProcess`] is dropped.
fn watch_adopted(process: &Weak<ObservedProcess>, emitter: &Emitter, log: &Path) {
    while let Some(process) = process.upgrade() {
        match process.wait(ADOPTED_WAIT) {
            Ok(true) => {
                emitter.exit(process.exit_code(), tail(log));
                return;
            }
            Ok(false) => {}
            Err(_) => thread::sleep(ADOPTED_WAIT),
        }
    }
}

/// Whether an OS identity is the recorded one: the same image and start time
/// (within `START_TOLERANCE_MS`, ±2000 ms, on Linux; exact elsewhere).
fn matches(record: &WorkerRecord, (start_ms, image): &(u64, PathBuf)) -> bool {
    #[cfg(target_os = "linux")]
    let same_start = start_ms.abs_diff(record.start_ms) <= START_TOLERANCE_MS;
    #[cfg(not(target_os = "linux"))]
    let same_start = *start_ms == record.start_ms;
    same_start && *image == record.image
}

/// `(start_ms, image)` of a live process: its OS start time in ms since the
/// Unix epoch and its executable path. Windows: `GetProcessTimes` creation
/// FILETIME and `QueryFullProcessImageNameW`. Linux: `/proc/<pid>/stat` field
/// 22 (clock ticks since boot, plus `btime`) and `/proc/<pid>/exe` (without
/// the ` (deleted)` suffix of a replaced binary). An error when the process
/// is gone or cannot be inspected.
pub fn process_identity(pid: u32) -> Result<(u64, PathBuf)> {
    #[cfg(windows)]
    {
        ObservedProcess::open_with(pid, os::QUERY | os::SYNCHRONIZE)?.identity()
    }
    #[cfg(target_os = "linux")]
    {
        os::identity(pid)
    }
    #[cfg(not(any(windows, target_os = "linux")))]
    {
        anyhow::bail!("PROCESS_IDENTITY_UNSUPPORTED: pid {pid}")
    }
}

/// Kills the recorded worker if it is still that process: `Ok(true)` after a
/// verified kill, `Ok(false)` without killing when the pid is gone or names
/// another process now. Windows verifies the identity on the handle it
/// terminates, and Linux on the pidfd it then signals
/// (`pidfd_send_signal`), so a recycled pid is never hit. Other platforms
/// cannot verify an identity and never kill (`Ok(false)`).
pub fn kill_verified(record: &WorkerRecord) -> Result<bool> {
    let pid = record.pid;
    if pid == 0 || pid == std::process::id() {
        return Ok(false);
    }
    #[cfg(windows)]
    {
        let process =
            match ObservedProcess::open_with(pid, os::QUERY | os::SYNCHRONIZE | os::TERMINATE) {
                Ok(process) => process,
                // Gone or not ours, unless it is the worker and we may not kill it.
                Err(error) => {
                    return match process_identity(pid) {
                        Ok(identity) if matches(record, &identity) => {
                            Err(error.context(format!("WORKER_KILL_FAILED: pid {pid}")))
                        }
                        _ => Ok(false),
                    };
                }
            };
        if !process
            .identity()
            .is_ok_and(|identity| matches(record, &identity))
        {
            return Ok(false);
        }
        if let Err(error) = process.kill() {
            if process.exited()? {
                return Ok(false);
            }
            return Err(error.context(format!("WORKER_KILL_FAILED: pid {pid}")));
        }
        Ok(true)
    }
    #[cfg(unix)]
    {
        let Ok(process) = ObservedProcess::open(pid) else {
            return Ok(false);
        };
        if !process
            .identity()
            .is_ok_and(|identity| matches(record, &identity))
        {
            return Ok(false);
        }
        // Linux signals the verified pidfd itself: a worker reaped meanwhile
        // gives ESRCH, never a recycled pid's process.
        #[cfg(target_os = "linux")]
        let sent = {
            use std::os::fd::AsRawFd;
            unsafe {
                libc::syscall(
                    libc::SYS_pidfd_send_signal,
                    process.pidfd.as_raw_fd(),
                    libc::SIGKILL,
                    std::ptr::null::<libc::siginfo_t>(),
                    0,
                )
            }
        };
        // Unreachable in practice: `identity` is unsupported here.
        #[cfg(not(target_os = "linux"))]
        let sent = unsafe { libc::kill(pid as libc::pid_t, libc::SIGKILL) };
        if sent != 0 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::ESRCH) {
                return Ok(false);
            }
            return Err(anyhow::Error::new(error).context(format!("WORKER_KILL_FAILED: pid {pid}")));
        }
        Ok(true)
    }
    #[cfg(not(any(windows, unix)))]
    {
        Ok(false)
    }
}

/// Kills `child` (if still running) and waits ≤ 10 s for it to go;
/// `STRANDED_PROCESS` otherwise.
pub fn terminate(child: &mut Child) -> Result<()> {
    if child.try_wait()?.is_some() {
        return Ok(());
    }
    let killed = child.kill();
    let end = Instant::now() + TERMINATE_WAIT;
    loop {
        if child.try_wait()?.is_some() {
            return Ok(());
        }
        ensure!(
            Instant::now() < end,
            "STRANDED_PROCESS: {} ({killed:?})",
            child.id()
        );
        thread::sleep(Duration::from_millis(20));
    }
}

// ---------------------------------------------------------------------------
// Spawned children

/// Delivers one process's events: lines in stdout order, then one `Exited`,
/// which drops the sink.
struct Emitter(Mutex<Option<mpsc::Sender<ProcessEvent>>>);

impl Emitter {
    fn new(sink: mpsc::Sender<ProcessEvent>) -> Self {
        Self(Mutex::new(Some(sink)))
    }

    fn send(&self, event: ProcessEvent) {
        if let Some(sink) = lock(&self.0).as_ref() {
            let _ = sink.send(event);
        }
    }

    fn exit(&self, code: Option<i32>, tail: String) {
        if let Some(sink) = lock(&self.0).take() {
            let _ = sink.send(ProcessEvent::Exited { code, tail });
        }
    }
}

fn read_stdout(stdout: ChildStdout, mut log: File, emitter: &Emitter, drained: mpsc::Sender<()>) {
    let mut reader = BufReader::new(stdout);
    let mut buffer = Vec::new();
    loop {
        buffer.clear();
        match reader
            .by_ref()
            .take(MAX_LINE as u64)
            .read_until(b'\n', &mut buffer)
        {
            Ok(0) => break,
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(_) => break,
        }
        let text = String::from_utf8_lossy(&buffer);
        let text = text.trim_end_matches(['\r', '\n']);
        // A failing log write (disk full) must not stop the pipe draining.
        let _ = writeln!(log, "{}", log::redact_line(text));
        if let Some(event) = parse_line(text) {
            emitter.send(event);
        }
    }
    let _ = drained.send(());
}

/// The Ready line or a fatal line `{"fatal":true,"code","message","exit"}`.
fn parse_line(line: &str) -> Option<ProcessEvent> {
    let value: Value = serde_json::from_str(line.trim()).ok()?;
    if value.get("ready") == Some(&Value::Bool(true)) {
        return serde_json::from_value(value).ok().map(ProcessEvent::Ready);
    }
    if value.get("fatal") == Some(&Value::Bool(true)) {
        let text = |key: &str| value.get(key).and_then(Value::as_str).map(str::to_owned);
        return Some(ProcessEvent::Fatal {
            code: text("code").unwrap_or_else(|| "WORKER_FATAL".into()),
            message: text("message").unwrap_or_default(),
        });
    }
    None
}

/// The last ≤ [`TAIL_BYTES`] of `path`, from a character boundary (empty if
/// it cannot be read).
fn tail(path: &Path) -> String {
    let read = || -> io::Result<Vec<u8>> {
        let mut file = File::open(path)?;
        let length = file.metadata()?.len();
        file.seek(SeekFrom::Start(length.saturating_sub(TAIL_BYTES as u64)))?;
        let mut bytes = Vec::with_capacity(TAIL_BYTES);
        file.take(TAIL_BYTES as u64).read_to_end(&mut bytes)?;
        Ok(bytes)
    };
    let bytes = read().unwrap_or_default();
    let skip = bytes
        .iter()
        .take(3)
        .take_while(|byte| (0x80..0xC0).contains(*byte))
        .count();
    String::from_utf8_lossy(&bytes[skip..]).into_owned()
}

// ---------------------------------------------------------------------------
// Observed processes

/// A process this one did not necessarily start, held by handle (Windows) or
/// pidfd (Linux), so its pid cannot be recycled under the observer.
pub struct ObservedProcess {
    pub pid: u32,
    #[cfg(windows)]
    handle: std::os::windows::io::OwnedHandle,
    #[cfg(target_os = "linux")]
    pidfd: std::os::fd::OwnedFd,
}

impl ObservedProcess {
    pub fn open(pid: u32) -> Result<Self> {
        ensure!(pid != std::process::id() && pid != 0, "INVALID_PID: {pid}");
        #[cfg(windows)]
        {
            Self::open_with(pid, os::QUERY | os::SYNCHRONIZE)
        }
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::FromRawFd;
            let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
            ensure!(
                fd >= 0,
                "PROCESS_UNAVAILABLE: pid {pid}: {}",
                io::Error::last_os_error()
            );
            Ok(Self {
                pid,
                // SAFETY: a fresh descriptor that nothing else owns.
                pidfd: unsafe { std::os::fd::OwnedFd::from_raw_fd(fd as i32) },
            })
        }
        #[cfg(not(any(windows, target_os = "linux")))]
        Ok(Self { pid })
    }

    #[cfg(windows)]
    fn open_with(pid: u32, access: u32) -> Result<Self> {
        use std::os::windows::io::FromRawHandle;
        ensure!(pid != 0, "INVALID_PID: {pid}");
        let handle = unsafe { os::OpenProcess(access, 0, pid) };
        ensure!(
            !handle.is_null(),
            "PROCESS_UNAVAILABLE: pid {pid}: {}",
            io::Error::last_os_error()
        );
        Ok(Self {
            pid,
            // SAFETY: a fresh process handle that nothing else owns.
            handle: unsafe { std::os::windows::io::OwnedHandle::from_raw_handle(handle) },
        })
    }

    pub fn exited(&self) -> Result<bool> {
        self.wait(Duration::ZERO)
    }

    /// Waits ≤ `timeout` for the process to exit; whether it has.
    pub fn wait(&self, timeout: Duration) -> Result<bool> {
        #[cfg(windows)]
        {
            use std::os::windows::io::AsRawHandle;
            let ms = u32::try_from(timeout.as_millis()).unwrap_or(u32::MAX - 1);
            let result = unsafe { os::WaitForSingleObject(self.handle.as_raw_handle(), ms) };
            ensure!(
                result == os::WAIT_OBJECT_0 || result == os::WAIT_TIMEOUT,
                "PROCESS_WAIT_FAILED: {}",
                io::Error::last_os_error()
            );
            Ok(result == os::WAIT_OBJECT_0)
        }
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::AsRawFd;
            let end = Instant::now() + timeout;
            loop {
                let left = end.saturating_duration_since(Instant::now());
                let ms = i32::try_from(left.as_millis()).unwrap_or(i32::MAX);
                let mut descriptor = libc::pollfd {
                    fd: self.pidfd.as_raw_fd(),
                    events: libc::POLLIN,
                    revents: 0,
                };
                if unsafe { libc::poll(&mut descriptor, 1, ms) } < 0 {
                    let error = io::Error::last_os_error();
                    if error.kind() == io::ErrorKind::Interrupted {
                        continue;
                    }
                    anyhow::bail!("PROCESS_WAIT_FAILED: {error}");
                }
                return Ok(descriptor.revents & libc::POLLIN != 0);
            }
        }
        #[cfg(all(unix, not(target_os = "linux")))]
        {
            let end = Instant::now() + timeout;
            loop {
                let gone = unsafe { libc::kill(self.pid as i32, 0) } != 0
                    && io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH);
                if gone || Instant::now() >= end {
                    return Ok(gone);
                }
                thread::sleep(Duration::from_millis(20));
            }
        }
        #[cfg(not(any(windows, unix)))]
        {
            let _ = timeout;
            anyhow::bail!("PROCESS_WAIT_UNSUPPORTED")
        }
    }

    /// Ok unless the exited process reported a non-zero exit code (only
    /// Windows reports one for a process that is not our child).
    pub fn success(&self) -> Result<()> {
        if let Some(code) = self.exit_code() {
            ensure!(code == 0, "PROCESS_EXIT_FAILED: exit code {code}");
        }
        Ok(())
    }

    fn exit_code(&self) -> Option<i32> {
        #[cfg(windows)]
        {
            use std::os::windows::io::AsRawHandle;
            let mut code = 0u32;
            let ok = unsafe { os::GetExitCodeProcess(self.handle.as_raw_handle(), &mut code) };
            (ok != 0 && code != os::STILL_ACTIVE).then_some(code as i32)
        }
        #[cfg(not(windows))]
        None
    }

    /// The identity of this very process: read, then confirmed still alive,
    /// so a recycled pid's identity is never reported.
    fn identity(&self) -> Result<(u64, PathBuf)> {
        #[cfg(any(windows, target_os = "linux"))]
        {
            #[cfg(windows)]
            let identity = os::identity(&self.handle)?;
            #[cfg(target_os = "linux")]
            let identity = os::identity(self.pid)?;
            ensure!(!self.exited()?, "PROCESS_GONE: pid {}", self.pid);
            Ok(identity)
        }
        #[cfg(not(any(windows, target_os = "linux")))]
        {
            anyhow::bail!("PROCESS_IDENTITY_UNSUPPORTED: pid {}", self.pid)
        }
    }

    #[cfg(windows)]
    fn kill(&self) -> Result<()> {
        use std::os::windows::io::AsRawHandle;
        // Exit code 1, as `Child::kill`.
        let ok = unsafe { os::TerminateProcess(self.handle.as_raw_handle(), 1) };
        ensure!(ok != 0, "TerminateProcess: {}", io::Error::last_os_error());
        Ok(())
    }
}

#[cfg(windows)]
mod os {
    use anyhow::{Result, ensure};
    use std::{
        ffi::{OsString, c_void},
        io,
        os::windows::{
            ffi::OsStringExt,
            io::{AsRawHandle, OwnedHandle},
        },
        path::PathBuf,
    };

    pub const TERMINATE: u32 = 0x0001;
    /// `PROCESS_QUERY_LIMITED_INFORMATION`.
    pub const QUERY: u32 = 0x1000;
    pub const SYNCHRONIZE: u32 = 0x0010_0000;
    pub const WAIT_OBJECT_0: u32 = 0;
    pub const WAIT_TIMEOUT: u32 = 0x102;
    pub const STILL_ACTIVE: u32 = 259;
    /// 100 ns intervals from 1601-01-01 to 1970-01-01, in ms.
    const EPOCH_DIFFERENCE_MS: u64 = 11_644_473_600_000;

    #[repr(C)]
    #[derive(Default, Clone, Copy)]
    pub struct FileTime {
        low: u32,
        high: u32,
    }

    #[link(name = "kernel32")]
    unsafe extern "system" {
        pub fn OpenProcess(access: u32, inherit: i32, pid: u32) -> *mut c_void;
        pub fn WaitForSingleObject(handle: *mut c_void, milliseconds: u32) -> u32;
        pub fn GetExitCodeProcess(process: *mut c_void, code: *mut u32) -> i32;
        pub fn TerminateProcess(process: *mut c_void, code: u32) -> i32;
        fn GetProcessTimes(
            process: *mut c_void,
            creation: *mut FileTime,
            exit: *mut FileTime,
            kernel: *mut FileTime,
            user: *mut FileTime,
        ) -> i32;
        fn QueryFullProcessImageNameW(
            process: *mut c_void,
            flags: u32,
            name: *mut u16,
            size: *mut u32,
        ) -> i32;
    }

    pub fn identity(handle: &OwnedHandle) -> Result<(u64, PathBuf)> {
        let mut creation = FileTime::default();
        let (mut exit, mut kernel, mut user) = (creation, creation, creation);
        let ok = unsafe {
            GetProcessTimes(
                handle.as_raw_handle(),
                &mut creation,
                &mut exit,
                &mut kernel,
                &mut user,
            )
        };
        ensure!(ok != 0, "GetProcessTimes: {}", io::Error::last_os_error());
        let filetime = (u64::from(creation.high) << 32) | u64::from(creation.low);
        let start_ms = (filetime / 10_000).saturating_sub(EPOCH_DIFFERENCE_MS);
        let mut buffer = vec![0u16; 32 * 1024];
        let mut size = buffer.len() as u32;
        let ok = unsafe {
            QueryFullProcessImageNameW(handle.as_raw_handle(), 0, buffer.as_mut_ptr(), &mut size)
        };
        ensure!(
            ok != 0,
            "QueryFullProcessImageNameW: {}",
            io::Error::last_os_error()
        );
        let image = PathBuf::from(OsString::from_wide(&buffer[..size as usize]));
        Ok((start_ms, image))
    }
}

#[cfg(target_os = "linux")]
mod os {
    use anyhow::{Context, Result, ensure};
    use std::{fs, path::PathBuf};

    pub fn identity(pid: u32) -> Result<(u64, PathBuf)> {
        let stat = fs::read_to_string(format!("/proc/{pid}/stat"))
            .with_context(|| format!("PROCESS_GONE: pid {pid}"))?;
        // Fields after the parenthesized command name start at field 3
        // (state); field 22 (starttime) is index 19.
        let fields: Vec<&str> = stat
            .rfind(')')
            .and_then(|at| stat.get(at + 1..))
            .context("malformed /proc/<pid>/stat")?
            .split_whitespace()
            .collect();
        ensure!(
            !matches!(fields.first(), Some(&("Z" | "X" | "x"))),
            "PROCESS_GONE: pid {pid} has exited"
        );
        let ticks: u64 = fields
            .get(19)
            .and_then(|field| field.parse().ok())
            .context("malformed /proc/<pid>/stat")?;
        let boot_s: u64 = fs::read_to_string("/proc/stat")?
            .lines()
            .find_map(|line| line.strip_prefix("btime "))
            .and_then(|value| value.trim().parse().ok())
            .context("no btime in /proc/stat")?;
        let hz = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
        ensure!(hz > 0, "sysconf(_SC_CLK_TCK) failed");
        let start_ms = boot_s * 1000 + ticks * 1000 / hz as u64;
        let image = fs::read_link(format!("/proc/{pid}/exe"))
            .with_context(|| format!("PROCESS_GONE: pid {pid}: no image"))?;
        // A binary replaced while running reads as "PATH (deleted)".
        let image = match image.to_str().and_then(|s| s.strip_suffix(" (deleted)")) {
            Some(original) => PathBuf::from(original),
            None => image,
        };
        Ok((start_ms, image))
    }
}
