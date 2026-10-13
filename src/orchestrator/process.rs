//! Owned by WP1.3.
//!
//! Worker process launcher and adoption (REVAMP_PLAN §3.3, §14 WP1.3).
//!
//! * [`RealLauncher`] implements [`WorkerLauncher`]: it spawns
//!   `EXE daemon --state S --supervised` ([`RealLauncher::command`]) with
//!   stdin null, stdout piped and stderr appended to `worker.log`, and runs two
//!   helper threads per child:
//!   - the **stdout reader** copies every line to `worker.log`, parses it with
//!     [`parse_line`] and pushes [`ProcessEvent::Line`] to the sink;
//!   - the **exit waiter** owns the [`Child`] and blocks in the OS wait, so an
//!     exit is reported immediately (§2.2 rule 3: signals, not sampling). It
//!     then lets the reader drain (≤ [`LogPolicy::drain_wait`]) and sends
//!     [`ProcessEvent::Exited`] with the last [`FatalLine`] and the last 4 KiB
//!     of `worker.log`. `Exited` is always the last event of a process.
//! * [`RealLauncher::adopt`] re-attaches to a worker started by an earlier
//!   daemon: it returns `None` unless the pid is alive and its OS start time
//!   and image path equal `worker.json` (invariant 23). An adopted process has a
//!   liveness probe thread (Windows: a wait on the process handle with a 1 s
//!   timeout; Unix: a 1 Hz identity probe) that emits `Exited`.
//! * [`process_identity`] reads `(process_start, image)` from the OS:
//!   Windows `GetProcessTimes` creation FILETIME and
//!   `QueryFullProcessImageNameW`; Linux `/proc/<pid>/stat` field 22 and
//!   `/proc/<pid>/exe`. These are the values `worker.json` records (§6.1).
//! * [`kill_verified`] kills a pid only if its identity still matches.
//!
//! Workers are never tied to the daemon's lifetime: no Job Object, no
//! `PDEATHSIG`, and on Unix the worker gets its own process group so a
//! terminal Ctrl-C aimed at the daemon does not reach it. Dropping a
//! [`WorkerProcess`] never kills anything.
//!
//! WP4.2 adds `worker.log` rotation at spawn.
use super::worker_client::JsonWorkerClient;
use crate::{
    model::{
        errors::{ErrorWire, UNKNOWN_CODE},
        ipc::{FatalLine, StartupLine, WorkerRecord, parse_line},
        orchestrator::{
            LaunchSpec, ProcessEvent, ProcessSink, WorkerClient, WorkerEndpoint, WorkerLauncher,
            WorkerProcess,
        },
    },
    util::lock::lock,
};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    process::{Child, ChildStdout, Command, Stdio},
    sync::{Arc, Mutex, mpsc},
    thread,
    time::Duration,
};

/// The worker subcommand and its flags (`tkfs daemon --state S --supervised`).
pub const DAEMON_SUBCOMMAND: &str = "daemon";
pub const STATE_FLAG: &str = "--state";
pub const SUPERVISED_FLAG: &str = "--supervised";

/// Windows `CREATE_NO_WINDOW`: the worker never gets a console.
#[cfg(windows)]
const CREATE_NO_WINDOW: u32 = 0x0800_0000;

/// Adopted processes are probed this often (Unix), or their handle is waited
/// on with this timeout (Windows).
const PROBE_INTERVAL: Duration = crate::model::orchestrator::limits::ADOPTED_PID_PROBE;
/// After a `kill()` of an adopted process, the Unix probe checks this often…
#[cfg(not(windows))]
const PROBE_FAST_INTERVAL: Duration = Duration::from_millis(25);
/// …for this long, so the `Exited` event follows the kill promptly.
#[cfg(not(windows))]
const PROBE_FAST_WINDOW: Duration = Duration::from_secs(2);

/// How the launcher treats `worker.log` (the `log_dir_policy` of §14 WP1.3).
///
/// The log path itself comes from [`LaunchSpec::log`]; WP4.2 adds rotation at
/// spawn here.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogPolicy {
    /// `ProcessEvent::Exited.stderr_tail` carries at most this many bytes from
    /// the end of `worker.log` (4 KiB).
    pub tail_bytes: usize,
    /// After the child exits, the waiter waits this long for the stdout reader
    /// to reach end of file, so a final `FatalLine` is not lost. A grandchild
    /// that inherited the pipe can delay `Exited` by at most this much.
    pub drain_wait: Duration,
    /// A stdout "line" longer than this is split (it is still logged).
    pub max_line: usize,
}

impl Default for LogPolicy {
    fn default() -> Self {
        Self {
            tail_bytes: 4096,
            drain_wait: Duration::from_secs(2),
            max_line: 1024 * 1024,
        }
    }
}

/// The real [`WorkerLauncher`]: OS processes, `worker.log`, verified adoption.
#[derive(Debug, Default)]
pub struct RealLauncher {
    policy: LogPolicy,
}

impl RealLauncher {
    pub fn new(policy: LogPolicy) -> Arc<Self> {
        Arc::new(Self { policy })
    }

    pub fn policy(&self) -> &LogPolicy {
        &self.policy
    }

    /// The worker command line for `spec`:
    /// `EXE daemon --state DIR [--supervised]`, plus `spec.env`. An empty
    /// `spec.executable` means the current executable.
    pub fn command(spec: &LaunchSpec) -> Command {
        let executable = if spec.executable.as_os_str().is_empty() {
            std::env::current_exe().unwrap_or_else(|_| PathBuf::from("tkfs"))
        } else {
            spec.executable.clone()
        };
        let mut command = Command::new(executable);
        command
            .arg(DAEMON_SUBCOMMAND)
            .arg(STATE_FLAG)
            .arg(&spec.directory);
        if spec.supervised {
            command.arg(SUPERVISED_FLAG);
        }
        for (key, value) in &spec.env {
            command.env(key, value);
        }
        command
    }

    /// Spawns `command` as a worker process: stdin null, stdout parsed and
    /// copied to `log`, stderr appended to `log`; `CREATE_NO_WINDOW` on
    /// Windows, a new process group on Unix. `label` names the helper threads.
    /// [`WorkerLauncher::launch`] is `launch_command(command(spec), spec.log)`.
    pub fn launch_command(
        &self,
        mut command: Command,
        log: &Path,
        label: &str,
        events: ProcessSink,
    ) -> Result<Box<dyn WorkerProcess>, ErrorWire> {
        let program = command.get_program().to_string_lossy().into_owned();
        if let Some(parent) = log.parent().filter(|p| !p.as_os_str().is_empty()) {
            fs::create_dir_all(parent).map_err(|e| start_error(&program, "create log dir", &e))?;
        }
        let log_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(log)
            .map_err(|e| start_error(&program, "open worker.log", &e))?;
        let stderr = log_file
            .try_clone()
            .map_err(|e| start_error(&program, "open worker.log", &e))?;
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
            use std::os::unix::process::CommandExt;
            command.process_group(0);
        }
        let mut child = command
            .spawn()
            .map_err(|e| start_error(&program, "spawn", &e))?;
        // The command keeps a copy of the stderr handle; close it now.
        drop(command);
        let pid = child.id();
        let control = match ChildControl::new(pid) {
            Ok(control) => Arc::new(control),
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(start_error(&program, "open process handle", &error));
            }
        };
        let stdout = child.stdout.take().expect("worker stdout is piped");
        let emitter = Arc::new(Emitter::new(events));
        let (drained, drained_rx) = mpsc::channel::<()>();
        let reader = {
            let emitter = emitter.clone();
            let max_line = self.policy.max_line;
            thread::Builder::new()
                .name(format!("worker-out-{label}"))
                .spawn(move || read_stdout(stdout, log_file, max_line, &emitter, drained))
        };
        if let Err(error) = reader {
            let _ = child.kill();
            let _ = child.wait();
            return Err(start_error(&program, "start stdout reader", &error));
        }
        let waiter = {
            let control = control.clone();
            let policy = self.policy.clone();
            let log = log.to_path_buf();
            thread::Builder::new()
                .name(format!("worker-exit-{label}"))
                .spawn(move || {
                    let code = wait_child(child, &control);
                    let _ = drained_rx.recv_timeout(policy.drain_wait);
                    let tail = log_tail(&log, policy.tail_bytes);
                    emitter.exit(code, tail);
                })
        };
        if let Err(error) = waiter {
            // The closure (and with it the `Child`) is gone; kill by handle.
            let _ = control.kill();
            return Err(start_error(&program, "start exit waiter", &error));
        }
        Ok(Box::new(SpawnedProcess { pid, control }))
    }
}

impl WorkerLauncher for RealLauncher {
    fn launch(
        &self,
        spec: &LaunchSpec,
        events: ProcessSink,
    ) -> Result<Box<dyn WorkerProcess>, ErrorWire> {
        self.launch_command(Self::command(spec), &spec.log, &spec.state, events)
    }

    fn adopt(
        &self,
        record: &WorkerRecord,
        events: ProcessSink,
    ) -> Result<Option<Box<dyn WorkerProcess>>, ErrorWire> {
        adopt_record(record, events)
    }

    fn client(&self, endpoint: &WorkerEndpoint) -> Arc<dyn WorkerClient> {
        JsonWorkerClient::new(endpoint.clone())
    }
}

// ---------------------------------------------------------------------------
// Identity

/// `(process_start, image)` of a live process, or `None` if it does not exist,
/// has exited (including Unix zombies) or cannot be inspected.
///
/// `process_start` is the Windows `GetProcessTimes` creation FILETIME
/// (`high << 32 | low`) or the Linux `/proc/<pid>/stat` field 22 (start time in
/// clock ticks since boot): the value `worker.json` records. On Linux a
/// ` (deleted)` suffix (the binary was replaced while running) is removed from
/// the image path.
pub fn process_identity(pid: u32) -> Option<(u64, PathBuf)> {
    match os::probe(pid) {
        Probe::Alive { start, image } => Some((start, image)),
        Probe::Gone | Probe::Unknown => None,
    }
}

/// Whether `pid` is alive with exactly this start time and executable.
pub fn identity_matches(pid: u32, process_start: u64, executable: &Path) -> bool {
    process_identity(pid)
        .is_some_and(|(start, image)| start == process_start && same_executable(&image, executable))
}

/// Kills `pid` only if it is alive with exactly this start time and
/// executable (invariant 23; §1.2 I8).
///
/// * `Ok(())`: the process was terminated now, or it had already exited.
/// * `Err(WORKER_NOT_RUNNING)`: a live process has this pid but a different
///   identity (or cannot be inspected); nothing was killed.
/// * `Err(PERMISSION_DENIED)`: the identity matches but the OS refused.
///
/// On Windows the identity is verified on the same handle that is terminated,
/// so there is no race. On Unix a pid could in theory be reused between the
/// check and `kill(SIGKILL)`; that needs the recorded process to exit and its
/// pid to be recycled within microseconds.
pub fn kill_verified(pid: u32, process_start: u64, executable: &Path) -> Result<(), ErrorWire> {
    os::kill_verified(pid, process_start, executable)
}

/// What the OS says about a pid.
#[derive(Debug)]
enum Probe {
    /// No such process, or it has exited (Unix zombies included).
    Gone,
    Alive {
        start: u64,
        image: PathBuf,
    },
    /// It exists but cannot be inspected (another user, a protected process).
    Unknown,
}

/// Whether two image paths name the same executable: equal, equal after
/// Windows normalization (case, `\\?\` prefix, separators), or equal after
/// canonicalization (symlinks, 8.3 names).
fn same_executable(a: &Path, b: &Path) -> bool {
    if a == b {
        return true;
    }
    #[cfg(windows)]
    {
        let normalize = |path: &Path| {
            let text = path.to_string_lossy().replace('/', "\\");
            let text = text.strip_prefix(r"\\?\").unwrap_or(&text);
            text.to_lowercase()
        };
        if normalize(a) == normalize(b) {
            return true;
        }
    }
    matches!(
        (fs::canonicalize(a), fs::canonicalize(b)),
        (Ok(x), Ok(y)) if x == y
    )
}

fn identity_mismatch(pid: u32) -> ErrorWire {
    ErrorWire::coded(
        "WORKER_NOT_RUNNING",
        format!(
            "pid {pid} is not the recorded worker (its start time or image differs, \
             or it cannot be inspected); nothing was killed"
        ),
    )
}

fn kill_failed(pid: u32, error: &io::Error) -> ErrorWire {
    if error.kind() == io::ErrorKind::PermissionDenied {
        ErrorWire::coded(
            "PERMISSION_DENIED",
            format!("cannot kill pid {pid}: {error}"),
        )
    } else {
        ErrorWire::coded(UNKNOWN_CODE, format!("cannot kill pid {pid}: {error}"))
    }
}

fn start_error(program: &str, step: &str, error: &io::Error) -> ErrorWire {
    ErrorWire::coded(
        "WORKER_START",
        format!("cannot launch worker {program}: {step}: {error}"),
    )
}

// ---------------------------------------------------------------------------
// Spawned children

/// Serializes `ProcessEvent`s of one child: lines in stdout order, then exactly
/// one `Exited`, then nothing. The sink runs under this lock, so it must not
/// block (it is a controller mailbox push).
struct Emitter {
    sink: ProcessSink,
    state: Mutex<EmitState>,
}

#[derive(Default)]
struct EmitState {
    closed: bool,
    last_fatal: Option<FatalLine>,
}

impl Emitter {
    fn new(sink: ProcessSink) -> Self {
        Self {
            sink,
            state: Mutex::new(EmitState::default()),
        }
    }

    fn line(&self, line: StartupLine) {
        let mut state = lock(&self.state);
        if state.closed {
            return;
        }
        if let StartupLine::Fatal(fatal) = &line {
            state.last_fatal = Some(fatal.clone());
        }
        deliver(&self.sink, ProcessEvent::Line(line));
    }

    fn exit(&self, code: Option<i32>, stderr_tail: String) {
        let mut state = lock(&self.state);
        if state.closed {
            return;
        }
        state.closed = true;
        let fatal = state.last_fatal.take();
        deliver(
            &self.sink,
            ProcessEvent::Exited {
                code,
                fatal,
                stderr_tail,
            },
        );
    }
}

/// Calls the sink; a panicking sink must not take a helper thread down (the
/// reader must keep draining the pipe and the waiter must report the exit).
fn deliver(sink: &ProcessSink, event: ProcessEvent) {
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| sink(event)));
}

fn read_stdout(
    stdout: ChildStdout,
    mut log: File,
    max_line: usize,
    emitter: &Emitter,
    drained: mpsc::Sender<()>,
) {
    let mut reader = BufReader::new(stdout);
    let mut buffer = Vec::new();
    loop {
        buffer.clear();
        match reader
            .by_ref()
            .take(max_line.max(1) as u64)
            .read_until(b'\n', &mut buffer)
        {
            Ok(0) => break,
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(_) => break,
        }
        let mut end = buffer.len();
        while end > 0 && matches!(buffer[end - 1], b'\n' | b'\r') {
            end -= 1;
        }
        let text = String::from_utf8_lossy(&buffer[..end]);
        // Every line reaches worker.log, parsed or not; a failing log write
        // (disk full) must not stop the pipe from draining.
        let _ = writeln!(log, "{text}");
        if let Some(line) = parse_line(&text) {
            emitter.line(line);
        }
    }
    let _ = drained.send(());
}

/// The last `max` bytes of a log file, decoded lossily, starting at a
/// character boundary (empty if the file cannot be read). This is the
/// `stderr_tail` of a spawned worker's `Exited`; a controller can use it to
/// fill the tail for an adopted worker, whose log the launcher does not know.
pub fn log_tail(path: &Path, max: usize) -> String {
    let Ok(mut file) = File::open(path) else {
        return String::new();
    };
    let Ok(length) = file.metadata().map(|m| m.len()) else {
        return String::new();
    };
    let start = length.saturating_sub(max as u64);
    if file.seek(SeekFrom::Start(start)).is_err() {
        return String::new();
    }
    let mut bytes = Vec::with_capacity(max);
    if file.take(max as u64).read_to_end(&mut bytes).is_err() {
        return String::new();
    }
    let skip = bytes
        .iter()
        .take(3)
        .take_while(|b| (0x80..0xC0).contains(*b))
        .count();
    String::from_utf8_lossy(&bytes[skip..]).into_owned()
}

/// Kill control for a child the exit waiter owns.
struct ChildControl {
    pid: u32,
    /// Set by the waiter once the child has exited (before it is reaped on
    /// Unix), so `kill` never signals a pid that could have been reused.
    exited: Mutex<bool>,
    /// A handle of our own (the waiter's `Child` owns another one); it keeps
    /// the process object, and so the pid, alive until we drop it.
    #[cfg(windows)]
    handle: std::os::windows::io::OwnedHandle,
}

impl ChildControl {
    fn new(pid: u32) -> io::Result<Self> {
        Ok(Self {
            pid,
            exited: Mutex::new(false),
            #[cfg(windows)]
            handle: os::open(pid, os::QUERY | os::SYNCHRONIZE | os::TERMINATE)?,
        })
    }

    fn kill(&self) -> Result<(), ErrorWire> {
        let exited = lock(&self.exited);
        if *exited {
            return Ok(());
        }
        #[cfg(windows)]
        {
            os::terminate(&self.handle).map_err(|error| kill_failed(self.pid, &error))?;
        }
        #[cfg(unix)]
        {
            // Not reaped yet (the waiter sets `exited` before reaping), so the
            // pid is still this child, even if it is already a zombie.
            if unsafe { libc::kill(self.pid as libc::pid_t, libc::SIGKILL) } != 0 {
                let error = io::Error::last_os_error();
                if error.raw_os_error() != Some(libc::ESRCH) {
                    return Err(kill_failed(self.pid, &error));
                }
            }
        }
        drop(exited);
        Ok(())
    }
}

/// Blocks until the child exits; returns its exit code (`None` when killed by
/// a signal on Unix).
fn wait_child(mut child: Child, control: &ChildControl) -> Option<i32> {
    #[cfg(unix)]
    {
        // Wait without reaping, mark the child exited, then reap: `kill` checks
        // the mark under the same lock, so it never signals a recycled pid.
        loop {
            let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
            let rc = unsafe {
                libc::waitid(
                    libc::P_PID,
                    control.pid as libc::id_t,
                    &mut info,
                    libc::WEXITED | libc::WNOWAIT,
                )
            };
            if rc == 0 || io::Error::last_os_error().kind() != io::ErrorKind::Interrupted {
                break;
            }
        }
        *lock(&control.exited) = true;
        child.wait().ok().and_then(|status| status.code())
    }
    #[cfg(not(unix))]
    {
        let code = child.wait().ok().and_then(|status| status.code());
        *lock(&control.exited) = true;
        code
    }
}

struct SpawnedProcess {
    pid: u32,
    control: Arc<ChildControl>,
}

impl WorkerProcess for SpawnedProcess {
    fn pid(&self) -> u32 {
        self.pid
    }

    fn kill(&self) -> Result<(), ErrorWire> {
        self.control.kill()
    }

    fn adopted(&self) -> bool {
        false
    }
}

// ---------------------------------------------------------------------------
// Adoption

fn adopt_record(
    record: &WorkerRecord,
    events: ProcessSink,
) -> Result<Option<Box<dyn WorkerProcess>>, ErrorWire> {
    let pid = record.pid;
    if !identity_matches(pid, record.process_start, &record.executable) {
        return Ok(None);
    }
    let (wake, wake_rx) = mpsc::channel::<()>();
    #[cfg(windows)]
    let watched = {
        // Keep a handle for the probe. Re-verify on it: the pid could have been
        // recycled between the check above and this open.
        let handle = match os::open(pid, os::QUERY | os::SYNCHRONIZE) {
            Ok(handle) => handle,
            Err(_) if process_identity(pid).is_none() => return Ok(None),
            Err(error) => {
                return Err(ErrorWire::coded(
                    "PERMISSION_DENIED",
                    format!("cannot watch adopted worker pid {pid}: {error}"),
                ));
            }
        };
        let same = !os::exited(&handle)
            && os::identity(&handle).is_some_and(|(start, image)| {
                start == record.process_start && same_executable(&image, &record.executable)
            });
        if !same {
            return Ok(None);
        }
        handle
    };
    let probe = {
        let start = record.process_start;
        let executable = record.executable.clone();
        thread::Builder::new()
            .name(format!("worker-probe-{pid}"))
            .spawn(move || {
                #[cfg(windows)]
                probe_adopted(watched, pid, start, &executable, &events, wake_rx);
                #[cfg(not(windows))]
                probe_adopted(pid, start, &executable, &events, wake_rx);
            })
    };
    probe.map_err(|error| {
        ErrorWire::coded(
            UNKNOWN_CODE,
            format!("cannot start the liveness probe of pid {pid}: {error}"),
        )
    })?;
    Ok(Some(Box::new(AdoptedProcess {
        pid,
        process_start: record.process_start,
        executable: record.executable.clone(),
        wake,
    })))
}

/// Emits `Exited` when the adopted process exits. Ends without an event when
/// the [`AdoptedProcess`] is dropped.
#[cfg(windows)]
fn probe_adopted(
    handle: std::os::windows::io::OwnedHandle,
    pid: u32,
    start: u64,
    executable: &Path,
    events: &ProcessSink,
    wake: mpsc::Receiver<()>,
) {
    loop {
        match os::wait(&handle, PROBE_INTERVAL) {
            os::Wait::Exited => {
                let code = os::exit_code(&handle).map(|code| code as i32);
                report_exit(events, code);
                return;
            }
            os::Wait::Running => {}
            os::Wait::Failed => {
                if !identity_matches(pid, start, executable) {
                    report_exit(events, None);
                    return;
                }
                thread::sleep(PROBE_INTERVAL);
            }
        }
        if matches!(wake.try_recv(), Err(mpsc::TryRecvError::Disconnected)) {
            return;
        }
    }
}

/// Emits `Exited` when the adopted process exits (1 Hz identity probe; faster
/// for a while after a kill). Ends without an event when the
/// [`AdoptedProcess`] is dropped.
#[cfg(not(windows))]
fn probe_adopted(
    pid: u32,
    start: u64,
    executable: &Path,
    events: &ProcessSink,
    wake: mpsc::Receiver<()>,
) {
    use std::{sync::mpsc::RecvTimeoutError, time::Instant};
    let mut fast_until: Option<Instant> = None;
    loop {
        let interval = if fast_until.is_some_and(|until| Instant::now() < until) {
            PROBE_FAST_INTERVAL
        } else {
            PROBE_INTERVAL
        };
        match wake.recv_timeout(interval) {
            Ok(()) => fast_until = Some(Instant::now() + PROBE_FAST_WINDOW),
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => return,
        }
        if !identity_matches(pid, start, executable) {
            report_exit(events, None);
            return;
        }
    }
}

fn report_exit(events: &ProcessSink, code: Option<i32>) {
    // The adopting daemon never saw this worker's stdout, and `worker.json`
    // does not name its log, so the tail is empty here; the controller knows
    // `STATE/worker.log` and can fill it with [`log_tail`].
    deliver(
        events,
        ProcessEvent::Exited {
            code,
            fatal: None,
            stderr_tail: String::new(),
        },
    );
}

struct AdoptedProcess {
    pid: u32,
    process_start: u64,
    executable: PathBuf,
    /// Wakes the probe after a kill; dropping it ends the probe.
    wake: mpsc::Sender<()>,
}

impl WorkerProcess for AdoptedProcess {
    fn pid(&self) -> u32 {
        self.pid
    }

    fn kill(&self) -> Result<(), ErrorWire> {
        let result = kill_verified(self.pid, self.process_start, &self.executable);
        let _ = self.wake.send(());
        result
    }

    fn adopted(&self) -> bool {
        true
    }
}

// ---------------------------------------------------------------------------
// OS specifics

#[cfg(windows)]
mod os {
    use super::{Probe, identity_mismatch, kill_failed, same_executable};
    use crate::model::errors::ErrorWire;
    use std::{
        ffi::{OsString, c_void},
        io,
        os::windows::{
            ffi::OsStringExt,
            io::{AsRawHandle, FromRawHandle, OwnedHandle},
        },
        path::{Path, PathBuf},
        time::Duration,
    };

    pub const TERMINATE: u32 = 0x0001;
    pub const QUERY: u32 = 0x1000; // PROCESS_QUERY_LIMITED_INFORMATION
    pub const SYNCHRONIZE: u32 = 0x0010_0000;
    const WAIT_OBJECT_0: u32 = 0;
    const WAIT_TIMEOUT: u32 = 0x102;
    const ERROR_INVALID_PARAMETER: i32 = 87;
    const STILL_ACTIVE: u32 = 259;
    /// Exit code of a worker killed by `TerminateProcess` (as `Child::kill`).
    const KILLED_EXIT_CODE: u32 = 1;

    #[repr(C)]
    #[derive(Default, Clone, Copy)]
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
        fn QueryFullProcessImageNameW(
            process: *mut c_void,
            flags: u32,
            name: *mut u16,
            size: *mut u32,
        ) -> i32;
        fn WaitForSingleObject(handle: *mut c_void, milliseconds: u32) -> u32;
        fn GetExitCodeProcess(process: *mut c_void, code: *mut u32) -> i32;
        fn TerminateProcess(process: *mut c_void, code: u32) -> i32;
    }

    pub fn open(pid: u32, access: u32) -> io::Result<OwnedHandle> {
        if pid == 0 {
            return Err(io::Error::from_raw_os_error(ERROR_INVALID_PARAMETER));
        }
        let handle = unsafe { OpenProcess(access, 0, pid) };
        if handle.is_null() {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: a fresh process handle that nothing else owns.
        Ok(unsafe { OwnedHandle::from_raw_handle(handle) })
    }

    pub enum Wait {
        Exited,
        Running,
        Failed,
    }

    pub fn wait(handle: &OwnedHandle, timeout: Duration) -> Wait {
        let ms = u32::try_from(timeout.as_millis()).unwrap_or(u32::MAX - 1);
        match unsafe { WaitForSingleObject(handle.as_raw_handle(), ms) } {
            WAIT_OBJECT_0 => Wait::Exited,
            WAIT_TIMEOUT => Wait::Running,
            _ => Wait::Failed,
        }
    }

    /// Whether the process has exited. Needs `SYNCHRONIZE`; without it, falls
    /// back to the exit code (a process that exited with 259 looks alive).
    pub fn exited(handle: &OwnedHandle) -> bool {
        match wait(handle, Duration::ZERO) {
            Wait::Exited => true,
            Wait::Running => false,
            Wait::Failed => exit_code(handle).is_some_and(|code| code != STILL_ACTIVE),
        }
    }

    pub fn exit_code(handle: &OwnedHandle) -> Option<u32> {
        let mut code = 0u32;
        (unsafe { GetExitCodeProcess(handle.as_raw_handle(), &mut code) } != 0).then_some(code)
    }

    pub fn identity(handle: &OwnedHandle) -> Option<(u64, PathBuf)> {
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
        if ok == 0 {
            return None;
        }
        let start = (u64::from(creation.high) << 32) | u64::from(creation.low);
        let mut buffer = vec![0u16; 32 * 1024];
        let mut size = buffer.len() as u32;
        let ok = unsafe {
            QueryFullProcessImageNameW(handle.as_raw_handle(), 0, buffer.as_mut_ptr(), &mut size)
        };
        if ok == 0 {
            return None;
        }
        let image = PathBuf::from(OsString::from_wide(&buffer[..size as usize]));
        Some((start, image))
    }

    pub fn probe(pid: u32) -> Probe {
        let handle = match open(pid, QUERY | SYNCHRONIZE).or_else(|_| open(pid, QUERY)) {
            Ok(handle) => handle,
            Err(error) if error.raw_os_error() == Some(ERROR_INVALID_PARAMETER) => {
                return Probe::Gone;
            }
            Err(_) => return Probe::Unknown,
        };
        if exited(&handle) {
            return Probe::Gone;
        }
        match identity(&handle) {
            Some((start, image)) => Probe::Alive { start, image },
            None => Probe::Unknown,
        }
    }

    pub fn terminate(handle: &OwnedHandle) -> io::Result<()> {
        if unsafe { TerminateProcess(handle.as_raw_handle(), KILLED_EXIT_CODE) } == 0 {
            let error = io::Error::last_os_error();
            // Terminating a process that already exited fails with access
            // denied; that is success for us.
            if !exited(handle) {
                return Err(error);
            }
        }
        Ok(())
    }

    pub fn kill_verified(pid: u32, start: u64, executable: &Path) -> Result<(), ErrorWire> {
        let handle = match open(pid, QUERY | SYNCHRONIZE | TERMINATE) {
            Ok(handle) => handle,
            Err(error) => {
                return match probe(pid) {
                    Probe::Gone => Ok(()),
                    Probe::Alive { start: s, image }
                        if s == start && same_executable(&image, executable) =>
                    {
                        Err(kill_failed(pid, &error))
                    }
                    _ => Err(identity_mismatch(pid)),
                };
            }
        };
        if exited(&handle) {
            return Ok(());
        }
        // Verified on the handle we terminate: the pid cannot be recycled
        // while we hold it.
        match identity(&handle) {
            Some((s, image)) if s == start && same_executable(&image, executable) => {}
            _ => return Err(identity_mismatch(pid)),
        }
        terminate(&handle).map_err(|error| kill_failed(pid, &error))
    }
}

#[cfg(target_os = "linux")]
mod os {
    use super::{Probe, identity_mismatch, kill_failed, same_executable};
    use crate::model::errors::ErrorWire;
    use std::{
        fs, io,
        path::{Path, PathBuf},
    };

    pub fn probe(pid: u32) -> Probe {
        let Ok(raw) = libc::pid_t::try_from(pid) else {
            return Probe::Gone;
        };
        if raw <= 0 {
            return Probe::Gone;
        }
        if unsafe { libc::kill(raw, 0) } != 0
            && io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
        {
            return Probe::Gone;
        }
        let stat = match fs::read_to_string(format!("/proc/{pid}/stat")) {
            Ok(stat) => stat,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Probe::Gone,
            Err(_) => return Probe::Unknown,
        };
        // Fields after the parenthesized command name start at field 3
        // (state); field 22 (starttime) is index 19.
        let Some(rest) = stat.rfind(')').and_then(|at| stat.get(at + 2..)) else {
            return Probe::Unknown;
        };
        let fields: Vec<&str> = rest.split_whitespace().collect();
        if matches!(fields.first(), Some(&("Z" | "X" | "x"))) {
            return Probe::Gone;
        }
        let Some(start) = fields.get(19).and_then(|field| field.parse::<u64>().ok()) else {
            return Probe::Unknown;
        };
        let image = match fs::read_link(format!("/proc/{pid}/exe")) {
            Ok(image) => image,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Probe::Gone,
            Err(_) => return Probe::Unknown,
        };
        let image = match image.to_str().and_then(|s| s.strip_suffix(" (deleted)")) {
            Some(original) => PathBuf::from(original),
            None => image,
        };
        Probe::Alive { start, image }
    }

    pub fn kill_verified(pid: u32, start: u64, executable: &Path) -> Result<(), ErrorWire> {
        match probe(pid) {
            Probe::Gone => Ok(()),
            Probe::Alive { start: s, image }
                if s == start && same_executable(&image, executable) =>
            {
                if unsafe { libc::kill(pid as libc::pid_t, libc::SIGKILL) } != 0 {
                    let error = io::Error::last_os_error();
                    if error.raw_os_error() != Some(libc::ESRCH) {
                        return Err(kill_failed(pid, &error));
                    }
                }
                Ok(())
            }
            _ => Err(identity_mismatch(pid)),
        }
    }
}

/// Other platforms cannot verify identities: nothing is adopted and nothing is
/// killed by pid.
#[cfg(not(any(windows, target_os = "linux")))]
mod os {
    use super::{Probe, identity_mismatch};
    use crate::model::errors::ErrorWire;
    use std::path::Path;

    pub fn probe(_pid: u32) -> Probe {
        Probe::Unknown
    }

    pub fn kill_verified(pid: u32, _start: u64, _executable: &Path) -> Result<(), ErrorWire> {
        Err(identity_mismatch(pid))
    }
}
