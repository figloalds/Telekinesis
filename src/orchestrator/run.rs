//! Owned by WP2.1 (WP1.12 in Wave 1, WP3.1 in Wave 3).
//!
//! The daemon's main loop (REVAMP_PLAN §2.5, §3.5, §3.6, §14 WP2.1 task 2),
//! with no global lock and no polling loop:
//!
//! * `control-accept` blocks in `accept` on the loopback control listener and
//!   gives each connection its own thread, whose upgrade must carry the
//!   daemon's control token (`X-Tkfs-Token`, WP4.1; CCR-WP4.1-3: required
//!   here until the control server stops, refused with HTTP 403 otherwise) (at most
//!   `limits::MAX_CONTROL_CONNECTIONS` outside a request, and as many more
//!   parked inside one, so requests waiting on a slow catalog never lock
//!   List, Inspect and Status out): a persistent `receive → manage →
//!   reply` loop. A complete frame that is not JSON, or JSON that is not a
//!   valid action, is answered (`INVALID_REQUEST`) and the connection stays
//!   usable; a transport failure (including a binary envelope the codec
//!   rejects) closes the connection, as before.
//! * the peer listener is served by the v3 inbound server (§3.8,
//!   `inbound::start`, WP3.1's cutover): its own `net-accept` thread, at most
//!   `limits::MAX_PREAUTH` handshakes and `limits::MAX_SESSIONS` sessions; a
//!   legacy (v2) dialer is answered `PEER_NEEDS_UPGRADE` (invariant 20). The
//!   server belongs to `ctx.network` ([`DaemonNetwork`](super::ctx::DaemonNetwork)),
//!   whose `shutdown` (§3.6 step 2) stops it with the outbound peer stack.
//! * `orchestrator.json` (`{format:3, pid, version, installation, control,
//!   network, started_at_ms}`) is written atomically and removed by a drop
//!   guard.
//! * The main thread waits on `ctx.shutdown`. The `Shutdown` action runs the
//!   sequence (`shutdown::run`) on its control thread, flushes the reply, then
//!   cancels `ctx.shutdown`; a signal cancels it directly and the main thread
//!   runs the sequence. Either way the main thread then stops accepting, lets
//!   requests in flight finish, closes the control connections and joins
//!   their threads (2 s), removes `orchestrator.json` (while the catalog
//!   still holds `orchestrator.lock`, so a successor's file is never
//!   touched), and closes the catalog.
//! * Stopping an accept loop wakes it with a connection of its own to the
//!   listener's port, on loopback when the listener is bound to every
//!   interface (`0.0.0.0`, `[::]`: Windows refuses a connect to the
//!   unspecified address), and waits for its thread a bounded time.
//!
//! On Windows the daemon first makes its own standard handles
//! non-inheritable (CCR-WP1.INT-1): a worker it spawns must not inherit the
//! pipe or file its parent gave the daemon (a reader waiting for the end of
//! that pipe would wait for every worker).
use super::{
    api::Action,
    config::Config,
    ctx::{self, Daemon},
    manage::manage,
    shutdown,
};
use crate::{
    model::orchestrator::{Ctx, NetworkApi, limits},
    util::{lock::Tracked, log::event, notify::Notify},
    ws,
};
use anyhow::{Result, anyhow};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs,
    io::Write,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, Shutdown, SocketAddr, TcpListener, TcpStream},
    panic::{AssertUnwindSafe, catch_unwind},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

/// `orchestrator.json` format written by this build.
pub const DISCOVERY_FORMAT: u32 = 3;
/// A persistent control connection idle this long is closed.
pub const CONTROL_IDLE: Duration = Duration::from_secs(300);
/// Control connection threads at most: `limits::MAX_CONTROL_CONNECTIONS`
/// connections outside a request, plus as many parked inside one
/// (`Connections::admits`).
pub const MAX_CONTROL_THREADS: usize = 2 * limits::MAX_CONTROL_CONNECTIONS;

/// Runs the daemon until a `Shutdown` action or a signal (§3.6).
pub fn run(config: Config) -> Result<()> {
    #[cfg(windows)]
    keep_inherited_handles_private();
    serve(ctx::build_with(
        config,
        ctx::BuildOptions {
            install_log: true,
            ..ctx::BuildOptions::default()
        },
    )?)
}

/// Serves a built daemon until it shuts down (the body of [`run`]), with
/// the signal handlers installed.
pub fn serve(daemon: Daemon) -> Result<()> {
    serve_with(daemon, true)
}

/// [`serve`]; `signals: false` leaves the process's signal handling alone
/// (tests run daemons in-process, and Ctrl-C must still end the test run).
pub fn serve_with(daemon: Daemon, signals: bool) -> Result<()> {
    let Daemon {
        ctx,
        control,
        mut control_token,
        network,
        net,
        catalog,
        ..
    } = daemon;
    if signals {
        shutdown::install(&ctx);
    }
    let endpoints = ctx.endpoints.load();
    let discovery = json!({
        "format": DISCOVERY_FORMAT,
        "pid": std::process::id(),
        "version": crate::worker::version(),
        "installation": ctx.installation,
        "control": endpoints.control,
        "network": endpoints.network,
        "started_at_ms": ctx.started_at_ms,
    });
    let file = DiscoveryFile::write(
        &super::api::discovery_path(&ctx.config.data_directory),
        &discovery,
    )?;
    // A closed stdout (a launcher that went away) must not end the daemon.
    let _ = writeln!(std::io::stdout(), "{discovery}");
    if let Some(listener) = network {
        // The v3 inbound server (§3.8); `net` stops it in the shutdown
        // sequence (§3.6 step 2).
        net.serve_inbound(&ctx, listener);
    }
    let controls = match ControlServer::start(ctx.clone(), control, control_token.token()) {
        Ok(controls) => controls,
        Err(error) => {
            // The inbound server and the peer stack run already: stop them
            // before giving up (WP3.INT; the WP3.1 review's minor), as the
            // shutdown sequence would.
            ctx.shutdown.cancel();
            net.shutdown(limits::SHUTDOWN_NETWORK);
            return Err(error);
        }
    };
    ctx.shutdown.wait();
    // The signal path runs the sequence here; after the Shutdown action this
    // returns the result it already has.
    let _ = shutdown::run(&ctx, limits::SHUTDOWN_FOLDERS);
    // The sequence stops the network in its step 2, but a sequence that
    // panicked before that step did not. Both stacks stop accepting by
    // themselves (their tokens are children of `ctx.shutdown`), yet only
    // this drains the inbound sessions and joins the peer supervisors, and
    // they hold the context, so dropping `net` would not: stop it here in
    // every case (a no-op once stopped), before the discovery file and the
    // catalog go.
    net.shutdown(limits::SHUTDOWN_NETWORK);
    drop(net);
    controls.stop(limits::SHUTDOWN_CONTROL_JOIN);
    control_token.release();
    // The file goes first, while `orchestrator.lock` is still held: a
    // successor cannot have written its own yet.
    drop(file);
    catalog.close_within(Duration::from_secs(1));
    shutdown::completed();
    Ok(())
}

// ---------------------------------------------------------------------------
// orchestrator.json

/// `orchestrator.json`, removed when dropped if it still holds what this
/// daemon wrote.
struct DiscoveryFile {
    path: PathBuf,
    written: Vec<u8>,
}

impl DiscoveryFile {
    /// Writes `value` atomically (a temporary file, then a rename).
    fn write(path: &Path, value: &Value) -> Result<Self> {
        let temporary = path.with_extension("json.tmp");
        let written = serde_json::to_vec_pretty(value)?;
        fs::write(&temporary, &written)?;
        fs::rename(&temporary, path)?;
        Ok(Self {
            path: path.to_owned(),
            written,
        })
    }
}

impl Drop for DiscoveryFile {
    fn drop(&mut self) {
        // Another daemon's file (it replaced ours) is left alone.
        if fs::read(&self.path).is_ok_and(|bytes| bytes == self.written) {
            let _ = fs::remove_file(&self.path);
        }
    }
}

// ---------------------------------------------------------------------------
// Accept loops

/// How long stopping an accept loop waits for its thread after the wake-up
/// connection; a thread still blocked then is left to the process exit.
const ACCEPT_JOIN: Duration = Duration::from_secs(2);

/// Where a wake-up connection for a listener bound to `address` goes: the
/// same address, except that an unspecified IP (`0.0.0.0`, `[::]`) becomes
/// the loopback of its family. Windows refuses a connect to the unspecified
/// address (`WSAEADDRNOTAVAIL`), and a listener on every interface also
/// listens on loopback.
fn wake_address(address: SocketAddr) -> SocketAddr {
    let ip = match address.ip() {
        IpAddr::V4(ip) if ip.is_unspecified() => IpAddr::V4(Ipv4Addr::LOCALHOST),
        IpAddr::V6(ip) if ip.is_unspecified() => IpAddr::V6(Ipv6Addr::LOCALHOST),
        ip => ip,
    };
    SocketAddr::new(ip, address.port())
}

/// Stops a blocking accept loop: sets its flag, wakes `accept` with a
/// connection of its own, and waits at most [`ACCEPT_JOIN`] for the thread.
/// Shutdown never depends on the wake-up: a thread that has not ended by
/// then (the connect failed) is logged and left blocked until the process
/// exits.
fn stop_accept(name: &str, stop: &AtomicBool, address: SocketAddr, accept: thread::JoinHandle<()>) {
    stop.store(true, Ordering::Release);
    let woken = TcpStream::connect_timeout(&wake_address(address), Duration::from_secs(1));
    let deadline = Instant::now() + ACCEPT_JOIN;
    while !accept.is_finished() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(5));
    }
    if accept.is_finished() {
        let _ = accept.join();
    } else {
        event!(warn, REQUEST_TIMEOUT, {
            "detail": "an accept loop did not stop; it is left to the process exit",
            "listener": name,
            "address": address.to_string(),
            "wake_error": woken.err().map(|error| error.to_string()),
        });
    }
}

/// Pause after an accept error (for example a full descriptor table), so a
/// persistent error cannot spin.
const ACCEPT_ERROR_PAUSE: Duration = Duration::from_millis(50);

/// One control connection: its socket (to close it at shutdown) and whether
/// it is handling a request.
struct Connection {
    killer: Option<TcpStream>,
    busy: Arc<AtomicBool>,
}

/// The control listener and its connections.
struct ControlServer {
    address: SocketAddr,
    stop: Arc<AtomicBool>,
    connections: Arc<Connections>,
    accept: Option<thread::JoinHandle<()>>,
}

struct Connections {
    next: AtomicU64,
    live: Tracked<BTreeMap<u64, Connection>>,
    ended: Notify,
}

impl Connections {
    fn new() -> Self {
        Self {
            next: AtomicU64::new(0),
            live: Tracked::new("control.connections", BTreeMap::new()),
            ended: Notify::new(),
        }
    }

    fn count(&self) -> usize {
        self.live.lock().len()
    }

    /// Whether one more connection may be served: at most
    /// `limits::MAX_CONTROL_CONNECTIONS` connections that are not inside a
    /// request (idle, or reading one), and at most [`MAX_CONTROL_THREADS`] in
    /// all. A connection parked in a request (a management write behind a
    /// slow catalog, a long Subscribe or Stop wait) thus never takes the
    /// place of a new List, Inspect or Status (§2.3: a catalog fault may stop
    /// writes, never status; post-integration re-review, WP2.INT).
    fn admits(&self) -> bool {
        let live = self.live.lock();
        let busy = live
            .values()
            .filter(|connection| connection.busy.load(Ordering::Acquire))
            .count();
        live.len() < MAX_CONTROL_THREADS && live.len() - busy < limits::MAX_CONTROL_CONNECTIONS
    }

    fn remove(&self, id: u64) {
        self.live.lock().remove(&id);
        self.ended.poke();
    }

    /// Waits until `done(&live)` holds or `deadline` passes.
    fn wait(&self, deadline: Instant, done: impl Fn(&BTreeMap<u64, Connection>) -> bool) -> bool {
        loop {
            let seen = self.ended.count();
            if done(&self.live.lock()) {
                return true;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return false;
            }
            // Busy flags change without a poke: re-check at least every 20 ms.
            self.ended
                .wait(seen, remaining.min(Duration::from_millis(20)));
        }
    }
}

impl ControlServer {
    fn start(ctx: Arc<Ctx>, listener: TcpListener, token: Arc<str>) -> Result<Self> {
        let address = listener.local_addr()?;
        let stop = Arc::new(AtomicBool::new(false));
        let connections = Arc::new(Connections::new());
        let accept = {
            let (stop, connections) = (stop.clone(), connections.clone());
            thread::Builder::new()
                .name("control-accept".into())
                .spawn(move || accept_control(ctx, listener, token, stop, connections))?
        };
        Ok(Self {
            address,
            stop,
            connections,
            accept: Some(accept),
        })
    }

    /// Stops accepting, lets requests in flight finish (half the budget),
    /// closes every connection and waits for their threads (the rest).
    fn stop(mut self, budget: Duration) {
        let deadline = Instant::now() + budget;
        if let Some(accept) = self.accept.take() {
            stop_accept("control", &self.stop, self.address, accept);
        }
        let settle = Instant::now() + budget / 2;
        self.connections.wait(settle, |live| {
            live.values().all(|c| !c.busy.load(Ordering::Acquire))
        });
        let killers: Vec<TcpStream> = self
            .connections
            .live
            .lock()
            .values_mut()
            .filter_map(|connection| connection.killer.take())
            .collect();
        for killer in killers {
            let _ = killer.shutdown(Shutdown::Both);
        }
        if !self.connections.wait(deadline, BTreeMap::is_empty) {
            event!(warn, REQUEST_TIMEOUT, {
                "detail": "control connections still open at the join deadline",
                "connections": self.connections.count(),
            });
        }
    }
}

fn accept_control(
    ctx: Arc<Ctx>,
    listener: TcpListener,
    token: Arc<str>,
    stop: Arc<AtomicBool>,
    connections: Arc<Connections>,
) {
    for stream in listener.incoming() {
        if stop.load(Ordering::Acquire) {
            return;
        }
        let stream = match stream {
            Ok(stream) => stream,
            Err(error) => {
                event!(warn, WS_CONNECT_FAILED, {
                    "detail": "control accept failed",
                    "error": error.to_string(),
                });
                thread::sleep(ACCEPT_ERROR_PAUSE);
                continue;
            }
        };
        if !connections.admits() {
            event!(warn, BUSY, {
                "detail": "control connection refused: too many open connections",
                "limit": limits::MAX_CONTROL_CONNECTIONS,
                "threads": MAX_CONTROL_THREADS,
            });
            continue;
        }
        let id = connections.next.fetch_add(1, Ordering::Relaxed);
        let busy = Arc::new(AtomicBool::new(false));
        connections.live.lock().insert(
            id,
            Connection {
                killer: stream.try_clone().ok(),
                busy: busy.clone(),
            },
        );
        let spawned = {
            let (ctx, connections, token) = (ctx.clone(), connections.clone(), token.clone());
            thread::Builder::new()
                .name(format!("control-conn-{id}"))
                .spawn(move || {
                    serve_control(&ctx, stream, &token, &busy);
                    connections.remove(id);
                })
        };
        if let Err(error) = spawned {
            connections.remove(id);
            event!(warn, BUSY, {
                "detail": "cannot start a control connection thread",
                "error": error.to_string(),
            });
        }
    }
}

/// A persistent control connection: requests are answered in order until
/// the client goes away, the connection idles for [`CONTROL_IDLE`], or it
/// asked for Shutdown (answered, then the daemon's shutdown is signalled).
/// The upgrade must carry `token` (refused before any request is read).
fn serve_control(ctx: &Arc<Ctx>, stream: TcpStream, token: &str, busy: &AtomicBool) {
    let Ok(mut socket) = ws::accept_authorized(stream, Some(token)) else {
        return;
    };
    if let tungstenite::stream::MaybeTlsStream::Plain(tcp) = socket.get_mut() {
        let _ = tcp.set_read_timeout(Some(CONTROL_IDLE));
    }
    loop {
        let received = match receive_request(&mut socket) {
            Ok(received) => received,
            Err(_) => return,
        };
        busy.store(true, Ordering::Release);
        let parsed = received.and_then(|raw| {
            serde_json::from_value::<Action>(raw).map_err(|error| error.to_string())
        });
        let shutdown = matches!(parsed, Ok(Action::Shutdown));
        let result = match parsed {
            Ok(action) => catch_unwind(AssertUnwindSafe(|| manage(ctx, action)))
                .unwrap_or_else(|_| Err(anyhow!("UNKNOWN_ERROR: the request handler panicked"))),
            Err(error) => Err(anyhow!("INVALID_REQUEST: {error}")),
        };
        let sent = ws::reply(&mut socket, result);
        busy.store(false, Ordering::Release);
        if shutdown {
            // The reply is flushed: now the main thread may finish (§3.6
            // 4–6). Also after an error: a refused Shutdown means a sequence
            // is already under way (the main thread waits for its result),
            // and a sequence that panicked must still end the daemon.
            ctx.shutdown.cancel();
            return;
        }
        if sent.is_err() {
            return;
        }
    }
}

/// One control request. `Err` is a transport failure (the connection is
/// done); `Ok(Err(detail))` is a complete frame that is not JSON: it was
/// read in full, so the connection stays usable and the caller answers it
/// `INVALID_REQUEST`.
fn receive_request(socket: &mut ws::Socket) -> Result<Result<Value, String>> {
    match ws::receive::<Value>(socket) {
        Ok(raw) => Ok(Ok(raw)),
        // `ws::receive` decodes only after the frame was read, so a JSON
        // error never leaves a partial frame behind.
        Err(error) => match error.downcast_ref::<serde_json::Error>() {
            Some(syntax) => Ok(Err(syntax.to_string())),
            None => Err(error),
        },
    }
}

// ---------------------------------------------------------------------------
// Windows: standard handles (CCR-WP1.INT-1)

/// Marks every handle the daemon inherited non-inheritable, its standard
/// handles first. `Command::spawn` creates processes with
/// `bInheritHandles = TRUE`, so every inheritable handle of the daemon would
/// reach its workers: the standard handles its parent gave it (a pipe from a
/// shell or CI runner, CCR-WP1.INT-1), and any other inheritable handle of
/// that parent (inherited by value into this process). This runs first in
/// `run`, before the daemon starts any process, when every inheritable
/// handle in the table came from the parent (`std` creates its own handles
/// non-inheritable). The explicit worker stdio (null, a pipe, `worker.log`)
/// is duplicated as inheritable by `std` at each spawn and is not affected.
#[cfg(windows)]
fn keep_inherited_handles_private() {
    type Handle = *mut core::ffi::c_void;
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetStdHandle(which: u32) -> Handle;
        fn GetCurrentProcess() -> Handle;
        fn SetHandleInformation(handle: Handle, mask: u32, flags: u32) -> i32;
    }
    #[link(name = "ntdll")]
    unsafe extern "system" {
        fn NtQueryInformationProcess(
            process: Handle,
            class: u32,
            information: *mut core::ffi::c_void,
            length: u32,
            returned: *mut u32,
        ) -> i32;
    }
    /// `PROCESS_HANDLE_TABLE_ENTRY_INFO` (ntpsapi.h).
    #[repr(C)]
    #[derive(Clone, Copy)]
    struct Entry {
        handle: Handle,
        handle_count: usize,
        pointer_count: usize,
        granted_access: u32,
        object_type_index: u32,
        attributes: u32,
        reserved: u32,
    }
    const STD_INPUT_HANDLE: u32 = -10i32 as u32;
    const STD_OUTPUT_HANDLE: u32 = -11i32 as u32;
    const STD_ERROR_HANDLE: u32 = -12i32 as u32;
    const HANDLE_FLAG_INHERIT: u32 = 1;
    /// `ProcessHandleInformation` (Windows 8 and later).
    const PROCESS_HANDLE_INFORMATION: u32 = 51;
    const STATUS_INFO_LENGTH_MISMATCH: i32 = 0xC000_0004_u32 as i32;
    const OBJ_INHERIT: u32 = 0x2;
    for which in [STD_INPUT_HANDLE, STD_OUTPUT_HANDLE, STD_ERROR_HANDLE] {
        // SAFETY: plain Win32 calls on this process's own standard handles;
        // a missing or invalid handle is skipped.
        unsafe {
            let handle = GetStdHandle(which);
            if !handle.is_null() && handle as isize != -1 {
                SetHandleInformation(handle, HANDLE_FLAG_INHERIT, 0);
            }
        }
    }
    // Every other inheritable handle of the table. A failed query leaves
    // them as they are (the standard handles are done).
    let mut buffer = vec![0u8; 64 * 1024];
    for _ in 0..4 {
        let mut needed = 0u32;
        // SAFETY: the buffer is valid for `len` bytes; the call writes at
        // most that many and reports the size it needs.
        let status = unsafe {
            NtQueryInformationProcess(
                GetCurrentProcess(),
                PROCESS_HANDLE_INFORMATION,
                buffer.as_mut_ptr().cast(),
                u32::try_from(buffer.len()).unwrap_or(u32::MAX),
                &mut needed,
            )
        };
        if status == STATUS_INFO_LENGTH_MISMATCH {
            let next = (needed as usize).max(buffer.len() * 2);
            buffer.resize(next + 4096, 0);
            continue;
        }
        if status < 0 {
            return;
        }
        let header = 2 * std::mem::size_of::<usize>();
        let entry = std::mem::size_of::<Entry>();
        // SAFETY: the first field is the entry count; unaligned reads of a
        // byte buffer the kernel filled.
        let count = unsafe { std::ptr::read_unaligned(buffer.as_ptr().cast::<usize>()) };
        let available = buffer.len().saturating_sub(header) / entry;
        for index in 0..count.min(available) {
            // SAFETY: `index < available` keeps the read inside the buffer.
            let entry: Entry = unsafe {
                std::ptr::read_unaligned(buffer.as_ptr().add(header + index * entry).cast())
            };
            if entry.attributes & OBJ_INHERIT != 0 {
                // SAFETY: a handle value of this process's own table.
                unsafe {
                    SetHandleInformation(entry.handle, HANDLE_FLAG_INHERIT, 0);
                }
            }
        }
        return;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A listener on every interface is woken on loopback: the stop returns
    /// well before the bounded wait (Windows refuses a connect to
    /// `0.0.0.0`/`[::]`, which used to leave the accept thread blocked).
    #[test]
    fn a_listener_on_every_interface_is_woken_on_loopback() {
        for bind in ["0.0.0.0:0", "[::]:0"] {
            let Ok(listener) = TcpListener::bind(bind) else {
                continue; // no IPv6 on this host
            };
            let address = listener.local_addr().unwrap();
            let wake = wake_address(address);
            assert!(wake.ip().is_loopback(), "{bind}: {wake}");
            assert_eq!(wake.port(), address.port());
            let stop = Arc::new(AtomicBool::new(false));
            let accept = {
                let stop = stop.clone();
                thread::spawn(move || {
                    for stream in listener.incoming() {
                        if stop.load(Ordering::Acquire) {
                            return;
                        }
                        drop(stream);
                    }
                })
            };
            let started = Instant::now();
            stop_accept("test", &stop, address, accept);
            assert!(
                started.elapsed() < ACCEPT_JOIN,
                "{bind}: the accept loop was not woken ({:?})",
                started.elapsed()
            );
        }
        // A specific address is kept as it is.
        let specific: SocketAddr = "127.0.0.1:5".parse().unwrap();
        assert_eq!(wake_address(specific), specific);
        let specific: SocketAddr = "[::1]:5".parse().unwrap();
        assert_eq!(wake_address(specific), specific);
    }
}
