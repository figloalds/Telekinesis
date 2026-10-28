//! The worker process: one folder store, served to local clients over a
//! token-authenticated loopback session. It never listens for peers.
//!
//! Startup writes `STATE/runtime.token` (owner-only), `worker.json` and
//! `runtime.json`, then prints one Ready line on stdout. A startup failure
//! prints one fatal line `{"fatal":true,"code","message","exit"}` instead and
//! exits with that code. A clean exit removes the three files.
use super::{
    control,
    demand::{Demand, Seq},
    settings::Settings,
    sync,
};
use crate::{
    content::working::lock,
    model::protocol::{IPC_VERSION, LaneRole},
    runtime::Engine,
    session::{self, Conn, ConnOptions, Handler, Incoming},
};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    fmt,
    io::Write,
    net::TcpListener,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

pub const EXIT_INTERNAL: i32 = 70;
pub const EXIT_STORE: i32 = 73;
pub const EXIT_LOCKED: i32 = 75;
pub const EXIT_CONFIG: i32 = 78;
/// Quiet exit after this long without contact (env `TKFS_UNSUPERVISED_EXIT_S`).
pub const UNSUPERVISED_EXIT_DEFAULT_S: u64 = 600;
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);
pub const WATCH_MAX_WAIT_MS: u64 = 5000;

pub const RUNTIME_FILE: &str = "runtime.json";
pub const TOKEN_FILE: &str = "runtime.token";
pub const RECORD_FILE: &str = "worker.json";
/// The plain-WebSocket control record of earlier versions; removed at start.
const LEGACY_CONTROL_FILE: &str = "control.json";

/// Bounds on loopback sessions being set up and served at once.
const MAX_HANDSHAKES: usize = 16;
const MAX_SESSIONS: usize = 64;
/// How long open sessions get to finish (e.g. deliver the `shutdown` reply).
const CLOSE_GRACE: Duration = Duration::from_secs(3);

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct WorkerRecord {
    pub format: u32,
    pub pid: u32,
    pub start_ms: u64,
    pub image: PathBuf,
    pub version: String,
    pub instance: String,
    pub address: String,
    pub ipc: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Ready {
    pub ready: bool,
    pub repo: String,
    pub device: String,
    pub instance: String,
    pub pid: u32,
    pub address: String,
    pub token: String,
    pub version: String,
    pub ipc: u32,
    pub mounts: Vec<PathBuf>,
}

pub struct Options {
    pub supervised: bool,
    pub mount: Option<PathBuf>,
}

/// A startup failure: printed as one JSON line on stdout, then the process
/// exits with `exit`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Fatal {
    pub fatal: bool,
    pub code: String,
    pub message: String,
    pub exit: i32,
}

impl Fatal {
    pub fn new(code: &str, message: impl Into<String>, exit: i32) -> Self {
        Self {
            fatal: true,
            code: code.into(),
            message: message.into(),
            exit,
        }
    }

    /// `code` is the error's own code when it has one, else `fallback`.
    pub fn from_error(error: &anyhow::Error, fallback: &str, exit: i32) -> Self {
        let wire = control::error_wire(error);
        let code = if wire.code == "WORKER_ERROR" {
            fallback
        } else {
            &wire.code
        };
        Self::new(code, format!("{error:#}"), exit)
    }

    /// A failed `Engine::open`: `STORE_ALREADY_OPEN` exits 75, anything else 73.
    pub fn store_open(error: &anyhow::Error) -> Self {
        if format!("{error:#}").contains("STORE_ALREADY_OPEN") {
            Self::new("STORE_ALREADY_OPEN", format!("{error:#}"), EXIT_LOCKED)
        } else {
            Self::from_error(error, "STORE_OPEN_FAILED", EXIT_STORE)
        }
    }

    pub fn line(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| {
            format!(
                "{{\"fatal\":true,\"code\":\"{}\",\"exit\":{}}}",
                self.code, self.exit
            )
        })
    }
}

impl fmt::Display for Fatal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for Fatal {}

/// Prints the fatal line on stdout (and the message on stderr) and exits.
pub fn exit_fatal(fatal: &Fatal) -> ! {
    print_line(&fatal.line());
    eprintln!("{}", fatal.message);
    std::process::exit(fatal.exit)
}

/// One line on stdout, flushed. A closed stdout (the supervisor went away)
/// is not an error: the worker keeps serving until told or timed out.
fn print_line(line: &str) {
    let mut out = std::io::stdout().lock();
    let _ = writeln!(out, "{line}");
    let _ = out.flush();
}

/// Serves the loopback session for one folder's worker process.
pub struct ControlService {
    pub engine: Arc<Engine>,
    pub settings: Settings,
    /// `feed.*`, `meta.exchange` and `objects.*`; owns the object demand.
    pub sync: sync::Service,
    pub seq: Arc<Seq>,
    pub instance: String,
    /// Last request from any client; drives the unsupervised quiet exit.
    pub supervised_contact: Mutex<Instant>,
}

impl ControlService {
    pub fn execute(&self, operation: &str, p: &Value) -> Result<Value> {
        match operation {
            "hello" => Ok(control::hello(&self.engine, &self.instance)),
            "watch" => control::watch(&self.engine, &self.seq, &self.sync.demand, p),
            _ => control::execute(&self.engine, &self.settings, operation, p),
        }
    }

    fn touch(&self) {
        *lock(&self.supervised_contact) = Instant::now();
    }
}

impl Handler for ControlService {
    fn handle(&self, req: Incoming) {
        self.touch();
        if sync::Service::routes(&req.op) {
            self.sync.handle(req);
            self.touch();
            return;
        }
        let result = self
            .execute(&req.op, &req.header)
            .and_then(|value| Ok(serde_json::to_vec(&value)?));
        self.touch();
        match result {
            Ok(body) => req.responder.reply(json!({}), body),
            Err(error) => req.responder.error(control::error_wire(&error)),
        }
    }
}

/// The worker main. Startup failures come back as [`Fatal`]; every exit path
/// shuts the engine down, detaches a mount and removes the runtime files.
pub fn run(engine: Arc<Engine>, options: Options) -> Result<()> {
    let root = lock(&engine.store).root.clone();
    let served = serve(engine.clone(), &options, &root);
    let shutdown = engine.shutdown();
    #[cfg(any(windows, target_os = "linux"))]
    let detach = crate::mount::detach();
    #[cfg(not(any(windows, target_os = "linux")))]
    let detach: Result<()> = Ok(());
    remove_runtime_files(&root);
    let cleanup: Vec<String> = [shutdown.err(), detach.err()]
        .into_iter()
        .flatten()
        .map(|error| format!("{error:#}"))
        .collect();
    match served {
        Err(error) if error.downcast_ref::<Fatal>().is_some() => {
            let mut fatal = error.downcast::<Fatal>().expect("checked above");
            if !cleanup.is_empty() {
                fatal.message = format!("{}; cleanup: {}", fatal.message, cleanup.join("; "));
            }
            Err(fatal.into())
        }
        served => {
            let errors: Vec<String> = served
                .err()
                .map(|error| format!("{error:#}"))
                .into_iter()
                .chain(cleanup)
                .collect();
            ensure!(
                errors.is_empty(),
                "SHUTDOWN_INCOMPLETE: {}",
                errors.join("; ")
            );
            Ok(())
        }
    }
}

fn remove_runtime_files(root: &Path) {
    for file in [RUNTIME_FILE, TOKEN_FILE, RECORD_FILE] {
        let _ = std::fs::remove_file(root.join(file));
    }
}

fn remove_if_present(path: &Path) -> std::io::Result<()> {
    match std::fs::remove_file(path) {
        Err(error) if error.kind() != std::io::ErrorKind::NotFound => Err(error),
        _ => Ok(()),
    }
}

/// Opens `path` with `options`, owner-only (`0600`) on Unix: a created file
/// gets that mode and an existing one is narrowed to it.
pub fn open_private(
    options: &mut std::fs::OpenOptions,
    path: &Path,
) -> std::io::Result<std::fs::File> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options.open(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(file)
}

/// Creates `path` afresh, owner-only (`0600`) on Unix.
fn write_private(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    remove_if_present(path)?;
    let mut file = open_private(
        std::fs::OpenOptions::new().write(true).create_new(true),
        path,
    )?;
    file.write_all(bytes)?;
    file.sync_all()
}

/// Writes `value` to a temporary file, then renames it over `path`, so a
/// reader never sees a partial record.
fn write_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let temporary = path.with_extension("json.tmp");
    write_private(&temporary, &serde_json::to_vec(value)?)?;
    std::fs::rename(&temporary, path)?;
    Ok(())
}

fn startup<T>(result: Result<T>, code: &str, exit: i32) -> Result<T> {
    result.map_err(|error| Fatal::from_error(&error, code, exit).into())
}

fn quiet_period() -> Duration {
    Duration::from_secs(
        std::env::var("TKFS_UNSUPERVISED_EXIT_S")
            .ok()
            .and_then(|value| value.trim().parse().ok())
            .unwrap_or(UNSUPERVISED_EXIT_DEFAULT_S),
    )
}

/// Sleeps `period` in short steps; false once the engine is stopping.
fn pause(engine: &Engine, period: Duration) -> bool {
    let end = Instant::now() + period;
    loop {
        if engine.stopping.load(Ordering::Acquire) {
            return false;
        }
        let left = end.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return true;
        }
        thread::sleep(left.min(Duration::from_millis(50)));
    }
}

fn serve(engine: Arc<Engine>, options: &Options, root: &Path) -> Result<()> {
    startup(crate::clock::check(), "CLOCK_INVALID", EXIT_CONFIG)?;
    if let Some(path) = &options.mount {
        #[cfg(any(windows, target_os = "linux"))]
        startup(
            crate::mount::attach(engine.clone(), path),
            "MOUNT_FAILED",
            EXIT_INTERNAL,
        )?;
        #[cfg(not(any(windows, target_os = "linux")))]
        return Err(Fatal::new(
            "MOUNT_UNSUPPORTED",
            format!("MOUNT_UNSUPPORTED: cannot mount {}", path.display()),
            EXIT_CONFIG,
        )
        .into());
    }
    let listener = startup(
        TcpListener::bind("127.0.0.1:0")
            .and_then(|listener| listener.set_nonblocking(true).map(|()| listener))
            .map_err(Into::into),
        "LOOPBACK_BIND_FAILED",
        EXIT_INTERNAL,
    )?;
    let address = format!("ws://{}", listener.local_addr()?);
    let token = crate::ws::tokens::generate();
    crate::util::log::register_secret(&token);
    let instance = crate::core::id();
    let mounts = control::mounts();
    let pid = std::process::id();
    let version = env!("CARGO_PKG_VERSION").to_owned();
    let record = WorkerRecord {
        format: 1,
        pid,
        // Task 7 replaces these with the process identity.
        start_ms: crate::util::time::now_ms(),
        image: std::env::current_exe().unwrap_or_default(),
        version: version.clone(),
        instance: instance.clone(),
        address: address.clone(),
        ipc: IPC_VERSION,
    };
    startup(
        (|| {
            let _ = std::fs::remove_file(root.join(LEGACY_CONTROL_FILE));
            // A reader that still sees an old runtime.json must never pair it
            // with the new token: the record goes first, the token next.
            remove_if_present(&root.join(RUNTIME_FILE))?;
            write_private(&root.join(TOKEN_FILE), token.as_bytes())?;
            write_json(&root.join(RECORD_FILE), &record)?;
            write_json(
                &root.join(RUNTIME_FILE),
                &json!({"address":address,"pid":pid,"instance":instance,"ipc":IPC_VERSION,"mounts":mounts}),
            )
        })(),
        "RUNTIME_FILES_FAILED",
        EXIT_STORE,
    )?;

    let seq = Seq::new();
    let demand = Demand::new(seq.clone(), engine.working.content().packs.clone());
    engine.working.content().set_fetcher(demand.clone());
    let service = Arc::new(ControlService {
        engine: engine.clone(),
        settings: Settings {
            engine: engine.clone(),
        },
        sync: sync::Service {
            engine: engine.clone(),
            demand,
        },
        seq: seq.clone(),
        instance: instance.clone(),
        supervised_contact: Mutex::new(Instant::now()),
    });

    let mut threads = Vec::new();
    {
        let engine = engine.clone();
        let seq = seq.clone();
        threads.push(
            thread::Builder::new()
                .name("tkfs-heartbeat".into())
                .spawn(move || {
                    seq.bump();
                    while pause(&engine, HEARTBEAT_INTERVAL) {
                        seq.bump();
                    }
                })?,
        );
    }
    if options.supervised {
        let engine = engine.clone();
        let service = service.clone();
        let quiet = quiet_period();
        threads.push(
            thread::Builder::new()
                .name("tkfs-unsupervised".into())
                .spawn(move || {
                    while pause(&engine, HEARTBEAT_INTERVAL) {
                        let idle = lock(&service.supervised_contact).elapsed();
                        if idle >= quiet && engine.writable_tokens().is_empty() {
                            eprintln!(
                                "UNSUPERVISED_EXIT: no contact for {} s; stopping",
                                idle.as_secs()
                            );
                            if let Err(error) = engine.request_shutdown() {
                                eprintln!("{error:#}");
                            }
                            break;
                        }
                    }
                })?,
        );
    }

    let (repo, device) = {
        let store = lock(&engine.store);
        (store.repo.clone(), store.device.clone())
    };
    let ready = Ready {
        ready: true,
        repo,
        device,
        instance,
        pid,
        address,
        token: token.clone(),
        version,
        ipc: IPC_VERSION,
        mounts,
    };
    service.touch();
    print_line(&serde_json::to_string(&ready)?);

    let sessions = accept_loop(&engine, &listener, &token, &service);
    drop(listener);
    // Wake parked `watch` calls; let replies already queued (the `shutdown`
    // answer) reach their clients before the process goes away.
    seq.bump();
    for conn in &sessions {
        conn.goaway("WORKER_STOPPING", "the worker is stopping");
    }
    let end = Instant::now() + CLOSE_GRACE;
    for conn in &sessions {
        conn.wait_closed(end.saturating_duration_since(Instant::now()));
        conn.close();
    }
    for thread in threads {
        let _ = thread.join();
    }
    Ok(())
}

/// Accepts loopback sessions until the engine stops; returns the live ones.
fn accept_loop(
    engine: &Engine,
    listener: &TcpListener,
    token: &str,
    service: &Arc<ControlService>,
) -> Vec<Conn> {
    let (tx, rx) = mpsc::channel::<Conn>();
    let handshakes = Arc::new(AtomicUsize::new(0));
    let mut sessions: Vec<Conn> = Vec::new();
    while !engine.stopping.load(Ordering::Acquire) {
        sessions.extend(rx.try_iter());
        sessions.retain(|conn| conn.closed().is_none());
        match listener.accept() {
            Ok((stream, _)) => {
                if sessions.len() >= MAX_SESSIONS
                    || handshakes.load(Ordering::Acquire) >= MAX_HANDSHAKES
                {
                    continue;
                }
                handshakes.fetch_add(1, Ordering::AcqRel);
                let (tx, token, service, count) = (
                    tx.clone(),
                    token.to_owned(),
                    service.clone(),
                    handshakes.clone(),
                );
                let spawned = thread::Builder::new()
                    .name("tkfs-loopback-accept".into())
                    .spawn(move || {
                        if let Ok(conn) = session::accept_loopback(
                            stream,
                            Some(&token),
                            ConnOptions::for_role(LaneRole::Control),
                        ) {
                            conn.serve(service);
                            if let Err(mpsc::SendError(conn)) = tx.send(conn) {
                                conn.close();
                            }
                        }
                        count.fetch_sub(1, Ordering::AcqRel);
                    });
                if spawned.is_err() {
                    handshakes.fetch_sub(1, Ordering::AcqRel);
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(20));
            }
            Err(error) => {
                eprintln!("loopback accept: {error}");
                thread::sleep(Duration::from_millis(20));
            }
        }
    }
    // Handshakes in flight finish within their own 5 s bound.
    let end = Instant::now() + Duration::from_secs(6);
    while handshakes.load(Ordering::Acquire) > 0 && Instant::now() < end {
        thread::sleep(Duration::from_millis(10));
    }
    drop(tx);
    sessions.extend(rx.try_iter());
    sessions
}
