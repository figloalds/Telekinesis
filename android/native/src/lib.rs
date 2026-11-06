//! The Android build of tkfs: the orchestrator and its workers on threads of the app
//! process, with a small JNI surface for the app and its DocumentsProvider.
use anyhow::{Context, Result, anyhow};
use jni::{
    JNIEnv,
    objects::{JByteArray, JClass, JString},
    sys::{jint, jlong, jstring},
};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, OnceLock, Weak,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Sender},
    },
    thread,
    time::{Duration, Instant},
};
use tkfs::{
    core::EntryKind,
    model::orchestrator::CallError,
    orchestrator::{
        folder::Launcher,
        hub::{self, Daemon},
        process::{EmbeddedWorker, LaunchSpec, ProcessEvent, WorkerProcess},
        worker::{self, Fatal, Ready, WorkerRecord},
        worker_client::WorkerLink,
    },
    runtime::Engine,
};

/// How long a cold-started request waits for the orchestrator and its worker.
const START_WAIT: Duration = Duration::from_secs(15);
/// How long a stopping worker gets to exit (its own close bounds are 9 s).
const KILL_WAIT: Duration = Duration::from_secs(12);
/// Seconds between the FILETIME epoch (1601) and the Unix epoch, in 100 ns.
const UNIX_EPOCH_FILETIME: u64 = 116_444_736_000_000_000;

static DAEMON: OnceLock<Arc<Daemon>> = OnceLock::new();
static START: OnceLock<()> = OnceLock::new();
static START_ERROR: Mutex<Option<String>> = Mutex::new(None);
static ENGINES: Mutex<BTreeMap<PathBuf, Arc<Engine>>> = Mutex::new(BTreeMap::new());
/// Running workers by instance id, for [`ThreadLauncher::kill_verified`].
static WORKERS: Mutex<BTreeMap<String, Arc<ThreadWorker>>> = Mutex::new(BTreeMap::new());

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poison| poison.into_inner())
}

struct ThreadWorker {
    /// Weak: the store's lock must go with the worker thread, even while the
    /// controller still holds this handle and is starting the next worker.
    engine: Weak<Engine>,
    exited: AtomicBool,
}

impl EmbeddedWorker for ThreadWorker {
    fn exited(&self) -> bool {
        self.exited.load(Ordering::Acquire)
    }

    fn stop(&self) -> Result<bool> {
        match self.engine.upgrade() {
            Some(engine) if !self.exited() => {
                engine.request_shutdown()?;
                Ok(true)
            }
            _ => Ok(false),
        }
    }
}

/// Runs each folder's worker on a thread and keeps its engine for the provider.
struct ThreadLauncher;

impl Launcher for ThreadLauncher {
    fn spawn(&self, spec: &LaunchSpec, sink: Sender<ProcessEvent>) -> Result<WorkerProcess> {
        std::fs::create_dir_all(&spec.state)?;
        let engine = Engine::open(&spec.state, None)
            .map_err(|error| anyhow::Error::new(Fatal::store_open(&error)))?;
        let state = spec.state.clone();
        lock(&ENGINES).insert(state.clone(), engine.clone());
        let handle = Arc::new(ThreadWorker {
            engine: Arc::downgrade(&engine),
            exited: AtomicBool::new(false),
        });
        let (ready_tx, ready_rx) = mpsc::channel::<Ready>();
        {
            let sink = sink.clone();
            let handle = handle.clone();
            thread::Builder::new()
                .name("tkfs-embedded-ready".into())
                .spawn(move || {
                    if let Ok(ready) = ready_rx.recv() {
                        let mut workers = lock(&WORKERS);
                        workers.retain(|_, worker| !worker.exited());
                        workers.insert(ready.instance.clone(), handle);
                        drop(workers);
                        let _ = sink.send(ProcessEvent::Ready(ready));
                    }
                })?;
        }
        let exiting = handle.clone();
        thread::Builder::new()
            .name("tkfs-embedded-worker".into())
            .spawn(move || {
                let options = worker::Options {
                    supervised: false,
                    mount: None,
                    on_ready: Some(ready_tx),
                };
                let result = worker::run(engine, options);
                lock(&ENGINES).remove(&state);
                exiting.exited.store(true, Ordering::Release);
                let code = match result {
                    Ok(()) => Some(0),
                    Err(error) => {
                        let fatal = match error.downcast::<Fatal>() {
                            Ok(fatal) => fatal,
                            Err(error) => Fatal::new(
                                "WORKER_ERROR",
                                format!("{error:#}"),
                                worker::EXIT_INTERNAL,
                            ),
                        };
                        let _ = sink.send(ProcessEvent::Fatal {
                            code: fatal.code.clone(),
                            message: fatal.message.clone(),
                        });
                        Some(fatal.exit)
                    }
                };
                let _ = sink.send(ProcessEvent::Exited {
                    code,
                    tail: String::new(),
                });
            })?;
        Ok(WorkerProcess::embedded(handle))
    }

    /// Workers die with the process, so a record left behind names nothing.
    fn adopt(
        &self,
        _: &Path,
        _: Sender<ProcessEvent>,
    ) -> Result<Option<(WorkerRecord, WorkerProcess)>> {
        Ok(None)
    }

    fn connect(&self, ready: &Ready, lanes: usize) -> Result<Arc<WorkerLink>, CallError> {
        WorkerLink::connect(ready, lanes)
    }

    /// A thread cannot be killed: asks it to stop and waits, bounded, so the
    /// store's lock is free before the controller starts the next worker.
    fn kill_verified(&self, record: &WorkerRecord) -> Result<bool> {
        let Some(worker) = lock(&WORKERS).get(&record.instance).cloned() else {
            return Ok(false);
        };
        // A failed final flush still stops it; unsealed data is recovered at open.
        let _ = worker.stop();
        let end = Instant::now() + KILL_WAIT;
        while !worker.exited() {
            if Instant::now() >= end {
                anyhow::bail!("STRANDED_WORKER: {} did not stop", record.instance);
            }
            thread::sleep(Duration::from_millis(20));
        }
        Ok(true)
    }
}

/// Starts the orchestrator once; later calls return immediately.
fn start(config: PathBuf) {
    START.get_or_init(|| {
        let spawned = thread::Builder::new()
            .name("tkfs-orchestrator".into())
            .spawn(move || {
                let result = hub::run_embedded(&config, Arc::new(ThreadLauncher), |daemon| {
                    let _ = DAEMON.set(daemon);
                });
                if let Err(error) = result {
                    *lock(&START_ERROR) = Some(format!("{error:#}"));
                }
            });
        if let Err(error) = spawned {
            *lock(&START_ERROR) = Some(error.to_string());
        }
    });
}

fn daemon() -> Result<&'static Arc<Daemon>> {
    let end = Instant::now() + START_WAIT;
    loop {
        if let Some(daemon) = DAEMON.get() {
            return Ok(daemon);
        }
        if let Some(error) = lock(&START_ERROR).clone() {
            return Err(anyhow!("ORCHESTRATOR_FAILED: {error}"));
        }
        if Instant::now() >= end {
            return Err(anyhow!("ORCHESTRATOR_NOT_READY"));
        }
        thread::sleep(Duration::from_millis(25));
    }
}

/// The folder's engine, waiting for its worker on a cold start.
fn engine(folder: &str) -> Result<Arc<Engine>> {
    let daemon = daemon()?;
    let record = daemon
        .catalog
        .folders()?
        .into_iter()
        .find(|record| record.id == folder)
        .with_context(|| format!("FOLDER_NOT_FOUND: {folder}"))?;
    let end = Instant::now() + START_WAIT;
    loop {
        // A stopping engine is on its way out: wait for its replacement.
        if let Some(engine) = lock(&ENGINES).get(&record.state)
            && !engine.stopping.load(Ordering::Acquire)
        {
            return Ok(engine.clone());
        }
        if Instant::now() >= end {
            return Err(anyhow!("WORKER_NOT_READY: {folder}"));
        }
        thread::sleep(Duration::from_millis(25));
    }
}

fn entry_json(entry: &tkfs::core::Entry) -> Value {
    let directory = entry.kind == EntryKind::Directory;
    let size = entry
        .working_len
        .or_else(|| entry.content.as_ref().map(|content| content.total_len))
        .unwrap_or(0);
    let modified = entry.times.modified.saturating_sub(UNIX_EPOCH_FILETIME) / 10_000;
    json!({"name": entry.name, "directory": directory, "size": size, "modified_ms": modified})
}

fn list(folder: &str, path: &str) -> Result<Value> {
    let engine = engine(folder)?;
    let token = engine.open_file(path, false, true, false)?;
    let entries = engine.children(token, true);
    engine.close(token)?;
    Ok(Value::Array(entries?.iter().map(entry_json).collect()))
}

fn text(env: &mut JNIEnv, value: &JString) -> Result<String> {
    Ok(env.get_string(value)?.into())
}

/// Turns a failure into a Java `IOException` carrying the error code.
fn or_throw<T>(env: &mut JNIEnv, result: Result<T>, fallback: T) -> T {
    match result {
        Ok(value) => value,
        Err(error) => {
            let _ = env.throw_new("java/io/IOException", format!("{error:#}"));
            fallback
        }
    }
}

fn string_or_throw(env: &mut JNIEnv, result: Result<String>) -> jstring {
    let created = result.and_then(|value| Ok(env.new_string(value)?));
    match created {
        Ok(value) => value.into_raw(),
        Err(error) => {
            let _ = env.throw_new("java/io/IOException", format!("{error:#}"));
            std::ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_dev_tkfs_spike_Native_start(
    mut env: JNIEnv,
    _: JClass,
    config: JString,
) {
    let result = text(&mut env, &config).map(|config| start(PathBuf::from(config)));
    or_throw(&mut env, result, ());
}

/// One `manage` action, in-process: the same actions the CLI sends.
#[unsafe(no_mangle)]
pub extern "system" fn Java_dev_tkfs_spike_Native_manage(
    mut env: JNIEnv,
    _: JClass,
    action: JString,
    payload: JString,
) -> jstring {
    let result = (|| {
        let action = text(&mut env, &action)?;
        let payload: Value = serde_json::from_str(&text(&mut env, &payload)?)?;
        let reply = daemon()?.execute(&action, &payload)?;
        Ok(reply.to_string())
    })();
    string_or_throw(&mut env, result)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_dev_tkfs_spike_Native_list(
    mut env: JNIEnv,
    _: JClass,
    folder: JString,
    path: JString,
) -> jstring {
    let result = (|| {
        let folder = text(&mut env, &folder)?;
        let path = text(&mut env, &path)?;
        Ok(list(&folder, &path)?.to_string())
    })();
    string_or_throw(&mut env, result)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_dev_tkfs_spike_Native_stat(
    mut env: JNIEnv,
    _: JClass,
    folder: JString,
    path: JString,
) -> jstring {
    let result = (|| {
        let folder = text(&mut env, &folder)?;
        let path = text(&mut env, &path)?;
        Ok(entry_json(&engine(&folder)?.lookup(&path)?).to_string())
    })();
    string_or_throw(&mut env, result)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_dev_tkfs_spike_Native_open(
    mut env: JNIEnv,
    _: JClass,
    folder: JString,
    path: JString,
) -> jlong {
    let result = (|| {
        let folder = text(&mut env, &folder)?;
        let path = text(&mut env, &path)?;
        Ok(engine(&folder)?.open_file(&path, false, false, false)? as jlong)
    })();
    or_throw(&mut env, result, -1)
}

/// Reads into `out`; hydrates missing chunks and blocks while it does.
#[unsafe(no_mangle)]
pub extern "system" fn Java_dev_tkfs_spike_Native_read(
    mut env: JNIEnv,
    _: JClass,
    folder: JString,
    token: jlong,
    offset: jlong,
    out: JByteArray,
    size: jint,
) -> jint {
    let result = (|| {
        let folder = text(&mut env, &folder)?;
        let mut buffer = vec![0u8; size.max(0) as usize];
        let count = engine(&folder)?.read(token as u64, offset as u64, &mut buffer)?;
        // SAFETY: i8 and u8 have the same layout; `count` is within `buffer`.
        let bytes = unsafe { std::slice::from_raw_parts(buffer.as_ptr().cast::<i8>(), count) };
        env.set_byte_array_region(&out, 0, bytes)?;
        Ok(count as jint)
    })();
    or_throw(&mut env, result, -1)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_dev_tkfs_spike_Native_close(
    mut env: JNIEnv,
    _: JClass,
    folder: JString,
    token: jlong,
) {
    let result = (|| {
        let folder = text(&mut env, &folder)?;
        engine(&folder)?.close(token as u64)
    })();
    or_throw(&mut env, result, ());
}
