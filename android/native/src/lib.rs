//! The Android build of tkfs: the orchestrator and its workers on threads of the app
//! process, with a small JNI surface for the app and its DocumentsProvider.
mod phone;

use anyhow::{Result, anyhow, ensure};
use jni::{
    JNIEnv,
    objects::{JByteArray, JClass, JString},
    sys::{JNI_FALSE, jboolean, jint, jlong, jstring},
};
use phone::{DaemonPeers, Phone};
use serde_json::{Value, json};
use std::{
    any::Any,
    panic::{AssertUnwindSafe, catch_unwind},
    path::PathBuf,
    sync::{Arc, Once, OnceLock, PoisonError},
};
use tkfs::{
    core::{EntryKind, ROOT},
    model::errors::{ErrorWire, UNKNOWN_CODE},
    orchestrator::embedded::Host,
    runtime::Engine,
};

/// Seconds between the FILETIME epoch (1601) and the Unix epoch, in 100 ns.
const UNIX_EPOCH_FILETIME: u64 = 116_444_736_000_000_000;

static HOST: OnceLock<Arc<Host>> = OnceLock::new();

/// Starts the orchestrator once; later calls return immediately.
fn start(config: PathBuf) {
    HOST.get_or_init(|| Host::start(config));
}

fn host() -> Result<&'static Arc<Host>> {
    HOST.get().ok_or_else(|| anyhow!("ORCHESTRATOR_NOT_READY"))
}

static PHONE: OnceLock<Phone> = OnceLock::new();

/// What the phone made and the PC has not confirmed, in `phone.sqlite` beside the catalog.
fn phone() -> Result<&'static Phone> {
    if let Some(phone) = PHONE.get() {
        return Ok(phone);
    }
    let opened = Phone::open(
        &host()?
            .daemon()?
            .catalog
            .data_directory
            .join("phone.sqlite"),
    )?;
    Ok(PHONE.get_or_init(|| opened))
}

/// The files of `folder` waiting to send, as `[{entity, path}]`.
fn waiting(folder: &str) -> Result<Value> {
    let record = host()?.daemon()?.catalog.resolve(folder)?;
    let files = phone()?.waiting(&record.id)?;
    Ok(Value::Array(
        files
            .iter()
            .map(|file| json!({"entity": file.entity, "path": file.path}))
            .collect(),
    ))
}

/// Asks the PCs what they now hold, and returns `{waiting, unsupported}` for `folder`.
fn confirm(folder: &str) -> Result<Value> {
    let daemon = host()?.daemon()?;
    let record = daemon.catalog.resolve(folder)?;
    let engine = engine(&record.id)?;
    let journal = engine
        .store
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .device
        .clone();
    let peers = DaemonPeers {
        daemon: &daemon,
        folder: &record.id,
        journal: &journal,
    };
    let confirmed = phone()?.confirm(&record.id, &record.repo, &engine, &peers)?;
    Ok(json!({"waiting": confirmed.waiting, "unsupported": confirmed.unsupported}))
}

fn engine(folder: &str) -> Result<Arc<Engine>> {
    host()?.engine(folder)
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

fn list(engine: &Engine, path: &str) -> Result<Value> {
    let token = engine.open_file(path, false, true, false)?;
    let entries = engine.children(token, true);
    engine.close(token)?;
    Ok(Value::Array(entries?.iter().map(entry_json).collect()))
}

/// A handle that attaches to any draft of the file but needs none of its contents, so these work
/// on a file the phone never downloaded, with no PC to ask.
fn open_metadata(engine: &Engine, path: &str) -> Result<u64> {
    engine.open_native_file(path, false, false, false, None, true)
}

/// Makes what the namespace calls just did durable. They leave it for the sealer thread to commit
/// a moment later, and by then that thread may be frozen and the process killed. No file's draft
/// comes along, as a half-written file is its own writer's to seal.
fn sync_namespace(engine: &Engine) -> Result<()> {
    engine
        .store
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .sync_metadata()
}

fn create(engine: &Engine, path: &str, directory: bool) -> Result<()> {
    let token = engine.open_file(path, true, directory, false)?;
    engine.close(token)?;
    sync_namespace(engine)
}

fn rename(engine: &Engine, from: &str, to: &str) -> Result<()> {
    let token = open_metadata(engine, from)?;
    let renamed = engine.rename(token, to, false);
    let closed = engine.close(token);
    renamed.and(closed)?;
    sync_namespace(engine)
}

fn delete(engine: &Engine, path: &str) -> Result<()> {
    delete_tree(engine, path)?;
    sync_namespace(engine)
}

/// Android's Files app asks once for a directory, and expects what it holds to go with it.
fn delete_tree(engine: &Engine, path: &str) -> Result<()> {
    let token = open_metadata(engine, path)?;
    let deleted = (|| {
        let (entry, _) = engine.info(token)?;
        ensure!(entry.id != ROOT, "ROOT_IMMUTABLE");
        if entry.kind == EntryKind::Directory {
            for child in engine.children(token, true)?.iter() {
                delete_tree(
                    engine,
                    &format!("{}/{}", path.trim_end_matches('/'), child.name),
                )?;
            }
        }
        engine.cleanup(token, true)
    })();
    let closed = engine.close(token);
    deleted.and(closed)
}

fn text(env: &mut JNIEnv, value: &JString) -> Result<String> {
    Ok(env.get_string(value)?.into())
}

/// The body of every entry point. Any failure reaches Java as an `IOException` whose message
/// starts with the error code, and `fallback` is returned. Nothing else can cross into Java,
/// where a panic out of `extern "system"` would abort the process. A new entry point only has
/// to put its body in here, or in [`guard_string`].
fn guard<'local, T>(
    env: &mut JNIEnv<'local>,
    fallback: T,
    body: impl FnOnce(&mut JNIEnv<'local>) -> Result<T>,
) -> T {
    install_panic_hook();
    let outcome = catch_unwind(AssertUnwindSafe(|| body(env)));
    // A failed JNI call leaves its exception pending. Calling JNI with one pending aborts under
    // CheckJNI, and left alone it would get past the app's `catch (IOException)`.
    let pending = take_pending(env);
    let message = match (outcome, pending) {
        (Ok(Ok(value)), None) => return value,
        (Ok(Ok(_)), Some(pending)) => format!("{UNKNOWN_CODE}: a JNI call left {pending} pending"),
        (Ok(Err(error)), None) => ErrorWire::from_error(&error).to_string(),
        (Ok(Err(error)), Some(pending)) => {
            format!("{} ({pending})", ErrorWire::from_error(&error))
        }
        (Err(panic), _) => format!("NATIVE_PANIC: {}", panic_text(panic.as_ref())),
    };
    let _ = env.throw_new("java/io/IOException", message);
    fallback
}

/// [`guard`] for an entry point that returns a string.
fn guard_string<'local>(
    env: &mut JNIEnv<'local>,
    body: impl FnOnce(&mut JNIEnv<'local>) -> Result<String>,
) -> jstring {
    guard(env, std::ptr::null_mut(), |env| {
        let value = body(env)?;
        Ok(env.new_string(value)?.into_raw())
    })
}

/// Clears a pending Java exception and says what it was.
fn take_pending(env: &mut JNIEnv) -> Option<String> {
    if !env.exception_check().unwrap_or(true) {
        return None;
    }
    let thrown = env.exception_occurred();
    let _ = env.exception_clear();
    let described = thrown.ok().and_then(|thrown| {
        let described = env
            .call_method(&thrown, "toString", "()Ljava/lang/String;", &[])
            .and_then(|value| value.l())
            .and_then(|value| env.get_string(&JString::from(value)).map(String::from));
        // toString can throw too.
        let _ = env.exception_clear();
        described.ok()
    });
    Some(described.unwrap_or_else(|| "a Java exception".to_owned()))
}

fn panic_text(panic: &(dyn Any + Send)) -> &str {
    panic
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("no message")
}

/// Every panic reaches logcat, on any thread: nothing reads this process's stderr, and only a
/// panic in an entry point becomes an exception. The first entry point runs before the
/// orchestrator starts, so the core's own hook, which also writes the log file, chains to this.
fn install_panic_hook() {
    static INSTALLED: Once = Once::new();
    INSTALLED.call_once(|| {
        std::panic::set_hook(Box::new(|info| {
            let thread = std::thread::current();
            let name = thread.name().unwrap_or("<unnamed>");
            log_error(&format!("panic on {name}: {info}"));
        }));
    });
}

#[cfg(target_os = "android")]
fn log_error(line: &str) {
    use std::ffi::{CString, c_char, c_int};
    const ANDROID_LOG_ERROR: c_int = 6;
    #[link(name = "log")]
    unsafe extern "C" {
        fn __android_log_write(priority: c_int, tag: *const c_char, text: *const c_char) -> c_int;
    }
    let text = CString::new(line.replace('\0', " ")).unwrap_or_default();
    // SAFETY: both are valid C strings for the length of the call.
    unsafe { __android_log_write(ANDROID_LOG_ERROR, c"tkfs".as_ptr(), text.as_ptr()) };
}

#[cfg(not(target_os = "android"))]
fn log_error(line: &str) {
    eprintln!("{line}");
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_dev_tkfs_Native_start(mut env: JNIEnv, _: JClass, config: JString) {
    guard(&mut env, (), |env| {
        start(PathBuf::from(text(env, &config)?));
        Ok(())
    })
}

/// One `manage` action, in-process: the same actions the CLI sends.
#[unsafe(no_mangle)]
pub extern "system" fn Java_dev_tkfs_Native_manage(
    mut env: JNIEnv,
    _: JClass,
    action: JString,
    payload: JString,
) -> jstring {
    guard_string(&mut env, |env| {
        let action = text(env, &action)?;
        let payload: Value = serde_json::from_str(&text(env, &payload)?)?;
        let reply = host()?.daemon()?.execute(&action, &payload)?;
        Ok(reply.to_string())
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_dev_tkfs_Native_list(
    mut env: JNIEnv,
    _: JClass,
    folder: JString,
    path: JString,
) -> jstring {
    guard_string(&mut env, |env| {
        let folder = text(env, &folder)?;
        let path = text(env, &path)?;
        Ok(list(&*engine(&folder)?, &path)?.to_string())
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_dev_tkfs_Native_stat(
    mut env: JNIEnv,
    _: JClass,
    folder: JString,
    path: JString,
) -> jstring {
    guard_string(&mut env, |env| {
        let folder = text(env, &folder)?;
        let path = text(env, &path)?;
        Ok(entry_json(&engine(&folder)?.lookup(&path)?).to_string())
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_dev_tkfs_Native_open(
    mut env: JNIEnv,
    _: JClass,
    folder: JString,
    path: JString,
    writable: jboolean,
) -> jlong {
    guard(&mut env, -1, |env| {
        let folder = text(env, &folder)?;
        let path = text(env, &path)?;
        Ok(engine(&folder)?.open_file(&path, false, false, writable != JNI_FALSE)? as jlong)
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_dev_tkfs_Native_openToReplace(
    mut env: JNIEnv,
    _: JClass,
    folder: JString,
    path: JString,
) -> jlong {
    guard(&mut env, -1, |env| {
        let folder = text(env, &folder)?;
        let path = text(env, &path)?;
        Ok(engine(&folder)?.open_to_replace(&path)? as jlong)
    })
}

/// Reads into `out`; hydrates missing chunks and blocks while it does, and asks for
/// the chunks after them before the reader gets there.
#[unsafe(no_mangle)]
pub extern "system" fn Java_dev_tkfs_Native_read(
    mut env: JNIEnv,
    _: JClass,
    folder: JString,
    token: jlong,
    offset: jlong,
    out: JByteArray,
    size: jint,
) -> jint {
    guard(&mut env, -1, |env| {
        let length = env.get_array_length(&out)?;
        ensure!(
            offset >= 0 && (0..=length).contains(&size),
            "INVALID_PARAMETER: a read of {size} bytes at {offset} into {length}"
        );
        let folder = text(env, &folder)?;
        let mut buffer = vec![0u8; size as usize];
        let count = engine(&folder)?.read_sequential(token as u64, offset as u64, &mut buffer)?;
        // SAFETY: i8 and u8 have the same layout; `count` is within `buffer`.
        let bytes = unsafe { std::slice::from_raw_parts(buffer.as_ptr().cast::<i8>(), count) };
        env.set_byte_array_region(&out, 0, bytes)?;
        Ok(count as jint)
    })
}

/// Writes from `data`. Writing part of a chunk that is not here hydrates it first, and fails with
/// nothing changed if it cannot.
#[unsafe(no_mangle)]
pub extern "system" fn Java_dev_tkfs_Native_write(
    mut env: JNIEnv,
    _: JClass,
    folder: JString,
    token: jlong,
    offset: jlong,
    data: JByteArray,
    size: jint,
    append: jboolean,
) -> jint {
    guard(&mut env, -1, |env| {
        let length = env.get_array_length(&data)?;
        ensure!(
            offset >= 0 && (0..=length).contains(&size),
            "INVALID_PARAMETER: a write of {size} bytes at {offset} from {length}"
        );
        let folder = text(env, &folder)?;
        let mut buffer = vec![0i8; size as usize];
        env.get_byte_array_region(&data, 0, &mut buffer)?;
        // SAFETY: i8 and u8 have the same layout, and this is all of `buffer`.
        let bytes =
            unsafe { std::slice::from_raw_parts(buffer.as_ptr().cast::<u8>(), buffer.len()) };
        let engine = engine(&folder)?;
        Ok(engine.write(
            token as u64,
            offset as u64,
            bytes,
            append != JNI_FALSE,
            false,
        )? as jint)
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_dev_tkfs_Native_length(
    mut env: JNIEnv,
    _: JClass,
    folder: JString,
    token: jlong,
) -> jlong {
    guard(&mut env, -1, |env| {
        let folder = text(env, &folder)?;
        Ok(engine(&folder)?.info(token as u64)?.1 as jlong)
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_dev_tkfs_Native_truncate(
    mut env: JNIEnv,
    _: JClass,
    folder: JString,
    token: jlong,
    size: jlong,
) {
    guard(&mut env, (), |env| {
        ensure!(size >= 0, "INVALID_PARAMETER: a truncate to {size}");
        let folder = text(env, &folder)?;
        engine(&folder)?.truncate(token as u64, size as u64, false)
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_dev_tkfs_Native_sync(
    mut env: JNIEnv,
    _: JClass,
    folder: JString,
    token: jlong,
) {
    guard(&mut env, (), |env| {
        let folder = text(env, &folder)?;
        engine(&folder)?.flush(Some(token as u64))
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_dev_tkfs_Native_create(
    mut env: JNIEnv,
    _: JClass,
    folder: JString,
    path: JString,
    directory: jboolean,
) {
    guard(&mut env, (), |env| {
        let folder = text(env, &folder)?;
        let path = text(env, &path)?;
        create(&*engine(&folder)?, &path, directory != JNI_FALSE)
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_dev_tkfs_Native_rename(
    mut env: JNIEnv,
    _: JClass,
    folder: JString,
    from: JString,
    to: JString,
) {
    guard(&mut env, (), |env| {
        let folder = text(env, &folder)?;
        let from = text(env, &from)?;
        let to = text(env, &to)?;
        rename(&*engine(&folder)?, &from, &to)
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_dev_tkfs_Native_delete(
    mut env: JNIEnv,
    _: JClass,
    folder: JString,
    path: JString,
) {
    guard(&mut env, (), |env| {
        let folder = text(env, &folder)?;
        let path = text(env, &path)?;
        delete(&*engine(&folder)?, &path)
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_dev_tkfs_Native_close(
    mut env: JNIEnv,
    _: JClass,
    folder: JString,
    token: jlong,
) {
    guard(&mut env, (), |env| {
        let folder = text(env, &folder)?;
        engine(&folder)?.close(token as u64)
    })
}

/// `[{entity, path}]`: what this phone made in the folder that no PC has confirmed yet.
#[unsafe(no_mangle)]
pub extern "system" fn Java_dev_tkfs_Native_waiting(
    mut env: JNIEnv,
    _: JClass,
    folder: JString,
) -> jstring {
    guard_string(&mut env, |env| {
        let folder = text(env, &folder)?;
        Ok(waiting(&folder)?.to_string())
    })
}

/// Asks the PCs, bounded, which of the waiting files they now hold, and answers `{waiting,
/// unsupported}`. Never says a file was sent on anything but a PC's own answer.
#[unsafe(no_mangle)]
pub extern "system" fn Java_dev_tkfs_Native_confirm(
    mut env: JNIEnv,
    _: JClass,
    folder: JString,
) -> jstring {
    guard_string(&mut env, |env| {
        let folder = text(env, &folder)?;
        Ok(confirm(&folder)?.to_string())
    })
}

/// For the end-to-end harness, through a debug build's shell control: the worst a guard must
/// survive, a panic with a Java exception pending, under Android's own JNI checks.
#[unsafe(no_mangle)]
pub extern "system" fn Java_dev_tkfs_Native_faultForTest(mut env: JNIEnv, _: JClass) {
    guard(&mut env, (), |env| {
        let _ = env.throw_new("java/lang/IllegalStateException", "left pending");
        panic!("faultForTest");
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use jni::{InitArgsBuilder, JNIVersion, JavaVM};
    use tkfs::model::content::{CHUNK_SIZE, ContentKind, Digest, FileContentRef};

    fn vm() -> &'static JavaVM {
        static VM: OnceLock<JavaVM> = OnceLock::new();
        VM.get_or_init(|| {
            let args = InitArgsBuilder::new()
                .version(JNIVersion::V8)
                .option("-Xcheck:jni")
                .build()
                .unwrap();
            JavaVM::new(args).unwrap()
        })
    }

    /// Calls `test` with a real `JNIEnv`.
    fn with_env(test: impl FnOnce(&mut JNIEnv)) {
        let mut attached = vm().attach_current_thread().unwrap();
        test(&mut attached);
    }

    fn thrown(env: &mut JNIEnv) -> String {
        take_pending(env).expect("nothing was thrown")
    }

    #[test]
    fn a_failure_is_an_ioexception_that_starts_with_its_code() {
        with_env(|env| {
            let value = guard(env, -1, |_| Err(anyhow!("WORKER_NOT_READY: gone")));
            assert_eq!(value, -1);
            assert_eq!(thrown(env), "java.io.IOException: WORKER_NOT_READY: gone");

            let value: i32 = guard(env, -1, |_| Err(anyhow!("no code here")));
            assert_eq!(value, -1);
            assert_eq!(
                thrown(env),
                "java.io.IOException: UNKNOWN_ERROR: no code here"
            );
        });
    }

    #[test]
    fn success_throws_nothing() {
        with_env(|env| {
            assert_eq!(guard(env, -1, |_| Ok(7)), 7);
            assert_eq!(take_pending(env), None);
        });
    }

    #[test]
    fn a_failure_with_an_exception_pending_ends_as_an_ioexception() {
        with_env(|env| {
            let value = guard(env, -1, |env| {
                env.throw_new("java/lang/IllegalStateException", "left pending")?;
                Err(anyhow!("WORKER_NOT_READY: gone"))
            });
            assert_eq!(value, -1);
            let pending = thrown(env);
            assert!(
                pending.starts_with("java.io.IOException: WORKER_NOT_READY"),
                "{pending}"
            );
            assert!(
                pending.contains("IllegalStateException: left pending"),
                "{pending}"
            );
        });
    }

    #[test]
    fn a_success_that_leaves_an_exception_pending_is_a_failure() {
        with_env(|env| {
            let value = guard(env, -1, |env| {
                env.throw_new("java/lang/IllegalStateException", "left pending")?;
                Ok(7)
            });
            assert_eq!(value, -1);
            let pending = thrown(env);
            assert!(
                pending.starts_with("java.io.IOException: UNKNOWN_ERROR"),
                "{pending}"
            );
        });
    }

    #[test]
    fn a_panic_is_an_ioexception_with_a_registered_code() {
        with_env(|env| {
            let value = guard(env, -1, |_| panic!("boom"));
            assert_eq!(value, -1);
            assert_eq!(thrown(env), "java.io.IOException: NATIVE_PANIC: boom");
            assert!(tkfs::model::errors::is_known("NATIVE_PANIC"));

            let number = 7;
            let value = guard(env, -1, |_| panic!("boom {number}"));
            assert_eq!(value, -1);
            assert_eq!(thrown(env), "java.io.IOException: NATIVE_PANIC: boom 7");
        });
    }

    #[test]
    fn a_panic_with_an_exception_pending_ends_as_an_ioexception() {
        with_env(|env| {
            let class = env.find_class("java/lang/Object").unwrap();
            Java_dev_tkfs_Native_faultForTest(unsafe { env.unsafe_clone() }, class);
            assert_eq!(
                thrown(env),
                "java.io.IOException: NATIVE_PANIC: faultForTest"
            );
        });
    }

    #[test]
    fn a_read_outside_its_buffer_is_refused_before_anything_else() {
        with_env(|env| {
            for (offset, size) in [(0, 9), (0, -1), (-1, 8), (i64::MIN, 0), (0, i32::MAX)] {
                let class = env.find_class("java/lang/Object").unwrap();
                let folder = env.new_string("F").unwrap();
                let out = env.new_byte_array(8).unwrap();
                let value = Java_dev_tkfs_Native_read(
                    unsafe { env.unsafe_clone() },
                    class,
                    folder,
                    1,
                    offset,
                    out,
                    size,
                );
                assert_eq!(value, -1);
                let pending = thrown(env);
                assert!(
                    pending.starts_with("java.io.IOException: INVALID_PARAMETER"),
                    "{pending}"
                );
            }
        });
    }

    #[test]
    fn a_read_of_the_whole_buffer_is_not_refused() {
        with_env(|env| {
            for size in [0, 8] {
                let class = env.find_class("java/lang/Object").unwrap();
                let folder = env.new_string("F").unwrap();
                let out = env.new_byte_array(8).unwrap();
                Java_dev_tkfs_Native_read(
                    unsafe { env.unsafe_clone() },
                    class,
                    folder,
                    1,
                    0,
                    out,
                    size,
                );
                // Past the check: this test process has no host.
                let pending = thrown(env);
                assert!(pending.contains("ORCHESTRATOR_NOT_READY"), "{pending}");
            }
        });
    }

    #[test]
    fn a_write_outside_its_buffer_is_refused_before_anything_else() {
        with_env(|env| {
            for (offset, size) in [(0, 9), (0, -1), (-1, 8), (i64::MIN, 0), (0, i32::MAX)] {
                let class = env.find_class("java/lang/Object").unwrap();
                let folder = env.new_string("F").unwrap();
                let data = env.new_byte_array(8).unwrap();
                let value = Java_dev_tkfs_Native_write(
                    unsafe { env.unsafe_clone() },
                    class,
                    folder,
                    1,
                    offset,
                    data,
                    size,
                    JNI_FALSE,
                );
                assert_eq!(value, -1);
                let pending = thrown(env);
                assert!(
                    pending.starts_with("java.io.IOException: INVALID_PARAMETER"),
                    "{pending}"
                );
            }
        });
    }

    #[test]
    fn a_write_of_the_whole_buffer_is_not_refused() {
        with_env(|env| {
            for size in [0, 8] {
                let class = env.find_class("java/lang/Object").unwrap();
                let folder = env.new_string("F").unwrap();
                let data = env.new_byte_array(8).unwrap();
                Java_dev_tkfs_Native_write(
                    unsafe { env.unsafe_clone() },
                    class,
                    folder,
                    1,
                    0,
                    data,
                    size,
                    JNI_FALSE,
                );
                // Past the check: this test process has no host.
                let pending = thrown(env);
                assert!(pending.contains("ORCHESTRATOR_NOT_READY"), "{pending}");
            }
        });
    }

    #[test]
    fn a_negative_truncate_is_refused_before_anything_else() {
        with_env(|env| {
            let class = env.find_class("java/lang/Object").unwrap();
            let folder = env.new_string("F").unwrap();
            Java_dev_tkfs_Native_truncate(unsafe { env.unsafe_clone() }, class, folder, 1, -1);
            let pending = thrown(env);
            assert!(
                pending.starts_with("java.io.IOException: INVALID_PARAMETER"),
                "{pending}"
            );
        });
    }

    /// An engine of its own, with no fetcher, so a call that needs a file's contents fails.
    fn test_engine() -> (tempfile::TempDir, Arc<Engine>) {
        let home = tempfile::tempdir().unwrap();
        let engine = Engine::open(home.path(), None).unwrap();
        (home, engine)
    }

    /// A large file the phone knows by name and never downloaded, not even its manifest.
    fn never_downloaded(engine: &Engine, path: &str) {
        let mut store = engine.store.lock().unwrap();
        let mut state = store.create(path, EntryKind::File).unwrap();
        let parent = store.projection().visible[&state.id].clone();
        state.content = Some(
            FileContentRef::new(
                ContentKind::Manifest,
                Digest::bytes(b"never here"),
                4 * CHUNK_SIZE,
            )
            .unwrap(),
        );
        let branch = store.active.clone();
        let event = store.event(&branch, state, vec![parent], tkfs::core::Operation::Content);
        store.apply(&[event]).unwrap();
    }

    fn names(engine: &Engine, path: &str) -> Vec<String> {
        let mut names: Vec<_> = list(engine, path)
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .map(|entry| entry["name"].as_str().unwrap().to_owned())
            .collect();
        names.sort();
        names
    }

    fn code(error: anyhow::Error) -> String {
        ErrorWire::from_error(&error).code
    }

    #[test]
    fn a_namespace_change_is_durable_when_its_call_returns() {
        let (_home, engine) = test_engine();
        let durable = |engine: &Engine| engine.store.lock().unwrap().metadata_is_durable();
        create(&engine, "/a b%#:é", true).unwrap();
        assert!(durable(&engine));
        create(&engine, "/a b%#:é/f.txt", false).unwrap();
        assert!(durable(&engine));
        rename(&engine, "/a b%#:é/f.txt", "/g.txt").unwrap();
        assert!(durable(&engine));
        delete(&engine, "/g.txt").unwrap();
        assert!(durable(&engine));
        engine.shutdown().unwrap();
    }

    #[test]
    fn create_refuses_a_name_that_is_taken_whatever_its_case() {
        let (_home, engine) = test_engine();
        create(&engine, "/Notes.txt", false).unwrap();
        for path in ["/Notes.txt", "/NOTES.TXT"] {
            assert_eq!(
                code(create(&engine, path, false).unwrap_err()),
                "ALREADY_EXISTS"
            );
        }
        engine.shutdown().unwrap();
    }

    #[test]
    fn a_file_never_downloaded_renames_moves_and_deletes_with_no_one_to_ask() {
        let (_home, engine) = test_engine();
        never_downloaded(&engine, "/big.bin");
        create(&engine, "/sub", true).unwrap();
        rename(&engine, "/big.bin", "/renamed.bin").unwrap();
        rename(&engine, "/renamed.bin", "/sub/renamed.bin").unwrap();
        assert_eq!(names(&engine, "/"), ["sub"]);
        assert_eq!(names(&engine, "/sub"), ["renamed.bin"]);
        delete(&engine, "/sub/renamed.bin").unwrap();
        assert!(names(&engine, "/sub").is_empty());
        engine.shutdown().unwrap();
    }

    #[test]
    fn deleting_a_directory_takes_what_it_holds() {
        let (_home, engine) = test_engine();
        create(&engine, "/d", true).unwrap();
        create(&engine, "/d/e", true).unwrap();
        create(&engine, "/d/e/f.txt", false).unwrap();
        never_downloaded(&engine, "/d/big.bin");
        create(&engine, "/keep.txt", false).unwrap();
        delete(&engine, "/d").unwrap();
        assert_eq!(names(&engine, "/"), ["keep.txt"]);
        engine.shutdown().unwrap();
    }

    #[test]
    fn the_root_is_not_deleted_and_nothing_in_it_goes_on_the_way() {
        let (_home, engine) = test_engine();
        create(&engine, "/keep.txt", false).unwrap();
        assert_eq!(code(delete(&engine, "/").unwrap_err()), "ROOT_IMMUTABLE");
        assert_eq!(names(&engine, "/"), ["keep.txt"]);
        assert_eq!(
            code(rename(&engine, "/", "/elsewhere").unwrap_err()),
            "ROOT_IMMUTABLE"
        );
        engine.shutdown().unwrap();
    }

    #[test]
    fn a_rename_onto_a_name_that_is_taken_changes_nothing() {
        let (_home, engine) = test_engine();
        create(&engine, "/a", false).unwrap();
        create(&engine, "/b", false).unwrap();
        assert_eq!(
            code(rename(&engine, "/a", "/B").unwrap_err()),
            "ALREADY_EXISTS"
        );
        assert_eq!(names(&engine, "/"), ["a", "b"]);
        engine.shutdown().unwrap();
    }

    #[test]
    fn a_string_entry_point_returns_null_and_throws_with_no_host() {
        with_env(|env| {
            let class = env.find_class("java/lang/Object").unwrap();
            let action = env.new_string("status").unwrap();
            let payload = env.new_string("{}").unwrap();
            let value =
                Java_dev_tkfs_Native_manage(unsafe { env.unsafe_clone() }, class, action, payload);
            assert!(value.is_null());
            assert_eq!(thrown(env), "java.io.IOException: ORCHESTRATOR_NOT_READY");
        });
    }

    #[test]
    fn waiting_and_confirm_fail_through_the_guard_with_no_host() {
        with_env(|env| {
            for entry in [Java_dev_tkfs_Native_waiting, Java_dev_tkfs_Native_confirm] {
                let class = env.find_class("java/lang/Object").unwrap();
                let folder = env.new_string("F").unwrap();
                let value = entry(unsafe { env.unsafe_clone() }, class, folder);
                assert!(value.is_null());
                assert_eq!(thrown(env), "java.io.IOException: ORCHESTRATOR_NOT_READY");
            }
        });
    }

    #[test]
    fn a_payload_that_is_not_json_is_a_failure_not_a_crash() {
        with_env(|env| {
            let class = env.find_class("java/lang/Object").unwrap();
            let action = env.new_string("status").unwrap();
            let payload = env.new_string("{not json").unwrap();
            let value =
                Java_dev_tkfs_Native_manage(unsafe { env.unsafe_clone() }, class, action, payload);
            assert!(value.is_null());
            let pending = thrown(env);
            assert!(
                pending.starts_with("java.io.IOException: UNKNOWN_ERROR"),
                "{pending}"
            );
        });
    }
}
