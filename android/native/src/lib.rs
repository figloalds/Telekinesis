//! The Android build of tkfs: the orchestrator and its workers on threads of the app
//! process, with a small JNI surface for the app and its DocumentsProvider.
use anyhow::{Result, anyhow, ensure};
use jni::{
    JNIEnv,
    objects::{JByteArray, JClass, JString},
    sys::{JNI_FALSE, jboolean, jint, jlong, jstring},
};
use serde_json::{Value, json};
use std::{
    any::Any,
    panic::{AssertUnwindSafe, catch_unwind},
    path::PathBuf,
    sync::{Arc, Once, OnceLock},
};
use tkfs::{
    core::EntryKind,
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
        Ok(list(&folder, &path)?.to_string())
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

/// Reads into `out`; hydrates missing chunks and blocks while it does.
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
        let count = engine(&folder)?.read(token as u64, offset as u64, &mut buffer)?;
        // SAFETY: i8 and u8 have the same layout; `count` is within `buffer`.
        let bytes = unsafe { std::slice::from_raw_parts(buffer.as_ptr().cast::<i8>(), count) };
        env.set_byte_array_region(&out, 0, bytes)?;
        Ok(count as jint)
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
