//! The Android build of tkfs: the orchestrator and its workers on threads of the app
//! process, with a small JNI surface for the app and its DocumentsProvider.
use anyhow::{Result, anyhow};
use jni::{
    JNIEnv,
    objects::{JByteArray, JClass, JString},
    sys::{JNI_FALSE, jboolean, jint, jlong, jstring},
};
use serde_json::{Value, json};
use std::{
    path::PathBuf,
    sync::{Arc, OnceLock},
};
use tkfs::{core::EntryKind, orchestrator::embedded::Host, runtime::Engine};

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
pub extern "system" fn Java_dev_tkfs_Native_start(
    mut env: JNIEnv,
    _: JClass,
    config: JString,
) {
    let result = text(&mut env, &config).map(|config| start(PathBuf::from(config)));
    or_throw(&mut env, result, ());
}

/// One `manage` action, in-process: the same actions the CLI sends.
#[unsafe(no_mangle)]
pub extern "system" fn Java_dev_tkfs_Native_manage(
    mut env: JNIEnv,
    _: JClass,
    action: JString,
    payload: JString,
) -> jstring {
    let result = (|| {
        let action = text(&mut env, &action)?;
        let payload: Value = serde_json::from_str(&text(&mut env, &payload)?)?;
        let reply = host()?.daemon()?.execute(&action, &payload)?;
        Ok(reply.to_string())
    })();
    string_or_throw(&mut env, result)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_dev_tkfs_Native_list(
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
pub extern "system" fn Java_dev_tkfs_Native_stat(
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
pub extern "system" fn Java_dev_tkfs_Native_open(
    mut env: JNIEnv,
    _: JClass,
    folder: JString,
    path: JString,
    writable: jboolean,
) -> jlong {
    let result = (|| {
        let folder = text(&mut env, &folder)?;
        let path = text(&mut env, &path)?;
        Ok(engine(&folder)?.open_file(&path, false, false, writable != JNI_FALSE)? as jlong)
    })();
    or_throw(&mut env, result, -1)
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
pub extern "system" fn Java_dev_tkfs_Native_close(
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
