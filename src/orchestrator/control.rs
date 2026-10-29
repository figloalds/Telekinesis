//! Worker control operations. Local clients (CLI, desktop, orchestrator) call
//! them over the worker's token-authenticated loopback session.
//!
//! Reply convention: success is header `{}` with a JSON body; failure is an
//! error reply whose code is the message's leading `SCREAMING_CASE` token
//! (before `:`), or `WORKER_ERROR`.
use super::{
    demand::{Demand, Seq},
    settings::Settings,
    worker::{RUNTIME_FILE, TOKEN_FILE, WATCH_MAX_WAIT_MS},
};
use crate::{
    content::working::lock,
    core::EntryKind,
    model::{errors::ErrorWire, protocol::LaneRole},
    runtime::Engine,
    session::{self, ConnOptions, Priority},
};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    time::Duration,
};

/// Deadline of one [`request`].
pub const REQUEST_DEADLINE: Duration = Duration::from_secs(10);
/// Deadline of a [`request`] for an op that moves file bytes or seals.
pub const LONG_REQUEST_DEADLINE: Duration = Duration::from_secs(60);

/// Largest file `cat` returns. Its reply is a JSON number array (up to 4
/// characters per byte), which must fit the 16 MiB `cat` body bound.
pub const CAT_MAX_BYTES: u64 = 3 * 1024 * 1024;
/// The most events `events {last}` returns.
pub const EVENTS_LAST_MAX: u64 = 1024;

/// The [`request`] deadline of `operation`.
pub fn request_deadline(operation: &str) -> Duration {
    if matches!(
        operation,
        "import" | "cat" | "gc" | "checkpoint" | "flush" | "shutdown"
    ) {
        LONG_REQUEST_DEADLINE
    } else {
        REQUEST_DEADLINE
    }
}

/// Calls `operation` on the worker serving the store at `root`: reads
/// `runtime.json` and `runtime.token`, dials the loopback session and parses
/// the reply body as JSON. No `runtime.json` means `WORKER_NOT_RUNNING`.
pub fn request(root: &Path, operation: &str, payload: Value) -> Result<Value> {
    let runtime: Value = match std::fs::read(root.join(RUNTIME_FILE)) {
        Ok(bytes) => serde_json::from_slice(&bytes).context("INVALID_RUNTIME_RECORD")?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            anyhow::bail!("WORKER_NOT_RUNNING: no worker serves {}", root.display())
        }
        Err(error) => return Err(error).context("INVALID_RUNTIME_RECORD"),
    };
    let address = runtime["address"]
        .as_str()
        .context("INVALID_RUNTIME_RECORD: address missing")?;
    let token = std::fs::read_to_string(root.join(TOKEN_FILE))
        .context("WORKER_NOT_RUNNING: runtime.token unreadable")?;
    let token = token.trim();
    crate::util::log::register_secret(token);
    let conn = session::dial_loopback(
        address,
        Some(token),
        ConnOptions::for_role(LaneRole::Control),
    )
    .map_err(|error| anyhow::anyhow!("{error}"))?;
    let reply = conn.call(
        operation,
        payload,
        Vec::new(),
        Priority::Control,
        request_deadline(operation),
    );
    conn.close();
    let reply = reply.map_err(|error| anyhow::anyhow!("{error}"))?;
    if reply.body.is_empty() {
        return Ok(Value::Null);
    }
    Ok(serde_json::from_slice(&reply.body)?)
}

/// The error reply for a failed op (see the module's reply convention).
pub fn error_wire(error: &anyhow::Error) -> ErrorWire {
    let message = format!("{error:#}");
    match leading_code(&message) {
        Some(code) => ErrorWire::new(code, message.clone()),
        None => ErrorWire::coded("WORKER_ERROR", message),
    }
}

fn leading_code(message: &str) -> Option<&str> {
    let head = message.split(':').next()?.trim();
    let mut bytes = head.bytes();
    (bytes.next().is_some_and(|b| b.is_ascii_uppercase())
        && bytes.all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_'))
    .then_some(head)
}

/// Paths this process serves as a filesystem mount.
pub fn mounts() -> Vec<PathBuf> {
    #[cfg(any(windows, target_os = "linux"))]
    return crate::mount::mounted_path().into_iter().collect();
    #[cfg(not(any(windows, target_os = "linux")))]
    Vec::new()
}

/// `hello` → `{repo, device, instance, pid, version, ipc, mounts}`.
pub fn hello(engine: &Engine, instance: &str) -> Value {
    let (repo, device) = {
        let store = lock(&engine.store);
        (store.repo.clone(), store.device.clone())
    };
    json!({
        "repo": repo,
        "device": device,
        "instance": instance,
        "pid": std::process::id(),
        "version": env!("CARGO_PKG_VERSION"),
        "ipc": crate::model::protocol::IPC_VERSION,
        "mounts": mounts(),
    })
}

/// The worker's health summary carried by every `watch` reply.
pub fn heartbeat(engine: &Engine) -> Result<Value> {
    let writable = engine.writable_tokens().len();
    let error = lock(&engine.error).clone();
    let store = lock(&engine.store);
    Ok(json!({
        "at_ms": crate::util::time::now_ms(),
        "entries": store.projection().entries.len(),
        "conflicts": store.conflicts(false)?.len(),
        "writable_handles": writable,
        "error": error,
    }))
}

/// `watch {after_seq, wait_ms}` → `{seq, heartbeat, demand}`. Returns at once
/// when `seq > after_seq` or objects are wanted; otherwise waits at most
/// `min(wait_ms, WATCH_MAX_WAIT_MS)` for the next wake-up.
pub fn watch(engine: &Engine, seq: &Seq, demand: &Demand, p: &Value) -> Result<Value> {
    let after = p["after_seq"].as_u64().unwrap_or(0);
    let wait = p["wait_ms"].as_u64().unwrap_or(0).min(WATCH_MAX_WAIT_MS);
    let current = seq.current();
    let seq = if current > after || !demand.pending().is_empty() {
        current
    } else {
        seq.wait_past(after, Duration::from_millis(wait))
    };
    Ok(json!({
        "seq": seq,
        "heartbeat": heartbeat(engine)?,
        "demand": demand.pending(),
    }))
}

/// The worker's engine and filesystem ops: everything but `hello`, `watch`
/// and the sync ops of [`super::sync::Service`].
pub fn execute(engine: &Engine, settings: &Settings, operation: &str, p: &Value) -> Result<Value> {
    let text = |key: &str| -> Result<&str> { p[key].as_str().context("ARGUMENT_REQUIRED") };
    match operation {
        #[cfg(windows)]
        "mount-stats" => Ok(serde_json::to_value(crate::mount::stats())?),
        "status" => {
            // Workers never listen for peers; the orchestrator owns the network.
            let endpoint: Option<String> = None;
            #[cfg(any(windows, target_os = "linux"))]
            let mount = crate::mount::mounted_path();
            #[cfg(not(any(windows, target_os = "linux")))]
            let mount: Option<std::path::PathBuf> = None;
            let s = lock(&engine.store);
            Ok(
                json!({"pid":std::process::id(),"endpoint":endpoint,"mount":mount,"device":s.device,"repo":s.repo,"branch":s.active,"entries":s.projection().entries.len(),"conflicts":s.conflicts(false)?.len(),"error":*lock(&engine.error)}),
            )
        }
        "set-label" => {
            settings.set("label", text("label")?)?;
            engine.flush(None)?;
            Ok(json!({}))
        }
        "preview-version" => {
            let event = {
                lock(&engine.store)
                    .version(text("version")?)?
                    .context("VERSION_NOT_FOUND")?
            };
            let mut preview = format!(
                "{} · {:?}\nVersion: {}\nPeer: {}\n",
                event.state.name, event.operation, event.id, event.author
            );
            if let Some(reference) = &event.state.content {
                let content = engine.working.content();
                let _pin = content.pin(reference)?;
                let mut bytes = vec![0; reference.total_len.min(65536) as usize];
                content.read_range(reference, 0, &mut bytes)?;
                preview.push_str(&format!("Length: {} bytes\n\n", reference.total_len));
                if bytes.contains(&0) {
                    preview.push_str("Binary content");
                } else {
                    preview.push_str(&String::from_utf8_lossy(&bytes));
                }
                if reference.total_len > bytes.len() as u64 {
                    preview.push_str("\n\n[Preview limited to the first 64 KiB]");
                }
            }
            Ok(json!({"preview":preview}))
        }
        "desktop-snapshot" => {
            let status = execute(engine, settings, "status", &json!({}))?;
            // Peers belong to the orchestrator; the desktop becomes its client.
            let peers: Vec<Value> = Vec::new();
            let store = lock(&engine.store);
            let branches = store
                .db
                .prepare("SELECT id,name FROM branches ORDER BY name")?
                .query_map([], |row| {
                    Ok(json!({"id":row.get::<_,String>(0)?,"name":row.get::<_,String>(1)?}))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let snapshots = store
                .db
                .prepare("SELECT id FROM snapshots ORDER BY rowid DESC LIMIT 64")?
                .query_map([], |row| row.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let last: u64 =
                store
                    .db
                    .query_row("SELECT coalesce(max(seq),0) FROM events", [], |row| {
                        row.get(0)
                    })?;
            Ok(
                json!({"status":status,"peers":peers,"branches":branches,"snapshots":snapshots,
                "conflicts":store.conflicts(true)?,"events":store.events_after(last.saturating_sub(128),128)?}),
            )
        }
        "tree" => Ok(serde_json::to_value(
            &*lock(&engine.store).projection().entries,
        )?),
        "lookup" => {
            let entry = engine.lookup(text("path")?)?;
            let applied = lock(&engine.last_applied)
                .get(&entry.id)
                .copied()
                .unwrap_or(0);
            Ok(json!({"entry":entry,"applied_ns":applied.to_string()}))
        }
        // `{after}`: at most 4096 events after `after`, oldest first.
        // `{last: N}`: the newest `min(N, EVENTS_LAST_MAX)`, oldest first.
        "events" => {
            let store = lock(&engine.store);
            let events = match &p["last"] {
                Value::Null => store.events_after(p["after"].as_u64().unwrap_or(0), 4096)?,
                last => {
                    let count = last
                        .as_u64()
                        .context("INVALID_REQUEST: last must be a number")?
                        .min(EVENTS_LAST_MAX);
                    let newest: u64 = store.db.query_row(
                        "SELECT coalesce(max(seq),0) FROM events",
                        [],
                        |row| row.get(0),
                    )?;
                    store.events_after(newest.saturating_sub(count), count as usize)?
                }
            };
            Ok(serde_json::to_value(events)?)
        }
        "conflicts" => Ok(serde_json::to_value(
            lock(&engine.store).conflicts(p["history"].as_bool().unwrap_or(false))?,
        )?),
        "resolve" => {
            let event = engine.resolve(text("entity")?, text("version")?)?;
            Ok(serde_json::to_value(event)?)
        }
        "checkpoint" => {
            engine.flush(None)?;
            Ok(serde_json::to_value(lock(&engine.store).checkpoint()?)?)
        }
        "fork" => {
            engine.flush(None)?;
            let mut s = lock(&engine.store);
            let snapshot = if let Some(snapshot) = p["snapshot"].as_str() {
                s.snapshot(snapshot)?
            } else {
                s.checkpoint()?
            };
            Ok(json!({"branch":s.fork(&snapshot,text("name")?)?}))
        }
        "checkout" => {
            engine.flush(None)?;
            let paths = {
                let mut store = lock(&engine.store);
                let paths = |s: &crate::core::Store| {
                    s.projection()
                        .entries
                        .keys()
                        .filter_map(|id| s.projection().path(id).ok())
                        .collect::<Vec<_>>()
                };
                let mut changed = paths(&store);
                store.checkout(text("branch")?)?;
                changed.extend(paths(&store));
                changed
            };
            engine.invalidate_paths(paths)?;
            Ok(json!({"checked_out":true}))
        }
        "mkdir" => {
            let entry = lock(&engine.store).create(text("path")?, EntryKind::Directory)?;
            engine.invalidate_paths([format!("/{}", text("path")?.trim_start_matches('/'))])?;
            Ok(serde_json::to_value(entry)?)
        }
        "rename" => {
            let mut s = lock(&engine.store);
            let entity = s.lookup(text("path")?)?.id;
            s.rename(
                &entity,
                text("target")?,
                p["replace"].as_bool().unwrap_or(false),
            )?;
            drop(s);
            engine.invalidate_paths([
                format!("/{}", text("path")?.trim_start_matches('/')),
                format!("/{}", text("target")?.trim_start_matches('/')),
            ])?;
            Ok(json!({}))
        }
        "delete" => {
            let mut s = lock(&engine.store);
            let entity = s.lookup(text("path")?)?.id;
            s.delete(&entity)?;
            drop(s);
            engine.invalidate_paths([format!("/{}", text("path")?.trim_start_matches('/'))])?;
            Ok(json!({}))
        }
        "import" => {
            let path = text("path")?;
            let mut input = std::fs::File::open(text("source")?)?;
            ensure!(input.metadata()?.is_file(), "IMPORT_SOURCE_NOT_A_FILE");
            let h = engine.open_file(path, engine.lookup(path).is_err(), false, true)?;
            let result = (|| -> Result<()> {
                engine.truncate(h, 0, false)?;
                let mut bytes = vec![0; 1024 * 1024];
                let mut offset = 0;
                loop {
                    use std::io::Read;
                    let count = input.read(&mut bytes)?;
                    if count == 0 {
                        break;
                    }
                    engine.write(h, offset, &bytes[..count], false, false)?;
                    offset += count as u64;
                }
                engine.flush(Some(h))
            })();
            let closed = engine.close(h);
            result?;
            closed?;
            engine.invalidate_paths([format!("/{}", path.trim_start_matches('/'))])?;
            Ok(json!({}))
        }
        "cat" => {
            let h = engine.open_file(text("path")?, false, false, false)?;
            let result = (|| -> Result<Value> {
                let (_, len) = engine.info(h)?;
                ensure!(
                    len <= CAT_MAX_BYTES,
                    "USE_EXPORT_FOR_LARGE_FILES: {len} bytes exceed the {CAT_MAX_BYTES}-byte cat limit"
                );
                let mut bytes = vec![0; len as usize];
                engine.read(h, 0, &mut bytes)?;
                Ok(json!({"bytes":bytes}))
            })();
            let closed = engine.close(h);
            let result = result?;
            closed?;
            Ok(result)
        }
        "gc" => Ok(json!({"removed":engine.collect()?})),
        "flush" => {
            engine.flush(None)?;
            Ok(json!({}))
        }
        "shutdown" => {
            if engine.take_fault("shutdown.hang") {
                std::thread::sleep(Duration::from_secs(60));
            }
            let open = engine.writable_tokens().len();
            ensure!(
                p["force"].as_bool().unwrap_or(false) || open == 0,
                "BUSY_VIEW: {open} files open"
            );
            // A failed final seal is a reported durability failure; it never
            // keeps the worker (and its mount) alive.
            engine.request_shutdown()?;
            Ok(json!({"stopping":true}))
        }
        "inject-fault" => {
            engine.inject_fault(text("name")?);
            Ok(json!({}))
        }
        _ => anyhow::bail!("UNSUPPORTED_OPERATION"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    #[test]
    fn shutdown_without_force_is_refused_while_a_writable_handle_is_open() -> Result<()> {
        let root = tempfile::tempdir()?;
        let engine = Engine::open(root.path(), None)?;
        let settings = Settings {
            engine: engine.clone(),
        };
        let h = engine.open_file("f", true, false, true)?;
        let error = execute(&engine, &settings, "shutdown", &json!({"force": false}))
            .expect_err("shutdown with a writable handle open");
        assert!(
            error.to_string().starts_with("BUSY_VIEW: 1 files open"),
            "{error:#}"
        );
        assert_eq!(error_wire(&error).code, "BUSY_VIEW");
        assert!(!engine.stopping.load(Ordering::Acquire));
        // Absent `force` is `false`.
        assert!(execute(&engine, &settings, "shutdown", &json!({})).is_err());
        assert!(!engine.stopping.load(Ordering::Acquire));
        assert_eq!(
            execute(&engine, &settings, "shutdown", &json!({"force": true}))?,
            json!({"stopping": true})
        );
        assert!(engine.stopping.load(Ordering::Acquire));
        engine.close(h)?;
        engine.shutdown()?;
        Ok(())
    }

    #[test]
    fn cat_refuses_files_its_reply_cannot_carry_and_serves_smaller_ones() -> Result<()> {
        let root = tempfile::tempdir()?;
        let engine = Engine::open(root.path(), None)?;
        let settings = Settings {
            engine: engine.clone(),
        };
        for (path, len) in [("large", 4 * 1024 * 1024), ("small", 2 * 1024 * 1024)] {
            let h = engine.open_file(path, true, false, true)?;
            // 0xFF is the widest byte in a JSON number array ("255,").
            engine.write(h, 0, &vec![0xFF; len], false, false)?;
            engine.flush(Some(h))?;
            engine.close(h)?;
        }
        let error = execute(&engine, &settings, "cat", &json!({"path":"large"})).unwrap_err();
        assert_eq!(
            error_wire(&error).code,
            "USE_EXPORT_FOR_LARGE_FILES",
            "{error:#}"
        );
        let reply = execute(&engine, &settings, "cat", &json!({"path":"small"}))?;
        assert_eq!(reply["bytes"].as_array().unwrap().len(), 2 * 1024 * 1024);
        assert!(
            serde_json::to_vec(&reply)?.len() <= crate::model::protocol::max_body("cat"),
            "a 2 MiB cat reply must fit the cat body bound"
        );
        // Even the largest accepted file fits.
        assert!(CAT_MAX_BYTES * 4 + 64 <= crate::model::protocol::max_body("cat") as u64);
        assert!(engine.writable_tokens().is_empty());
        engine.shutdown()?;
        Ok(())
    }

    #[test]
    fn events_last_returns_the_newest_in_order_and_after_is_unchanged() -> Result<()> {
        let root = tempfile::tempdir()?;
        let engine = Engine::open(root.path(), None)?;
        let settings = Settings {
            engine: engine.clone(),
        };
        for name in ["a", "b", "c", "d"] {
            execute(&engine, &settings, "mkdir", &json!({"path": name}))?;
        }
        let seqs = |reply: &Value| -> Vec<u64> {
            reply
                .as_array()
                .unwrap()
                .iter()
                .map(|row| row[0].as_u64().unwrap())
                .collect()
        };
        let all = seqs(&execute(&engine, &settings, "events", &json!({}))?);
        assert!(all.len() >= 4, "{all:?}");
        assert!(all.windows(2).all(|w| w[0] < w[1]), "{all:?}");
        let newest = &all[all.len() - 2..];
        let last = execute(&engine, &settings, "events", &json!({"last": 2}))?;
        assert_eq!(seqs(&last), newest);
        assert_eq!(last.as_array().unwrap()[1][1]["state"]["name"], "d");
        // `after` reads forward from a cursor as before.
        let after = execute(&engine, &settings, "events", &json!({"after": all[1]}))?;
        assert_eq!(seqs(&after), &all[2..]);
        // A large `last` is bounded and returns everything there is.
        let many = execute(&engine, &settings, "events", &json!({"last": 1_000_000}))?;
        assert_eq!(seqs(&many), all);
        assert_eq!(
            seqs(&execute(&engine, &settings, "events", &json!({"last": 0}))?),
            Vec::<u64>::new()
        );
        let error = execute(&engine, &settings, "events", &json!({"last": "x"})).unwrap_err();
        assert!(
            error.to_string().starts_with("INVALID_REQUEST"),
            "{error:#}"
        );
        engine.shutdown()?;
        Ok(())
    }

    #[test]
    fn byte_moving_and_sealing_ops_get_the_long_deadline() {
        for op in ["import", "cat", "gc", "checkpoint", "flush", "shutdown"] {
            assert_eq!(request_deadline(op), LONG_REQUEST_DEADLINE, "{op}");
        }
        assert_eq!(request_deadline("hello"), REQUEST_DEADLINE);
        assert_eq!(request_deadline("watch"), REQUEST_DEADLINE);
    }

    #[test]
    fn error_replies_carry_the_leading_code_or_worker_error() {
        let coded = error_wire(&anyhow::anyhow!("VERSION_NOT_FOUND"));
        assert_eq!(coded.code, "VERSION_NOT_FOUND");
        assert_eq!(coded.message, "VERSION_NOT_FOUND");
        let detailed = error_wire(&anyhow::anyhow!("BUSY_VIEW: 2 files open"));
        assert_eq!(detailed.code, "BUSY_VIEW");
        assert_eq!(detailed.message, "BUSY_VIEW: 2 files open");
        let plain = error_wire(&anyhow::anyhow!("No such file: x"));
        assert_eq!(plain.code, "WORKER_ERROR");
        assert_eq!(plain.message, "WORKER_ERROR: No such file: x");
    }
}
