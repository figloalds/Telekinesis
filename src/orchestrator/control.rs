//! Token-authenticated local control. CLI and desktop use the same operations.
use super::catalog::Catalog;
use crate::{content::working::lock, core::EntryKind, runtime::Engine, ws};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::{
    net::TcpListener,
    path::Path,
    sync::{Arc, atomic::Ordering},
};

pub fn request(root: &Path, operation: &str, payload: Value) -> Result<Value> {
    let info: Value = serde_json::from_slice(&std::fs::read(root.join("control.json"))?)?;
    let mut socket = ws::connect_authorized(
        info["endpoint"].as_str().context("INVALID_CONTROL")?,
        info["token"].as_str(),
    )?;
    ws::call(
        &mut socket,
        &json!({"operation":operation,"payload":payload}),
    )
}
pub fn start(engine: Arc<Engine>, catalog: Arc<Catalog>) -> Result<String> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    listener.set_nonblocking(true)?;
    let endpoint = format!("ws://{}", listener.local_addr()?);
    let token = hex::encode(crate::session::auth::new_secret());
    let root = lock(&engine.store).root.clone();
    std::fs::write(
        root.join("control.json"),
        serde_json::to_vec(&json!({"endpoint":endpoint,"token":token}))?,
    )?;
    std::thread::spawn(move || {
        while !engine.stopping.load(Ordering::Acquire) {
            match listener.accept() {
                Ok((stream, _)) => {
                    let e = engine.clone();
                    let c = catalog.clone();
                    let t = token.clone();
                    std::thread::spawn(move || {
                        let result = (|| -> Result<()> {
                            let mut socket = ws::accept_authorized(stream, Some(&t))?;
                            loop {
                                let req: Value = ws::receive(&mut socket)?;
                                let reply = match execute(
                                    &e,
                                    &c,
                                    req["operation"].as_str().context("OPERATION_REQUIRED")?,
                                    &req["payload"],
                                ) {
                                    Ok(value) => json!({"ok":true,"result":value}),
                                    Err(error) => json!({"ok":false,"error":format!("{error:#}")}),
                                };
                                ws::send(&mut socket, &reply)?;
                                if e.stopping.load(Ordering::Acquire) {
                                    return Ok(());
                                }
                            }
                        })();
                        if let Err(error) = result
                            && !error.to_string().contains("Connection closed")
                        {
                            eprintln!("control: {error:#}");
                        }
                    });
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(std::time::Duration::from_millis(20))
                }
                Err(_) => break,
            }
        }
    });
    Ok(endpoint)
}
pub fn execute(engine: &Engine, catalog: &Catalog, operation: &str, p: &Value) -> Result<Value> {
    let text = |key: &str| -> Result<&str> { p[key].as_str().context("ARGUMENT_REQUIRED") };
    match operation {
        #[cfg(windows)]
        "mount-stats" => Ok(serde_json::to_value(crate::mount::stats())?),
        "status" => {
            let endpoint = catalog.get("advertise")?;
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
            catalog.set("label", text("label")?)?;
            engine.flush(None)?;
            Ok(json!({}))
        }
        "peer-info" => catalog.inspect_peer(text("endpoint")?),
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
            let status = execute(engine, catalog, "status", &json!({}))?;
            let runtime = lock(&engine.peer_status).clone();
            let peers: Vec<_> = catalog
                .peers()?
                .into_iter()
                .map(|peer| json!({"endpoint":peer.endpoint,"installation":peer.installation,"runtime":runtime.get(&peer.endpoint)}))
                .collect();
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
        "events" => Ok(serde_json::to_value(
            lock(&engine.store).events_after(p["after"].as_u64().unwrap_or(0), 4096)?,
        )?),
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
                ensure!(len <= 16 * 1024 * 1024, "USE_EXPORT_FOR_LARGE_FILES");
                let mut bytes = vec![0; len as usize];
                engine.read(h, 0, &mut bytes)?;
                Ok(json!({"bytes":bytes}))
            })();
            let closed = engine.close(h);
            let result = result?;
            closed?;
            Ok(result)
        }
        "pairing-code" => Ok(json!({"code":catalog.reset_code()?})),
        "pair" => Ok(json!({"installation":catalog.pair(text("endpoint")?,text("code")?)?})),
        "peers" => Ok(Value::Array(
            catalog
                .peers()?
                .into_iter()
                .map(|p| json!({"endpoint":p.endpoint,"installation":p.installation}))
                .collect(),
        )),
        "unpair" => {
            catalog.unpair(text("installation")?)?;
            Ok(json!({}))
        }
        "gc" => Ok(json!({"removed":engine.collect()?})),
        "flush" => {
            engine.flush(None)?;
            Ok(json!({}))
        }
        "shutdown" => {
            // A failed final seal is a reported durability failure; it never
            // keeps the daemon (and its mount) alive.
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
