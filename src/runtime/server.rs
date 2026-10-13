//! Loopback RPC listener and per-request dispatch for one worker.
use super::{
    Discovery, Shared, content::fetch_verified, export::export_bucket,
    notify::deliver_notifications, published::published_rpc,
};
use crate::core::*;
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::io::Read;
use std::{
    net::TcpListener,
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

pub fn start_rpc(engine: Shared, state: &Path, mount: Option<String>) -> Result<Discovery> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    listener.set_nonblocking(true)?;
    let (info, stopping, observation, bulk) = {
        let e = engine.lock().unwrap();
        (
            Discovery {
                format: 2,
                address: format!("ws://{}", listener.local_addr()?),
                state: state.to_string_lossy().into_owned(),
                mount,
                repo: e.store.repo.clone(),
                device: e.store.device.clone(),
            },
            e.stopping.clone(),
            e.observation.clone(),
            crate::bulk_sync::Worker {
                root: e.store.root.clone(),
                repo: e.store.repo.clone(),
                device: e.store.device.clone(),
                locks: e.transfers.clone(),
            },
        )
    };
    engine.lock().unwrap().discovery = serde_json::to_vec(&info)?;
    std::fs::write(
        state.join("runtime.json"),
        serde_json::to_vec_pretty(&info)?,
    )?;
    let recovery = engine.clone();
    let recovery_stop = stopping.clone();
    std::thread::spawn(move || {
        while !recovery_stop.load(std::sync::atomic::Ordering::Relaxed) {
            std::thread::sleep(Duration::from_millis(250));
            if let Ok(mut e) = recovery.try_lock() {
                let _ = e.retry_pending();
                e.publish_observation();
            }
            deliver_notifications(&recovery);
        }
    });
    std::thread::spawn(move || {
        while !stopping.load(std::sync::atomic::Ordering::Relaxed) {
            match listener.accept() {
                Ok((stream, _)) => {
                    let engine = engine.clone();
                    let observation = observation.clone();
                    let stopping = stopping.clone();
                    let bulk = bulk.clone();
                    std::thread::spawn(move || {
                        let _ = (|| -> Result<()> {
                            let mut socket = crate::ws::accept(stream)?;
                            loop {
                                let request: Value = crate::ws::receive(&mut socket)?;
                                let result = dispatch_rpc(&engine, &observation, &bulk, &request);
                                deliver_notifications(&engine);
                                crate::ws::reply(&mut socket, result)?;
                                if stopping.load(std::sync::atomic::Ordering::Relaxed) {
                                    return Ok(());
                                }
                            }
                        })();
                    });
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(_) => break,
            }
        }
    });
    Ok(info)
}
fn dispatch_rpc(
    engine: &Shared,
    observation: &Arc<Mutex<Value>>,
    bulk: &crate::bulk_sync::Worker,
    r: &Value,
) -> Result<Value> {
    let request = r["request"].as_str().context("INVALID_REQUEST_ID")?;
    let payload = &r["payload"];
    if payload["op"]
        .as_str()
        .is_some_and(|op| op.starts_with("published-bulk-"))
    {
        return crate::bulk_sync::worker_control(bulk, payload);
    }
    if payload["op"] == "published-summary" {
        let store = bulk.store()?;
        return Ok(json!(crate::paired_sync::PublishedState {
            repo: store.repo.clone(),
            replica: store.device.clone(),
            label: store.repo.clone(),
            branches: store
                .branches()?
                .into_iter()
                .filter(|branch| branch.shared)
                .collect(),
        }));
    }
    if payload["op"]
        .as_str()
        .is_some_and(|op| op.starts_with("published-"))
    {
        return published_rpc(engine, payload);
    }
    if payload["op"] == "status" {
        if let Ok(mut e) = engine.try_lock() {
            return e.control(request, payload);
        }
        let mut status = observation.lock().unwrap().clone();
        status["busy"] = json!(true);
        status["stale"] = json!(true);
        return Ok(status);
    }
    if payload["op"] == "preview-object" || payload["op"] == "cat-object" {
        let object = payload["object"].as_str().context("MISSING_ARGUMENT")?;
        let mut file = fetch_verified(
            engine,
            object,
            std::time::Instant::now() + Duration::from_secs(15),
        )?;
        use std::io::Seek;
        file.rewind()?;
        let total = file.metadata()?.len();
        if payload["op"] == "cat-object" {
            ensure!(total <= MAX_FILE as u64, "OBJECT_REQUIRES_STREAMING");
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes)?;
            return Ok(json!({"hex":hex::encode(bytes)}));
        }
        let mut bytes = Vec::new();
        file.take(64 * 1024).read_to_end(&mut bytes)?;
        let binary = bytes.contains(&0) || std::str::from_utf8(&bytes).is_err();
        return Ok(
            json!({"object":object,"size":total,"truncated":total > bytes.len() as u64,
            "text":if binary { "Binary content; text preview unavailable".to_owned() } else { String::from_utf8_lossy(&bytes).into_owned() }}),
        );
    }
    if payload["op"] == "checkout" && engine.lock().unwrap().mounted {
        return crate::mount::checkout(engine, request, payload);
    }
    if payload["op"] == "stop" {
        if engine.lock().unwrap().mounted {
            crate::mount::stop(engine)?;
        } else {
            engine.lock().unwrap().quiet()?;
        }
        engine
            .lock()
            .unwrap()
            .stopping
            .store(true, std::sync::atomic::Ordering::Relaxed);
        return Ok(json!({"stopped":true}));
    }
    if payload["op"] == "bucket-export" {
        return export_bucket(engine, payload);
    }
    engine.lock().unwrap().control(request, payload)
}
