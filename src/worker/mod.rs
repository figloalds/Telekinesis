//! Worker process entry: one folder state, its engine, the loopback RPC
//! listener and an optional mount. `tkfs daemon --state S [--mount M]` runs it.
pub mod activator;
pub mod heartbeat;
pub mod maintenance;
pub mod mount_ctl;
pub mod record;
pub mod status_cell;

use crate::{
    core::Store,
    orchestrator,
    runtime::{self, Engine},
};
use anyhow::{Context, Result, ensure};
use serde_json::json;
use std::{
    path::Path,
    sync::{Arc, Mutex, atomic::Ordering},
    time::Duration,
};

#[cfg(target_os = "linux")]
static STOP: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
#[cfg(target_os = "linux")]
extern "C" fn signal_stop(_: libc::c_int) {
    STOP.store(true, Ordering::Relaxed);
}

pub fn run(state: &Path, mount: Option<&Path>) -> Result<()> {
    let state = std::fs::canonicalize(state)?;
    let _lock = orchestrator::lock_state(&state)?;
    let reporting = crate::startup::Reporting::begin();
    let store = Store::open_existing(&state)?;
    crate::startup::progress("recovering pending saves");
    let engine = Arc::new(Mutex::new(Engine::new(store)));
    let mount = mount.map(std::path::absolute).transpose()?;
    if let Some(path) = &mount {
        let parent = path.parent().context("MOUNT_PARENT_REQUIRED")?;
        std::fs::create_dir_all(parent)?;
        let resolved =
            std::fs::canonicalize(parent)?.join(path.file_name().context("MOUNT_NAME_REQUIRED")?);
        ensure!(
            !resolved.starts_with(&state) && !state.starts_with(&resolved),
            "MOUNT_OVERLAPS_STATE"
        );
    }
    // Publish discovery only after the real mount is ready.
    if let Some(path) = &mount {
        crate::startup::progress("mounting filesystem");
        crate::mount::start(engine.clone(), path)?;
    }
    let info = runtime::start_rpc(
        engine.clone(),
        &state,
        mount.as_ref().map(|p| p.to_string_lossy().into_owned()),
    )?;
    println!(
        "{}",
        json!({"ready":true,"runtime":state.join("runtime.json"),"repo":info.repo,"device":info.device,"mounted":mount.is_some()})
    );
    drop(reporting);
    let stop = engine.lock().unwrap().stopping.clone();
    #[cfg(target_os = "linux")]
    unsafe {
        libc::signal(libc::SIGINT, signal_stop as libc::sighandler_t);
        libc::signal(libc::SIGTERM, signal_stop as libc::sighandler_t);
    }
    while !stop.load(Ordering::Relaxed) {
        #[cfg(target_os = "linux")]
        if STOP.swap(false, Ordering::Relaxed) {
            match runtime::rpc(&info, &crate::core::id(), json!({"op":"stop"})) {
                Ok(_) => break,
                Err(error) => eprintln!("Cannot stop while files are busy: {error:#}"),
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    // Allow the control thread to flush the successful stop reply.
    std::thread::sleep(Duration::from_millis(100));
    let _ = std::fs::remove_file(state.join("runtime.json"));
    Ok(())
}
